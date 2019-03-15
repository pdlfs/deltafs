/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */
#include "db_impl.h"
#include "builder.h"
#include "db_iter.h"
#include "memtable.h"
#include "table_cache.h"
#include "version_set.h"

#include "../merger.h"
#include "../table_stats.h"
#include "../two_level_iterator.h"

#include "pdlfs-common/leveldb/block.h"
#include "pdlfs-common/leveldb/db/db.h"
#include "pdlfs-common/leveldb/db/dbformat.h"
#include "pdlfs-common/leveldb/iterator_wrapper.h"
#include "pdlfs-common/leveldb/table.h"
#include "pdlfs-common/leveldb/table_builder.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/dbfiles.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/log_reader.h"
#include "pdlfs-common/log_writer.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/status.h"
#include "pdlfs-common/strutil.h"

#include <stdint.h>
#include <stdio.h>
#include <algorithm>
#include <set>
#include <string>
#include <vector>

namespace pdlfs {

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) {}
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c), outfile(NULL), builder(NULL), total_bytes(0) {}
};

struct DBImpl::InsertionState {
  const InsertOptions* const options;

  // Source table files involved in this bulk insertion
  const std::string& source_dir;
  std::vector<std::string> source_names;

  // Table info
  struct File {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
    SequenceNumber min_seq, max_seq;
  };
  std::vector<File> files;

  File* current_file() { return &files[files.size() - 1]; }

  explicit InsertionState(const InsertOptions& options, const std::string& dir)
      : options(&options), source_dir(dir) {}
};

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options, true)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      owns_table_cache_(options_.table_cache != raw_options.table_cache),
      dbname_(dbname),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      mem_(NULL),
      imm_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      seed_(0),
      bg_compaction_paused_(0),
      bg_compaction_scheduled_(false),
      bulk_insert_in_progress_(false),
      manual_compaction_(NULL) {
  if (!options_.no_memtable) {
    mem_ = new MemTable(internal_comparator_);
    mem_->Ref();
  }
  has_imm_.Release_Store(NULL);
  table_cache_ = new TableCache(dbname_, &options_, options_.table_cache);

  versions_ =
      new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_);

  if (options_.info_log == Logger::Default()) {
    owns_info_log_ = false;
  }
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  Log(options_.info_log, "Shutting down ...");
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_ || bulk_insert_in_progress_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) delete options_.info_log;
  if (owns_table_cache_) delete options_.table_cache;
  if (owns_cache_) delete options_.block_cache;
  // Remove LOCK file
  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  // Detach db directory so other processes may mount the db.
  env_->DetachDir(dbname_.c_str());
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(4);
  new_db.SetLastSequence(0);

  // Descriptor numbers begin with 3 if we are not set to use odd/even MANIFEST
  // files.
  const uint64_t num = options_.rotating_manifest ? 1 : 3;
  const std::string manifest = DescriptorFileName(dbname_, num);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest.c_str(), &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    if (!options_.rotating_manifest) {
      // Make "CURRENT" file that points to the new manifest file.
      s = SetCurrentFile(env_, dbname_, num);
    }
  } else {
    env_->DeleteFile(manifest.c_str());
  }
  return s;
}

Status DBImpl::SyncWAL() {  // Force fsync on the write ahead log file
  return Write(WriteOptions(), &sync_wal_);
}

Status DBImpl::FlushMemTable(const FlushOptions& options) {
  Status s = Write(WriteOptions(), &flush_memtable_);
  if (!s.ok()) {
    // Abort on errors
  } else if (options.wait || options.force_flush_l0) {
    MutexLock l(&mutex_);
    // Either mine is being compacted, or someone else's table
    // is being compacted.
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (options.force_flush_l0 && bg_error_.ok()) {
      mutex_.Unlock();
      TEST_CompactRange(0, NULL, NULL);
      mutex_.Lock();
    }
    if (!bg_error_.ok()) {
      s = bg_error_;
    }
  }
  return s;
}

Status DBImpl::DrainCompactions() {
  Status s;
  MutexLock l(&mutex_);
  while (HasCompaction() && bg_error_.ok()) {
    MaybeScheduleCompaction();
    bg_cv_.Wait();
  }
  if (!bg_error_.ok()) {
    s = bg_error_;
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_.c_str(), &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%llu\n", int(type),
            static_cast<unsigned long long>(number));
        if (!options_.gc_skip_deletion) {
          Log(options_.info_log, "Remove %s", filenames[i].c_str());
          std::string fname = dbname_ + "/" + filenames[i];
          env_->DeleteFile(fname.c_str());
        }
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit) {
  mutex_.AssertHeld();
  Status s;
  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_.c_str());
  assert(db_lock_ == NULL);
  if (!options_.skip_lock_file) {
    const std::string lockname = LockFileName(dbname_);
    s = env_->LockFile(lockname.c_str(), &db_lock_);
    if (!s.ok()) {
      return s;
    }
  }

  const std::string man1 = DescriptorFileName(dbname_, 1);
  const std::string man2 = DescriptorFileName(dbname_, 2);
  const std::string curr = CurrentFileName(dbname_);
  if (!env_->FileExists(man1.c_str()) && !env_->FileExists(man2.c_str()) &&
      !env_->FileExists(curr.c_str())) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(dbname_, "does not exist");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_, "exists");
    }
  }

  s = versions_->Recover();
  if (s.ok()) {
    SequenceNumber max_sequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of leveldb.
    const uint64_t min_log = versions_->LogNumber();
    const uint64_t prev_log = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_.c_str(), &filenames);
    if (!s.ok()) {
      return s;
    }
    std::set<uint64_t> expected;
    versions_->AddLiveFiles(&expected);
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)) {
        expected.erase(number);
        if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
          logs.push_back(number);
      }
    }
    if (!expected.empty()) {
      char buf[50];
      snprintf(buf, sizeof(buf), "%d missing files; e.g.",
               static_cast<int>(expected.size()));
      return Status::Corruption(buf,
                                TableFileName(dbname_, *(expected.begin())));
    }

    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size(); i++) {
      s = RecoverLogFile(logs[i], edit, &max_sequence);

      // The previous incarnation may not have written any MANIFEST
      // records after allocating this log number.  So we manually
      // update the file number allocation counter in VersionSet.
      versions_->MarkFileNumberUsed(logs[i]);
    }

    if (s.ok()) {
      if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
      }
    }
  }

  return s;
}

Status DBImpl::RecoverLogFile(uint64_t log_number, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  const std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname.c_str(), &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      status = WriteMemTable(mem, edit, NULL);
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
      mem->Unref();
      mem = NULL;
    }
  }

  if (status.ok() && mem != NULL) {
    status = WriteMemTable(mem, edit, NULL);
    // Reflect errors immediately so that conditions like full
    // file-systems cause the DB::Open() to fail.
  }

  if (mem != NULL) mem->Unref();
  delete file;
  return status;
}

// REQUIRES: mutex_ has been locked.
Status DBImpl::WriteMemTable(MemTable* mem, VersionEdit* edit, Version* base) {
  mutex_.AssertHeld();
  SequenceNumber ignored_min_seq;
  SequenceNumber ignored_max_seq;
  Iterator* iter = mem->NewIterator();
  Status s = WriteLevel0Table(iter, edit, base, &ignored_min_seq,
                              &ignored_max_seq, false);
  delete iter;
  return s;
}

// REQUIRES: mutex_ has been locked.
Status DBImpl::WriteLevel0Table(Iterator* iter, VersionEdit* edit,
                                Version* base, SequenceNumber* min_seq,
                                SequenceNumber* max_seq, bool force_level0) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, min_seq,
                   max_seq, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();

    if (base != NULL) {
      if (!force_level0 && !options_.disable_compaction) {
        level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
      } else {
        // If compaction has been disabled, or force_level0 has been set,
        // all MemTable dumps will only go to level-0.
      }
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.seq_off,
                  meta.smallest, meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteMemTable(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting db during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == NULL) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      bg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = NULL;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_cv_.SignalAll();
  }
}

bool DBImpl::HasCompaction() {
  if (imm_ != NULL) {
    return true;
  } else if (manual_compaction_ != NULL) {
    return true;
  } else if (options_.disable_compaction) {
    return false;
  } else if (versions_->NeedsCompaction(!options_.disable_seek_compaction)) {
    return true;
  } else {
    return false;  // No compaction needed
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_ || bg_compaction_paused_) {
    // Already scheduled or paused
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (!HasCompaction()) {
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    if (options_.compaction_pool != NULL) {
      options_.compaction_pool->Schedule(&DBImpl::BGWork, this);
    } else {
      env_->Schedule(&DBImpl::BGWork, this);
    }
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else if (bulk_insert_in_progress_) {
    // Yield to bulk insertion
  } else {
    BackgroundCompaction();
  }

  bg_compaction_scheduled_ = false;
  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != NULL) {
    CompactMemTable();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else if (!options_.disable_compaction) {
    c = versions_->PickCompaction(!options_.disable_seek_compaction);
  } else {
    c = NULL;
  }

  Status status;
  if (c == NULL) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->seq_off,
                       f->smallest, f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = NULL;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname.c_str(), &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }

  // Obtain table properties
  const TableProperties* props = compact->builder->properties();
  compact->current_output()->smallest.DecodeFrom(props->first_key());
  compact->current_output()->largest.DecodeFrom(props->last_key());
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

  if (s.ok() && current_entries > 0) {
    const SequenceOff off = 0;
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(), output_number,
                                               current_bytes, off);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu: %lld keys, %lld bytes",
          (unsigned long long)output_number,
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const SequenceOff off = 0;
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         off, out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (; input->Valid() && !shutting_down_.Acquire_Load();) {
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != NULL) {
        CompactMemTable();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != NULL) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }

      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting db during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  if (state->mem != NULL) state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  if (mem_ != NULL) {
    list.push_back(mem_->NewIterator());
    mem_->Ref();
  }
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options, const LookupKey& lkey,
                   Buffer* value) {
  Status s;
  MutexLock l(&mutex_);
  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  if (mem != NULL) mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    if (mem != NULL && mem->Get(lkey, value, options.limit, &s)) {
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, options.limit, &s)) {
      // Done
    } else {
      current->Get(options, lkey, value, &s, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    if (!options_.disable_seek_compaction) {
      MaybeScheduleCompaction();
    }
  }
  if (mem != NULL) mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   Buffer* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  if (mem != NULL) mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem != NULL && mem->Get(lkey, value, options.limit, &s)) {
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, options.limit, &s)) {
      // Done
    } else {
      current->Get(options, lkey, value, &s, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    if (!options_.disable_seek_compaction) {
      MaybeScheduleCompaction();
    }
  }
  if (mem != NULL) mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  buffer::StringBuf buf(value);
  Status s = Get(options, key, &buf);
  return s;
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key, Slice* value,
                   char* scratch, size_t scratch_size) {
  buffer::DirectBuf buf(scratch, scratch_size);
  Status s = Get(options, key, &buf);
  if (s.ok()) {
    *value = buf.Read();
    if (value->data() == NULL) {
      s = Status::BufferFull(Slice());
    }
  }
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
           ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
           : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    if (!options_.disable_seek_compaction) {
      MaybeScheduleCompaction();
    }
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& o, const Slice& key) {
  return DB::Delete(o, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  if (my_batch == NULL) {
    // NULL batch is for memtable compaction
    my_batch = &flush_memtable_;
  }

  Writer w(&mutex_);
  w.sync = options.sync;
  w.done = false;
  w.batch = my_batch;

  bool flush_or_sync = false;
  if (my_batch == &flush_memtable_ || my_batch == &sync_wal_) {
    flush_or_sync = true;
  }

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(my_batch == &flush_memtable_);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  bool sync_error = false;

  if (status.ok() && !flush_or_sync) {
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    if (!options_.no_memtable) {
      mutex_.Unlock();
      if (!options_.disable_write_ahead_log) {
        status = log_->AddRecord(WriteBatchInternal::Contents(updates));
        if (status.ok() && options.sync) {
          status = logfile_->Sync();
          if (!status.ok()) {
            sync_error = true;
          }
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }
      mutex_.Lock();
    } else {
      // Temporarily disable any background compaction
      bg_compaction_paused_++;
      while (bg_compaction_scheduled_ || bulk_insert_in_progress_) {
        bg_cv_.Wait();
      }

      bulk_insert_in_progress_ = true;
      MemTable* mem = new MemTable(internal_comparator_);
      mem->Ref();

      status = WriteBatchInternal::InsertInto(updates, mem);
      if (status.ok()) {
        VersionEdit edit;
        Version* base = versions_->current();
        base->Ref();
        Iterator* iter = mem->NewIterator();
        SequenceNumber ignored_min_seq;
        SequenceNumber ignored_max_seq;
        status = WriteLevel0Table(iter, &edit, base, &ignored_min_seq,
                                  &ignored_max_seq, true);
        delete iter;
        base->Unref();

        if (status.ok()) {
          versions_->SetLastSequence(last_sequence);
          status = versions_->LogAndApply(&edit, &mutex_);
        }

        if (!status.ok()) {
          RecordBackgroundError(status);
        }
      }

      mem->Unref();
      bulk_insert_in_progress_ = false;
      // Restart background compaction
      assert(bg_compaction_paused_ > 0);
      bg_compaction_paused_--;
      MaybeScheduleCompaction();
      bg_cv_.SignalAll();
    }

    if (updates == &tmp_batch_) {
      updates->Clear();
    }

    versions_->SetLastSequence(last_sequence);
  } else if (status.ok() && my_batch == &sync_wal_) {
    if (!options_.disable_write_ahead_log) {
      mutex_.Unlock();
      status = logfile_->Sync();
      if (!status.ok()) {
        sync_error = true;
      }
      mutex_.Lock();
    }
  }

  if (sync_error) {
    // The state of the log file is indeterminate: the log record we
    // just added may or may not show up when the DB is re-opened.
    // So we force the DB into a mode where all future writes fail.
    RecordBackgroundError(status);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) {
      break;
    }
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    assert(w->batch != NULL);

    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch == &flush_memtable_ || w->batch == &sync_wal_) {
      // Stop before the next flush or sync point
      break;
    } else {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = &tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (!options_.disable_compaction && allow_delay &&
               versions_->NumLevelFiles(0) >= options_.l0_soft_limit) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force && mem_ != NULL &&
               mem_->ApproximateMemoryUsage() <= options_.write_buffer_size) {
      // There is room in current memtable
      break;
    } else if (imm_ != NULL) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      bg_cv_.Wait();
    } else if (!options_.disable_compaction &&
               versions_->NumLevelFiles(0) >= options_.l0_hard_limit) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      bg_cv_.Wait();
    } else if (!options_.no_memtable) {
      // Close the current log file and open a new one
      if (!options_.disable_write_ahead_log) {
        assert(versions_->PrevLogNumber() == 0);
        const uint64_t new_log_number = versions_->NewFileNumber();
        const std::string fname = LogFileName(dbname_, new_log_number);
        WritableFile* file = NULL;
        s = env_->NewWritableFile(fname.c_str(), &file);
        if (!s.ok()) {
          // Avoid chewing through file number space in a tight loop.
          versions_->ReuseFileNumber(new_log_number);
          break;
        }
        delete log_;
        delete logfile_;  // This closes the file
        logfile_ = file;
        logfile_number_ = new_log_number;
        log_ = new log::Writer(file);
      }

      // Attempt to switch to a new memtable and
      // trigger compaction of old
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;  // Do not force another compaction if have room
      MaybeScheduleCompaction();
    } else {
      break;
    }
  }
  return s;
}

Status DBImpl::BulkInsert(Iterator* iter) {
  Status s;
  SequenceNumber min_seq;
  SequenceNumber max_seq;

  MutexLock l(&mutex_);
  // Temporarily disable any background compaction
  bg_compaction_paused_++;
  while (bg_compaction_scheduled_ || bulk_insert_in_progress_) {
    bg_cv_.Wait();
  }

  bulk_insert_in_progress_ = true;
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  s = WriteLevel0Table(iter, &edit, base, &min_seq, &max_seq, true);
  base->Unref();
  if (s.ok()) {
    if (max_seq > versions_->LastSequence()) {
      versions_->SetLastSequence(max_seq);
    }
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (!s.ok()) {
    RecordBackgroundError(s);
  }

  bulk_insert_in_progress_ = false;
  // Restart background compaction
  assert(bg_compaction_paused_ > 0);
  bg_compaction_paused_--;
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n", level,
                 files, versions_->NumLevelBytes(level) / 1048576.0,
                 stats_[level].micros / 1e6,
                 stats_[level].bytes_read / 1048576.0,
                 stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

namespace {
typedef DBOptions Options;

static Status FetchFirstKey(Table* table, Iterator* iter, InternalKey* result,
                            const Options& options) {
  Status s;
  iter->SeekToFirst();
  if (!iter->Valid()) {
    s = iter->status();
    if (s.ok()) {
      s = Status::Corruption("Table is empty");
    }
  } else {
    result->DecodeFrom(iter->key());
    const bool has_stats = TableStats::HasStats(table);
    if (has_stats && options.paranoid_checks) {
      ParsedInternalKey parsed;
      if (!ParseInternalKey(result->Encode(), &parsed)) {
        s = Status::Corruption("First key corrupted");
      } else {
        Slice reported = TableStats::FirstKey(table);
        const InternalKeyComparator* icmp =
            reinterpret_cast<const InternalKeyComparator*>(options.comparator);
        if (icmp->Compare(result->Encode(), reported) != 0) {
          s = Status::Corruption("First key corrupted");
        }
      }
    }
  }
  return s;
}

static Status FetchLastKey(Table* table, Iterator* iter, InternalKey* result,
                           const Options& options) {
  Status s;
  iter->SeekToLast();
  if (!iter->Valid()) {
    s = iter->status();
    if (s.ok()) {
      s = Status::Corruption("Table is empty");
    }
  } else {
    result->DecodeFrom(iter->key());
    const bool has_stats = TableStats::HasStats(table);
    if (has_stats && options.paranoid_checks) {
      ParsedInternalKey parsed;
      if (!ParseInternalKey(result->Encode(), &parsed)) {
        s = Status::Corruption("Last key corrupted");
      } else {
        Slice reported = TableStats::LastKey(table);
        const InternalKeyComparator* icmp =
            reinterpret_cast<const InternalKeyComparator*>(options.comparator);
        if (icmp->Compare(result->Encode(), reported) != 0) {
          s = Status::Corruption("Last key corrupted");
        }
      }
    }
  }
  return s;
}
}  // namespace

Status DBImpl::LoadLevel0Table(InsertionState* insert) {
  Status s;
  Table* table;
  ReadOptions opt;
  opt.verify_checksums = insert->options->verify_checksums;
  InsertionState::File* info = insert->current_file();
  Iterator* it =
      table_cache_->NewIterator(opt, info->number, info->file_size, 0, &table);

  s = it->status();
  if (s.ok()) {
    assert(table != NULL);
    const bool has_stats = TableStats::HasStats(table);
    if (!insert->options->no_seq_adjustment) {
      if (!has_stats) {
        s = Status::NotFound("Missing table stats");
      } else {
        info->min_seq = TableStats::MinSeq(table);
        info->max_seq = TableStats::MaxSeq(table);
        if (info->min_seq > info->max_seq) {
          s = Status::Corruption("Min seq > Max seq?");
        }
      }
    }
    if (s.ok()) {
      s = FetchFirstKey(table, it, &info->smallest, options_);
    }
    if (s.ok()) {
      s = FetchLastKey(table, it, &info->largest, options_);
    }
  }

  delete it;
  return s;
}

Status DBImpl::MigrateLevel0Table(InsertionState* insert,
                                  const std::string& source) {
  mutex_.AssertHeld();
  uint64_t file_number = versions_->NewFileNumber();
  pending_outputs_.insert(file_number);

  {
    InsertionState::File info;
    info.number = file_number;
    info.min_seq = info.max_seq = kMaxSequenceNumber + 1;
    insert->files.push_back(info);
  }

  mutex_.Unlock();
  std::string dst = TableFileName(dbname_, file_number);
  Log(options_.info_log, "Insert #%llu: %s -> %s",
      (unsigned long long)file_number, source.c_str(), dst.c_str());

  Status s;
  uint64_t file_size;
  s = env_->GetFileSize(source.c_str(), &file_size);
  if (s.ok()) {
    if (file_size == 0) {
      s = Status::Corruption(source, "file is empty");
    } else {
      switch (insert->options->method) {
        case kCopy:
          s = env_->CopyFile(source.c_str(), dst.c_str());
          break;
        case kRename:
          s = env_->RenameFile(source.c_str(), dst.c_str());
          break;
      }
    }
    if (s.ok()) {
      insert->current_file()->file_size = file_size;
      s = LoadLevel0Table(insert);
    }
  }

  Log(options_.info_log, "Insert #%llu: %llu bytes %s",
      (unsigned long long)file_number, (unsigned long long)file_size,
      s.ToString().c_str());

  mutex_.Lock();

  if (!s.ok()) {
    versions_->ReuseFileNumber(file_number);
  }
  return s;
}

Status DBImpl::InsertLevel0Tables(InsertionState* insert) {
  mutex_.AssertHeld();

  Status s;
  Version* base = versions_->current();
  base->Ref();

  std::string fname = insert->source_dir;
  fname.push_back('/');
  size_t prefix = fname.size();
  for (size_t i = 0; i < insert->source_names.size(); i++) {
    uint64_t ignored_number;
    FileType type;
    if (!ParseFileName(insert->source_names[i], &ignored_number, &type)) {
      // Ignore non-DB files; usually they are "." and ".."
    } else {
      fname.resize(prefix);
      fname.append(insert->source_names[i]);
      switch (type) {
        case kTableFile:
          s = MigrateLevel0Table(insert, fname);
          break;
        default:
          // Skip all other types of file
          break;
      }
      if (!s.ok()) {
        break;
      }
    }
  }

  if (s.ok()) {
    const int level = 0;
    VersionEdit edit;
    SequenceNumber next = versions_->LastSequence() + 1;
    for (size_t i = 0; i < insert->files.size(); i++) {
      SequenceOff off = 0;
      if (!insert->options->no_seq_adjustment) {
        assert(insert->files[i].min_seq <= insert->files[i].max_seq);
        assert(insert->files[i].max_seq <= kMaxSequenceNumber);
        off = next - insert->files[i].min_seq;
        next += insert->files[i].max_seq - insert->files[i].min_seq + 1;
      }
      edit.AddFile(level, insert->files[i].number, insert->files[i].file_size,
                   off, insert->files[i].smallest, insert->files[i].largest);
    }
    s = versions_->LogAndApply(&edit, &mutex_);
    if (s.ok()) {
      versions_->SetLastSequence(
          std::max(next, insert->options->suggested_max_seq));
    }
  }

  for (size_t i = 0; i < insert->files.size(); i++) {
    pending_outputs_.erase(insert->files[i].number);
  }
  base->Unref();
  return s;
}

Status DBImpl::AddL0Tables(const InsertOptions& options,
                           const std::string& bulk_dir) {
  Status s;
  InsertionState insert(options, bulk_dir);
  std::vector<std::string>* const names = &insert.source_names;
  s = env_->GetChildren(bulk_dir.c_str(), names);
  if (!s.ok()) {
    return s;
  }

  std::sort(names->begin(), names->end());

  MutexLock l(&mutex_);
  // Temporarily disable any background compaction
  bg_compaction_paused_++;
  while (bg_compaction_scheduled_ || bulk_insert_in_progress_) {
    bg_cv_.Wait();
  }

  bulk_insert_in_progress_ = true;
  s = InsertLevel0Tables(&insert);
  bulk_insert_in_progress_ = false;
  // Restart background compaction
  assert(bg_compaction_paused_ > 0);
  bg_compaction_paused_--;
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
  return s;
}

Status DBImpl::Dump(const DumpOptions& options, const Range& r,
                    const std::string& dump_dir, SequenceNumber* min_seq,
                    SequenceNumber* max_seq) {
  Status s;
  SequenceNumber seq;
  uint32_t ignored_seed;
  ReadOptions opt;
  opt.verify_checksums = options.verify_checksums;
  IteratorWrapper iter(NewInternalIterator(opt, &seq, &ignored_seed));
  if (options.snapshot != NULL) {
    seq = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  }

  uint64_t file_size = 0;
  uint64_t file_number = 1;
  const std::string fname = TableFileName(dump_dir, file_number);
  env_->CreateDir(dump_dir.c_str());

  std::string key_buf;
  if (r.start.empty()) {
    iter.SeekToFirst();
  } else {
    ParsedInternalKey ikey(r.start, kMaxSequenceNumber, kValueTypeForSeek);
    AppendInternalKey(&key_buf, ikey);
    iter.Seek(key_buf);
  }

  key_buf.resize(0);
  if (iter.Valid() && BeforeUserLimit(iter.key(), r.limit)) {
    WritableFile* file;
    s = env_->NewWritableFile(fname.c_str(), &file);
    if (!s.ok()) {
      return s;
    }

    if (min_seq != NULL) {
      *min_seq = kMaxSequenceNumber;
    }
    if (max_seq != NULL) {
      *max_seq = 0;
    }
    TableBuilder* builder = new TableBuilder(options_, file);
    std::string* last_user_key = &key_buf;
    while (s.ok() && iter.Valid() && BeforeUserLimit(iter.key(), r.limit)) {
      ParsedInternalKey ikey;
      if (!ParseInternalKey(iter.key(), &ikey)) {
        s = Status::Corruption(Slice());
      } else if (ikey.sequence <= seq) {
        if (last_user_key->empty() ||
            user_comparator()->Compare(ikey.user_key, *last_user_key) > 0) {
          last_user_key->assign(ikey.user_key.data(), ikey.user_key.size());
          switch (ikey.type) {
            case kTypeDeletion:
              break;
            case kTypeValue:
              builder->Add(iter.key(), iter.value());
              if (min_seq != NULL) {
                *min_seq = std::min(*min_seq, ikey.sequence);
              }
              if (max_seq != NULL) {
                *max_seq = std::max(*max_seq, ikey.sequence);
              }
              break;
          }
        }
      }
      iter.Next();
    }

    if (s.ok() && builder->NumEntries() != 0) {
      s = builder->Finish();
      if (s.ok()) {
        file_size = builder->FileSize();
        assert(file_size != 0);
      }
    } else {
      builder->Abandon();
    }
    delete builder;

    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
  }

  if (s.ok()) {
    s = iter.status();
  }
  if (s.ok() && file_size != 0) {
    // OK
  } else {
    env_->DeleteFile(fname.c_str());
  }

  return s;
}

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = NULL;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  Status s =
      impl->Recover(&edit);  // Handles create_if_missing, error_if_exists
  if (s.ok()) {
    if (!options.disable_write_ahead_log) {
      const uint64_t new_log_number = impl->versions_->NewFileNumber();
      const std::string fname = LogFileName(dbname, new_log_number);
      WritableFile* file;
      s = options.env->NewWritableFile(fname.c_str(), &file);
      if (s.ok()) {
        edit.SetLogNumber(new_log_number);
        impl->logfile_ = file;
        impl->logfile_number_ = new_log_number;
        impl->log_ = new log::Writer(file);
      }
    }
    if (s.ok()) {
      s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
    }
    if (s.ok()) {
      impl->DeleteObsoleteFiles();
      impl->MaybeScheduleCompaction();
    }
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

}  // namespace pdlfs
