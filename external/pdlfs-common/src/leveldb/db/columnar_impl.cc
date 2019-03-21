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

#include "columnar_impl.h"
#include "../merger.h"
#include "db_iter.h"
#include "memtable.h"
#include "version_edit.h"
#include "version_set.h"

#include "pdlfs-common/leveldb/dbfiles.h"
#include "pdlfs-common/mutexlock.h"

namespace pdlfs {

Column::~Column() {}

ColumnImpl::~ColumnImpl() { delete db_; }

ColumnarDBWrapper::~ColumnarDBWrapper() { delete impl_; }

ColumnSelector::~ColumnSelector() {}

Status ColumnImpl::WriteTableStart() {
  // TODO
  return Status::OK();
}

Status ColumnImpl::WriteTableEnd() {
  // TODO
  return Status::OK();
}

Status ColumnImpl::WriteTable(Iterator* contents) {
  return db_->BulkInsert(contents);
}

Iterator* ColumnImpl::NewInternalIterator(const ReadOptions& options) {
  SequenceNumber ignored_last_sequence;
  uint32_t ignored_seed;
  return db_->NewInternalIterator(options, &ignored_last_sequence,
                                  &ignored_seed);
}

Status ColumnImpl::Get(const ReadOptions& options, const LookupKey& lkey,
                       Buffer* result) {
  return db_->Get(options, lkey, result);
}

Status ColumnImpl::PreRecover(RecoverMethod method) {
  // TODO
  return Status::OK();
}

Status ColumnImpl::Recover() {
  VersionEdit edit;
  MutexLock l(&db_->mutex_);

  Status s = db_->Recover(&edit);  // Handles create_if_missing, error_if_exists
  if (s.ok()) {
    s = db_->versions_->LogAndApply(&edit, &db_->mutex_);
  }
  if (s.ok()) {
    db_->DeleteObsoleteFiles();
    db_->MaybeScheduleCompaction();
  }

  return s;
}

ColumnarDBImpl::ColumnarDBImpl(const Options& options,
                               const std::string& dbname)
    : DBImpl(options, dbname) {}

ColumnarDBImpl::~ColumnarDBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_ || bulk_insert_in_progress_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  size_t num_columns = columns_.size();
  for (size_t i = 0; i < num_columns; i++) {
    columns_[i]->Unref();
  }
}

Status ColumnarDBImpl::PreRecover(Column::RecoverMethod* method) {
  env_->CreateDir(dbname_.c_str());
  *method = Column::kRollback;
  return Status::OK();
}

Status ColumnarDBImpl::RecoverLogFile(uint64_t log_number, VersionEdit* edit,
                                      SequenceNumber* max_sequence) {
  // TODO
  return Status::OK();
}

void ColumnarDBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);
  size_t num_columns = columns_.size();
  assert(num_columns != 0);
  VersionEdit edit;
  MemTable* imm = imm_;
  Status s;

  {
    mutex_.Unlock();
    s = BeginMemTableCompaction();

    if (s.ok()) {
      for (size_t i = 0; i < num_columns; i++) {
        s = WriteToColumn(imm, i);
        if (!s.ok()) {
          break;
        }
      }
    }
    mutex_.Lock();
  }

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting db during memtable compaction");
  }

  // Commit
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    mutex_.Unlock();
    s = FinishMemTableCompaction();
    mutex_.Lock();
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

Status ColumnarDBImpl::BeginMemTableCompaction() {
  Status s;
  size_t num_columns = columns_.size();
  assert(num_columns != 0);
  for (size_t i = 0; i < num_columns; i++) {
    s = columns_[i]->WriteTableStart();
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

Status ColumnarDBImpl::FinishMemTableCompaction() {
  Status s;
  size_t num_columns = columns_.size();
  assert(num_columns != 0);
  for (size_t i = 0; i < num_columns; i++) {
    s = columns_[i]->WriteTableEnd();
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

Status ColumnarDBImpl::WriteToColumn(MemTable* mem, size_t column_index) {
  Status s;
  Iterator* iter = mem->NewIterator();
  assert(column_index < columns_.size());
  s = columns_[column_index]->WriteTable(iter);
  delete iter;
  return s;
}

Column* ColumnarDBImpl::PickColumn(const Slice& key) {
  size_t column_index = selector_->Select(key);
  if (column_index < columns_.size()) {
    return columns_[column_index];
  } else {
    return NULL;
  }
}

namespace {
struct IterState {
  port::Mutex* mu;
  Column* column;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};
}  // namespace

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  if (state->mem != NULL) state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  if (state->column != NULL) {
    state->column->Unref();
  }
  state->mu->Unlock();
  delete state;
}

Iterator* ColumnarDBImpl::NewColumnarInternalIterator(
    const ReadOptions& options, const Slice& pivot,
    SequenceNumber* latest_snapshot, uint32_t* seed) {
  IterState* cleanup = new IterState;
  MutexLock l(&mutex_);
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
  Version* current = versions_->current();
  current->AddIterators(options, &list);
  Column* column = PickColumn(pivot);
  if (column != NULL) {
    list.push_back(column->NewInternalIterator(options));
    column->Ref();
  }
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  current->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = current;
  cleanup->column = column;
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  return internal_iter;
}

Status ColumnarDBImpl::ColumnarGet(const ReadOptions& options, const Slice& key,
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
  Column* column = PickColumn(key);
  if (mem != NULL) mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();
  if (column != NULL) {
    column->Ref();
  }

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
      have_stat_update = true;
      if (!current->Get(options, lkey, value, &s, &stats)) {
        if (column != NULL) {
          s = column->Get(options, lkey, value);
        }
      }
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
  if (column != NULL) {
    column->Unref();
  }
  return s;
}

Iterator* ColumnarDBImpl::NewColumnarIterator(const ReadOptions& options,
                                              const Slice& pivot) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter =
      NewColumnarInternalIterator(options, pivot, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
           ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
           : latest_snapshot),
      seed);
}

Status ColumnarDBWrapper::FlushMemTable(const FlushOptions& options) {
  return impl_->FlushMemTable(options);
}

Status ColumnarDBWrapper::Put(const WriteOptions& options, const Slice& key,
                              const Slice& value) {
  return impl_->Put(options, key, value);
}

Status ColumnarDBWrapper::Delete(const WriteOptions& options,
                                 const Slice& key) {
  return impl_->Delete(options, key);
}

Status ColumnarDBWrapper::Write(const WriteOptions& options,
                                WriteBatch* updates) {
  return impl_->Write(options, updates);
}

Status ColumnarDBWrapper::Get(const ReadOptions& options, const Slice& key,
                              std::string* value) {
  buffer::StringBuf buf(value);
  Status s = impl_->ColumnarGet(options, key, &buf);
  return s;
}

Status ColumnarDBWrapper::Get(const ReadOptions& options, const Slice& key,
                              Slice* value, char* scratch,
                              size_t scratch_size) {
  buffer::DirectBuf buf(scratch, scratch_size);
  Status s = impl_->ColumnarGet(options, key, &buf);
  if (s.ok()) {
    *value = buf.Read();
    if (value->data() == NULL) {
      s = Status::BufferFull(Slice());
    }
  }
  return s;
}

Iterator* ColumnarDBWrapper::NewIterator(const ReadOptions& options) {
  return impl_->NewColumnarIterator(options, Slice());
}

const Snapshot* ColumnarDBWrapper::GetSnapshot() {
  return impl_->GetSnapshot();
}

void ColumnarDBWrapper::ReleaseSnapshot(const Snapshot* snapshot) {
  return impl_->ReleaseSnapshot(snapshot);
}

Status ColumnarDBWrapper::TEST_CompactMemTable() {
  return impl_->TEST_CompactMemTable();
}

Status Column::Open(ColumnStyle style, RecoverMethod method,
                    const Options& options, const std::string& name,
                    Column** result) {
  *result = NULL;

  Column* column = NULL;
  switch (style) {
    case kLSMStyle:
      column = new ColumnImpl(new DBImpl(options, name));
      break;
    default:
      return Status::NotSupported(Slice());
  }

  column->Ref();
  Status s = column->PreRecover(method);
  if (s.ok()) {
    s = column->Recover();
  }

  if (s.ok()) {
    *result = column;
  } else {
    column->Unref();
  }

  return s;
}

Status ColumnarDB::Open(const Options& options, const std::string& dbname,
                        const ColumnSelector* selector,
                        ColumnStyle* column_styles, size_t num_columns,
                        DB** dbptr) {
  *dbptr = NULL;
  if (selector == NULL || column_styles == NULL) {
    return Status::InvalidArgument("No column configurations");
  } else if (num_columns == 0) {
    return Status::InvalidArgument("No column defined");
  }
  ColumnarDBImpl* impl = new ColumnarDBImpl(options, dbname);
  impl->selector_ = selector;
  impl->mutex_.Lock();
  Column::RecoverMethod method;
  Status s = impl->PreRecover(&method);
  if (s.ok()) {
    for (size_t i = 0; i < num_columns; i++) {
      Column* column;
      s = Column::Open(column_styles[i], method, options, ColumnName(dbname, i),
                       &column);
      if (s.ok()) {
        impl->columns_.push_back(column);
      } else {
        break;
      }
    }
  }

  if (s.ok()) {
    VersionEdit edit;
    s = impl->Recover(&edit);  // Handles create_if_missing, error_if_exists
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
  }

  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = new ColumnarDBWrapper(impl);
  } else {
    delete impl;
  }

  return s;
}

}  // namespace pdlfs
