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
#pragma once

#include "write_batch_internal.h"

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/internal_types.h"
#include "pdlfs-common/leveldb/snapshot.h"

#include "pdlfs-common/env.h"
#include "pdlfs-common/log_writer.h"
#include "pdlfs-common/port.h"

#include <deque>
#include <set>

namespace pdlfs {
// Sanitize db options. The caller should delete result.info_log if it is not
// equal to raw_options.info_log. The caller should also delete
// result.block_cache if it is not equal to raw_options.block_cache. Finally,
// the caller should delete result.table_cache if it is not equal to
// raw_options.table_cache.
extern DBOptions SanitizeOptions(const std::string& dbname,
                                 const InternalKeyComparator* icmp,
                                 const InternalFilterPolicy* ipolicy,
                                 const DBOptions& raw_options,
                                 bool create_infolog);
class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status SyncWAL();
  virtual Status FlushMemTable(const FlushOptions&);
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions&, WriteBatch* updates);
  virtual Status Get(const ReadOptions&, const Slice& key, std::string* value);
  virtual Status Get(const ReadOptions&, const Slice& key, Slice* value,
                     char* scratch, size_t scratch_size);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual void CompactRange(const Slice* begin, const Slice* end);
  virtual Status AddL0Tables(const InsertOptions&, const std::string& dir);
  virtual Status Dump(const DumpOptions&, const Range& range,
                      const std::string& dir, SequenceNumber* min_seq,
                      SequenceNumber* max_seq);
  // Compaction control interface
  virtual Status ResumeDbCompaction();  // Dynamically resume bg compaction
  virtual Status FreezeDbCompaction();  // Dynamically pause compaction
  virtual Status DrainCompactions();

  // Extra methods that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

 protected:
  friend class DB;
  struct CompactionState;
  struct InsertionState;
  struct Writer;

  Status Get(const ReadOptions&, const Slice& key, Buffer* buf);
  // The snapshots specified in read options are ignored by the following calls
  Status Get(const ReadOptions&, const LookupKey& lkey, Buffer* buf);
  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  // Bulk insert a list of pre-ordered and pre-sequenced updates.
  Status BulkInsert(Iterator* updates);

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit);

  Status NewDB();

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable();
  Status RecoverLogFile(uint64_t log_number, VersionEdit* edit,
                        SequenceNumber* max_sequence);

  Status DumpMemTable(MemTable* mem, VersionEdit* edit, Version* base);
  Status WriteLevel0Table(Iterator* iter, VersionEdit* edit, Version* base,
                          SequenceNumber* min_seq, SequenceNumber* max_seq);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */);
  WriteBatch* BuildBatchGroup(Writer** last_writer);

  void RecordBackgroundError(const Status& s);

  bool HasCompaction();
  void MaybeScheduleCompaction();
  static void BGWork(void* db);
  void BackgroundCall();
  void BackgroundCompactionWrapper();
  void BackgroundCompaction();
  void CleanupCompaction(CompactionState* compact);
  Status DoCompactionWork(CompactionState* compact);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact);

  Status LoadLevel0Table(InsertionState* insert);
  Status MigrateLevel0Table(InsertionState* insert, const std::string& fname);
  Status InsertLevel0Tables(InsertionState* insert);

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  bool owns_info_log_;
  bool owns_cache_;
  bool owns_table_cache_;
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache* table_cache_;

  // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;  // Signalled when background work finishes
  MemTable* mem_;
  MemTable* imm_;                // Memtable being compacted
  port::AtomicPointer has_imm_;  // So bg thread can detect non-NULL imm_
  WritableFile* logfile_;
  uint64_t logfile_number_;
  log::Writer* log_;
  uint32_t seed_;  // For sampling.

  // Queue of writers.
  std::deque<Writer*> writers_;
  WriteBatch flush_memtable_;  // Dummy batch representing a compaction request
  WriteBatch sync_wal_;        // Dummy batch representing a WAL sync request
  // Temporary storage for grouping write batches
  WriteBatch tmp_batch_;
  // Number of time a writer is soft limited, hard limited, or waits for buffer
  // room
  uint64_t l0_soft_limits_;
  uint64_t l0_hard_limits_;
  uint64_t l0_waits_;

  SnapshotList snapshots_;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_;

  // If not zero, will disable the scheduling of compactions that are not
  // memtable compactions or manual compactions
  unsigned int bg_compaction_disabled_;
  // If not zero, will stop scheduling any new compactions and will pause the
  // progress of an ongoing compaction if there is one
  unsigned int bg_compaction_paused_;
  // Has a background compaction been scheduled and not yet completed?
  bool bg_compaction_scheduled_;
  // Is there an active background compaction job? Background compaction work
  // may be paused (inactive) in the middle
  bool bg_compaction_in_progress_;
  // Is there an active foreground bulk insertion job?
  bool bulk_insert_in_progress_;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;  // NULL means beginning of key range
    const InternalKey* end;    // NULL means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };
  ManualCompaction* manual_compaction_;

  VersionSet* versions_;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
    // Number of files produced.
    int64_t files;
    // Number of files read at the target level of compaction.
    int64_t in0;
    // Number of files read at the parent level.
    int64_t in1;
    // Number of compactions.
    int64_t n;

    CompactionStats()
        : micros(0),
          bytes_read(0),
          bytes_written(0),
          files(0),
          in0(0),
          in1(0),
          n(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
      this->files += c.files;
      this->in0 += c.in0;
      this->in1 += c.in1;
      this->n += c.n;
    }
  };
  CompactionStats stats_[config::kNumLevels];

  // No copying allowed
  void operator=(const DBImpl&);
  DBImpl(const DBImpl&);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  bool BeforeUserLimit(const Slice& ikey, const Slice& limit) {
    if (!limit.empty()) {
      return user_comparator()->Compare(ExtractUserKey(ikey), limit) < 0;
    } else {
      return true;
    }
  }
};

}  // namespace pdlfs
