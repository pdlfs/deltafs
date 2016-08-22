#pragma once

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdint.h>
#include <stdio.h>

#include "pdlfs-common/leveldb/db/options.h"
#include "pdlfs-common/leveldb/iterator.h"

namespace pdlfs {

class Snapshot;
class WriteBatch;

// A range of keys
struct Range {
  Slice start;  // Included in the range
  Slice limit;  // Not included in the range

  Range() {}
  Range(const Slice& s, const Slice& l) : start(s), limit(l) {}
};

// A DB is a persistent ordered map from keys to values.
// A DB is safe for concurrent access from multiple threads without
// any external synchronization.
class DB {
 protected:
  typedef DBOptions Options;

 public:
  // Open a db instance on a named image with full access.
  // Stores a pointer to the db in *dbptr and returns OK on success.
  // Otherwise, stores NULL in *dbptr and returns a non-OK status.
  // Only a single db instance can be opened on a specified db image.
  // Caller should delete *dbptr when it is no longer needed.
  static Status Open(const Options& options, const std::string& name,
                     DB** dbptr);

  DB() {}
  virtual ~DB();

  // Set the database entry for "key" to "value".  Returns OK on success,
  // and a non-OK status on error.
  // Note: consider setting options.sync = true.
  virtual Status Put(const WriteOptions& options, const Slice& key,
                     const Slice& value) = 0;

  // Remove the database entry (if any) for "key".  Returns OK on
  // success, and a non-OK status on error.  It is not an error if "key"
  // did not exist in the database.
  // Note: consider setting options.sync = true.
  virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;

  // Apply the specified updates to the database.
  // Returns OK on success, non-OK on failure.
  // Note: consider setting options.sync = true.
  virtual Status Write(const WriteOptions& options, WriteBatch* updates) = 0;

  // If the database contains an entry for "key", store the
  // corresponding value in *value and return OK.
  //
  // Consider setting "options.size_limit" to force a maximum
  // on the number of bytes to be copied into *value. This
  // is useful when the caller only needs a prefix of the value.
  //
  // If there is no entry for "key" leave *value unchanged and return
  // a status for which Status::IsNotFound() returns true.
  //
  // May return some other Status on an error.
  virtual Status Get(const ReadOptions& options, const Slice& key,
                     std::string* value) = 0;

  // If the database contains an entry for "key", store the
  // corresponding value in *value and return OK.
  //
  // The space of *value is backed by a pre-allocated buffer
  // at [scratch, scratch + scratch_size). This enables callers
  // to manage their own value buffers and have DB values
  // copied directly into those buffers.
  //
  // If scratch doesn't have enough space to hold the value,
  // return Status::BufferFull() and the *value is set to
  // Slice(NULL, value_size). The caller can then retry the call
  // with a larger buffer.
  //
  // Consider setting "options.size_limit" to force a maximum
  // on the number of bytes to be copied into *value. This
  // is useful when the caller only needs a prefix of the value.
  //
  // If there is no entry for "key" leave *value unchanged and return
  // a status for which Status::IsNotFound() returns true.
  //
  // May return some other Status on an error.
  virtual Status Get(const ReadOptions& options, const Slice& key, Slice* value,
                     char* scratch, size_t scratch_size) = 0;

  // Return a heap-allocated iterator over the contents of the database.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  //
  // Caller should delete the iterator when it is no longer needed.
  // The returned iterator should be deleted before this db is deleted.
  virtual Iterator* NewIterator(const ReadOptions& options) = 0;

  // Return a handle to the current DB state.  Iterators created with
  // this handle will all observe a stable snapshot of the current DB
  // state.  The caller must call ReleaseSnapshot(result) when the
  // snapshot is no longer needed.
  virtual const Snapshot* GetSnapshot() = 0;

  // Release a previously acquired snapshot.  The caller must not
  // use "snapshot" after this call.
  virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;

  // DB implementations can export properties about their state
  // via this method.  If "property" is a valid property understood by this
  // DB implementation, fills "*value" with its current value and returns
  // true.  Otherwise returns false.
  //
  //
  // Valid property names include:
  //
  //  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
  //     where <N> is an ASCII representation of a level number (e.g. "0").
  //  "leveldb.stats" - returns a multi-line string that describes statistics
  //     about the internal operation of the DB.
  //  "leveldb.sstables" - returns a multi-line string that describes all
  //     of the sstables that make up the db contents.
  virtual bool GetProperty(const Slice& property, std::string* value) = 0;

  // For each i in [0,n-1], store in "sizes[i]", the approximate
  // file system space used by keys in "[range[i].start .. range[i].limit)".
  //
  // Note that the returned sizes measure file system space usage, so
  // if the user data compresses by a factor of ten, the returned
  // sizes will be one-tenth the size of the corresponding user data size.
  //
  // The results may not include the sizes of recently written data.
  virtual void GetApproximateSizes(const Range* range, int n,
                                   uint64_t* sizes) = 0;

  // Compact the underlying storage for the key range [*begin,*end].
  // In particular, deleted and overwritten versions are discarded,
  // and the data is rearranged to reduce the cost of operations
  // needed to access the data.  This operation should typically only
  // be invoked by users who understand the underlying implementation.
  //
  // begin==NULL is treated as a key before all keys in the database.
  // end==NULL is treated as a key after all keys in the database.
  // Therefore the following call will compact the entire database:
  //    db->CompactRange(NULL, NULL);
  virtual void CompactRange(const Slice* begin, const Slice* end) = 0;

  // Immediately force a minor compaction on the current MemTable.
  // Return OK on success, or a non-OK status on errors.
  virtual Status FlushMemTable(const FlushOptions& options) = 0;

  // Call "fsync" on the write-ahead log file.
  // Return OK on success, or a non-OK status on errors.
  virtual Status SyncWAL() = 0;

  // Insert native Table files under a specified directory into Level-0.
  // Return OK on success, or a non-OK status on errors.
  virtual Status AddL0Tables(const InsertOptions& options,
                             const std::string& src_dir) = 0;

  // Extract a logic range of keys into raw Table files that will be stored
  // under the specified dump directory.  If there is no key within the
  // specified range, no files will be generated.  If "min_seq" or "max_seq"
  // is not NULL, they are piggy-backed to the caller.
  // Return OK on success, or a non-OK status on errors.
  virtual Status Dump(const DumpOptions& options, const Range& range,
                      const std::string& dst_dir, SequenceNumber* min_seq,
                      SequenceNumber* max_seq) = 0;

 private:
  // No copying allowed
  void operator=(const DB&);
  DB(const DB&);
};

// Destroy the contents of the specified database.
// Be very careful using this method.
Status DestroyDB(const std::string& dbname, const DBOptions& options);

// If a DB cannot be opened, you may attempt to call this method to
// resurrect as much of the contents of the database as possible.
// Some data may be lost, so be careful when calling this function
// on a database that contains important information.
Status RepairDB(const std::string& dbname, const DBOptions& options);

}  // namespace pdlfs
