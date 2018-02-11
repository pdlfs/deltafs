/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */
#pragma once

#include "pdlfs-common/leveldb/db/db.h"

namespace pdlfs {

// An empty DB that implements nothing.
class EmptyDB : public DB {
 public:
  EmptyDB() {}
  virtual ~EmptyDB() {}
  virtual Status Put(const WriteOptions& o, const Slice& k, const Slice& v) {
    return DB::Put(o, k, v);
  }
  virtual Status Delete(const WriteOptions& o, const Slice& key) {
    return DB::Delete(o, key);
  }
  virtual Status Get(const ReadOptions& o, const Slice& key,
                     std::string* value) {
    return Status::NotFound(key);
  }
  virtual Status Get(const ReadOptions& o, const Slice& key, Slice* value,
                     char* scratch, size_t scratch_size) {
    return Status::NotFound(key);
  }
  virtual Iterator* NewIterator(const ReadOptions& o) {
    return NewEmptyIterator();
  }
  virtual const Snapshot* GetSnapshot() { return NULL; }
  virtual void ReleaseSnapshot(const Snapshot* snapshot) {}
  virtual Status Write(const WriteOptions& o, WriteBatch* batch) {
    return Status::BufferFull(Slice());
  }
  virtual bool GetProperty(const Slice& property, std::string* value) {
    return false;
  }
  virtual void GetApproximateSizes(const Range* r, int n, uint64_t* sizes) {
    for (int i = 0; i < n; i++) {
      sizes[i] = 0;
    }
  }
  virtual void CompactRange(const Slice* start, const Slice* end) {}
  virtual Status WaitForCompactions() { return Status::OK(); }
  virtual Status FlushMemTable(const FlushOptions& o) {
    return Status::BufferFull(Slice());
  }
  virtual Status SyncWAL() { return Status::BufferFull(Slice()); }
  virtual Status AddL0Tables(const InsertOptions& o, const std::string& dir) {
    return Status::BufferFull(Slice());
  }
  virtual Status Dump(const DumpOptions& o, const Range& range,
                      const std::string& dir, SequenceNumber* min_seq,
                      SequenceNumber* max_seq) {
    return Status::OK();
  }
};

}  // namespace pdlfs
