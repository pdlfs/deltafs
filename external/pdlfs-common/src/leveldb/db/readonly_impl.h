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
#pragma once

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/internal_types.h"
#include "pdlfs-common/leveldb/readonly.h"
#include "pdlfs-common/log_reader.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

class TableCache;
class Version;
class VersionSet;

// An alternative DB implementation that allows concurrent readonly access
// to a db image and performs only a partial recovery process during startup
// which doesn't include a compaction of the MANIFEST file and
// a replay of memtable logs.
// Multiple readonly db instances can share a same db image and
// can follow a read-write instance to access new updates.
class ReadonlyDBImpl : public ReadonlyDB {
 public:
  ReadonlyDBImpl(const Options& options, const std::string& dbname);
  virtual ~ReadonlyDBImpl();

  virtual Status Load();
  virtual Status Reload();
  virtual Status Get(const ReadOptions&, const Slice& key, std::string* value);
  virtual Status Get(const ReadOptions&, const Slice& key, Slice* value,
                     char* scratch, size_t scratch_size);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual Status Dump(const DumpOptions&, const Range& range,
                      const std::string& dir, SequenceNumber* min_seq,
                      SequenceNumber* max_seq);

 private:
  friend class ReadonlyDB;

  Status InternalGet(const ReadOptions&, const Slice& key, Buffer* buf);
  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot);

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  bool owns_cache_;
  bool owns_table_cache_;
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache* table_cache_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  VersionSet* versions_;
  SequentialFile* logfile_;
  log::Reader* log_;

  // No copying allowed
  void operator=(const ReadonlyDBImpl&);
  ReadonlyDBImpl(const ReadonlyDBImpl&);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
};

}  // namespace pdlfs
