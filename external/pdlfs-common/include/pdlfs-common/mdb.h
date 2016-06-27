#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/env.h"
#include "pdlfs-common/fstypes.h"
#include "pdlfs-common/leveldb/db/db.h"
#include "pdlfs-common/leveldb/db/snapshot.h"
#include "pdlfs-common/leveldb/db/write_batch.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

class DirIndex;
class DirInfo;

// A helper class used by deltafs and indexfs to access file system
// metadata stored as key-value pairs inside LevelDB.
class MDB {
 public:
  explicit MDB(DB* db) : db_(db) {}
  ~MDB();

  struct Tx {
    const Snapshot* snap;
    WriteBatch batch;
  };
  Tx* CreateTx(bool with_snapshot = true) {
    Tx* tx = new Tx;
    if (with_snapshot) {
      tx->snap = db_->GetSnapshot();
    } else {
      tx->snap = NULL;
    }
    return tx;
  }
  Status Commit(Tx* tx) {
    if (tx != NULL) {
      return db_->Write(WriteOptions(), &tx->batch);
    } else {
      return Status::OK();
    }
  }
  void Release(Tx* tx) {
    if (tx != NULL) {
      if (tx->snap != NULL) {
        db_->ReleaseSnapshot(tx->snap);
      }
      delete tx;
    }
  }

  Status Getidx(uint64_t ino, DirIndex* idx, Tx* tx);
  Status Setidx(uint64_t ino, const DirIndex& idx, Tx* tx);
  Status Delidx(uint64_t ino, Tx* tx);

  Status Getattr(uint64_t ino, const Slice& hash, Stat* stat, Slice* name,
                 Tx* tx);
  Status Setattr(uint64_t ino, const Slice& hash, const Stat& stat,
                 const Slice& name, Tx* tx);
  Status Delattr(uint64_t ino, const Slice& hash, Tx* tx);

  Status Getdir(uint64_t ino, DirInfo* dir, Tx* tx);
  Status Setdir(uint64_t ino, const DirInfo& dir, Tx* tx);
  Status Deldir(uint64_t ino, Tx* tx);

  typedef std::vector<std::string> NameList;
  typedef std::vector<Stat> StatList;
  int List(uint64_t ino, StatList* stats, NameList* names, Tx* tx);
  bool Exists(uint64_t ino, const Slice& hash, Tx* tx);

 private:
  DB* db_;
};

}  // namespace pdlfs
