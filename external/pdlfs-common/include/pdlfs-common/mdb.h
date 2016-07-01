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

class DirIndex;  // GIGA index

struct DirInfo;
struct DirId {
  DirId() {}
  DirId(uint64_t ino) : ino(ino) {}
  DirId(uint64_t reg, uint64_t snap, uint64_t ino)
      : reg(reg), snap(snap), ino(ino) {}

  uint64_t reg;
  uint64_t snap;
  uint64_t ino;
};

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

  Status GetIdx(const DirId& id, DirIndex* idx, Tx* tx);
  Status SetIdx(const DirId& id, const DirIndex& idx, Tx* tx);
  Status DelIdx(const DirId& id, Tx* tx);

  Status GetNode(const DirId& id, const Slice& hash, Stat* stat, Slice* name,
                 Tx* tx);
  Status SetNode(const DirId& id, const Slice& hash, const Stat& stat,
                 const Slice& name, Tx* tx);
  Status DelNode(const DirId& id, const Slice& hash, Tx* tx);

  Status GetInfo(const DirId& id, DirInfo* info, Tx* tx);
  Status SetInfo(const DirId& id, const DirInfo& info, Tx* tx);
  Status DelInfo(const DirId& id, Tx* tx);

  typedef std::vector<std::string> NameList;
  typedef std::vector<Stat> StatList;
  int List(const DirId& id, StatList* stats, NameList* names, Tx* tx);
  bool Exists(const DirId& id, const Slice& hash, Tx* tx);

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

 private:
  DB* db_;
};

}  // namespace pdlfs
