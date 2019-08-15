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
#if !defined(DELTAFS)
  DirId(uint64_t ino) : ino(ino) {}
#endif
  DirId(const Stat& stat)
      : reg(stat.RegId()), snap(stat.SnapId()), ino(stat.InodeNo()) {}
  DirId(const LookupStat& stat)
      : reg(stat.RegId()), snap(stat.SnapId()), ino(stat.InodeNo()) {}
  DirId(uint64_t reg, uint64_t snap, uint64_t ino)
      : reg(reg), snap(snap), ino(ino) {}

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "other",
  //   == 0 iff "*this" == "other",
  //   >  0 iff "*this" >  "other"
  int compare(const DirId& other) const;
  std::string DebugString() const;

  uint64_t reg;
  uint64_t snap;
  uint64_t ino;
};

inline bool operator==(const DirId& x, const DirId& y) {
#if defined(DELTAFS)
  if (x.reg != y.reg) {
    return false;
  } else {
    if (x.snap != y.snap) {
      return false;
    }
  }
#endif

  return (x.ino == y.ino);
}

inline bool operator!=(const DirId& x, const DirId& y) {
  return !(x == y);  // Works for all file system ports
}

inline int DirId::compare(const DirId& other) const {
#if defined(DELTAFS)
  int r = reg - other.reg;
  if (r != 0) {
    return r;
  } else {
    r = snap - other.snap;
    if (r != 0) {
      return r;
    }
  }
#endif

  return (ino - other.ino);
}

struct MDBOptions {
  MDBOptions();
  bool verify_checksums;  // Default: false
  bool sync;              // Default: false
  DB* db;
};

// A helper class used by deltafs and indexfs to access file system
// metadata stored as key-value pairs inside LevelDB.
class MDB {
 public:
  MDB(const MDBOptions& opts) : options_(opts), db_(opts.db) {
    assert(db_ != NULL);
  }
  ~MDB();

  struct Tx {
    const Snapshot* snap;
    WriteBatch bat;
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
  size_t List(const DirId& id, StatList* stats, NameList* names, Tx* tx,
              size_t limit);
  bool Exists(const DirId& id, const Slice& hash, Tx* tx);

  Status Commit(Tx* tx) {
    if (tx != NULL) {
      WriteOptions options;
      options.sync = options_.sync;
      return db_->Write(options, &tx->bat);
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
  MDBOptions options_;
  DB* db_;
};

}  // namespace pdlfs
