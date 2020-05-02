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
#include "pdlfs-common/fsdbx.h"
#include "pdlfs-common/fstypes.h"
#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/readonly.h"
#include "pdlfs-common/leveldb/snapshot.h"
#include "pdlfs-common/leveldb/write_batch.h"
#include "pdlfs-common/status.h"

namespace pdlfs {
// Tablefs has its own MDB definitions, so we won't define it.
#if defined(DELTAFS) || defined(INDEXFS)
class DirIndex;  // GIGA index

struct MDBOptions {
  MDBOptions();
  // Always set fill_cache to the following for all ReadOptions.
  // Default: false
  bool fill_cache;
  // Always set verify_checksums to the following for all ReadOptions.
  // Default: false
  bool verify_checksums;
  // Always set sync to the following for all WriteOptions.
  // Default: false
  bool sync;
  // The underlying KV-store.
  DB* db;
};

class MDB : public MXDB<> {
 public:
  explicit MDB(const MDBOptions& opts);
  ~MDB();

  struct Tx {
    Tx() {}  // Note that snap is initialized via Create Tx
    const Snapshot* snap;
    WriteBatch bat;
  };
  Tx* CreateTx(bool snap = true) {  // Start a new Tx
    return STARTTX<Tx>(snap);
  }

  Status GetNode(const DirId& id, const Slice& hash, Stat* stat,
                 std::string* name, Tx* tx);
  Status SetNode(const DirId& id, const Slice& hash, const Stat& stat,
                 const Slice& name, Tx* tx);
  Status DelNode(const DirId& id, const Slice& hash, Tx* tx);

  Status GetDirIdx(const DirId& id, DirIndex* idx, Tx* tx);
  Status SetDirIdx(const DirId& id, const DirIndex& idx, Tx* tx);
  Status DelDirIdx(const DirId& id, Tx* tx);

  Status GetInfo(const DirId& id, DirInfo* info, Tx* tx);
  Status SetInfo(const DirId& id, const DirInfo& info, Tx* tx);
  Status DelInfo(const DirId& id, Tx* tx);

  size_t List(const DirId& id, StatList* stats, NameList* names, Tx* tx,
              size_t limit);
  bool Exists(const DirId& id, const Slice& hash, Tx* tx);

  // Finish a Tx by submitting all its writes
  Status Commit(Tx* tx) {
    WriteOptions options;
    return COMMIT<Tx, WriteOptions>(&options, tx);
  }

  void Release(Tx* tx) {  // Discard a Tx
    RELEASE<Tx>(tx);
  }

 private:
  MDBOptions options_;
  void operator=(const MDB&);  // No copying allowed
  MDB(const MDB&);
};

#endif
}  // namespace pdlfs
