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

#include "pdlfs-common/mdb.h"
#include "pdlfs-common/dcntl.h"
#include "pdlfs-common/gigaplus.h"

namespace pdlfs {

std::string DirId::DebugString() const {
#define LLU(x) static_cast<unsigned long long>(x)
  char tmp[30];
#if defined(DELTAFS)
  snprintf(tmp, sizeof(tmp), "dirid[%llu:%llu:%llu]", LLU(reg), LLU(snap),
           LLU(ino));
#else
  snprintf(tmp, sizeof(tmp), "dirid[%llu]", LLU(ino));
#endif
  return tmp;
}

DirId::DirId() : reg(0), snap(0), ino(0) {}
#if !defined(DELTAFS)
DirId::DirId(uint64_t ino) : reg(0), snap(0), ino(ino) {}
#endif

namespace {
int compare64(uint64_t a, uint64_t b) {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}
}  // namespace
int DirId::compare(const DirId& other) const {
  int r;
#if defined(DELTAFS)
  r = compare64(reg, other.reg);
  if (r != 0) return r;
  r = compare64(snap, other.snap);
  if (r != 0) {
    return r;
  }
#endif

  r = compare64(ino, other.ino);
  return r;
}

MDBOptions::MDBOptions() : MXDBOptions() {}

MDB::MDB(const pdlfs::MDBOptions& opts) : MXDB(opts) {}

MDB::~MDB() {}

#if defined(DELTAFS)
#define KEY_INITIALIZER(id, tp) id.reg, id.snap, id.ino, tp
#else
#define KEY_INITIALIZER(id, tp) id.ino, tp
#endif

Status MDB::GetNode(const DirId& id, const Slice& hash, Stat* stat, Slice* name,
                    Tx* tx) {
  return __Get<Key, Tx, ReadOptions>(id, hash, stat, name, tx);
}

Status MDB::GetDirIdx(const DirId& id, DirIndex* idx, Tx* tx) {
  Status s;
  Key key(KEY_INITIALIZER(id, kDirIdxType));
  std::string tmp;
  ReadOptions options;
  options.verify_checksums = options_.verify_checksums;
  if (tx != NULL) {
    options.snapshot = tx->snap;
  }
  s = db_->Get(options, key.prefix(), &tmp);
  if (s.ok()) {
    if (!idx->Update(tmp)) {
      s = Status::Corruption(Slice());
    }
  }
  return s;
}

Status MDB::GetInfo(const DirId& id, DirInfo* info, Tx* tx) {
  Status s;
  Key key(KEY_INITIALIZER(id, kDirMetaType));
  char tmp[20];
  ReadOptions options;
  options.verify_checksums = options_.verify_checksums;
  if (tx != NULL) {
    options.snapshot = tx->snap;
  }
  Slice result;
  s = db_->Get(options, key.prefix(), &result, tmp, sizeof(tmp));
  if (s.ok()) {
    if (!info->DecodeFrom(&result)) {
      s = Status::Corruption(Slice());
    }
  }
  return s;
}

Status MDB::SetNode(const DirId& id, const Slice& hash, const Stat& stat,
                    const Slice& name, Tx* tx) {
  return __Set<Key, Tx, WriteOptions>(id, hash, stat, name, tx);
}

Status MDB::SetDirIdx(const DirId& id, const DirIndex& idx, Tx* tx) {
  Status s;
  Key key(KEY_INITIALIZER(id, kDirIdxType));
  Slice encoding = idx.Encode();
  if (tx == NULL) {
    WriteOptions options;
    options.sync = options_.sync;
    s = db_->Put(options, key.prefix(), encoding);
  } else {
    tx->bat.Put(key.prefix(), encoding);
  }
  return s;
}

Status MDB::SetInfo(const DirId& id, const DirInfo& info, Tx* tx) {
  Status s;
  Key key(KEY_INITIALIZER(id, kDirMetaType));
  char tmp[20];
  Slice encoding = info.EncodeTo(tmp);
  if (tx == NULL) {
    WriteOptions options;
    options.sync = options_.sync;
    s = db_->Put(options, key.prefix(), encoding);
  } else {
    tx->bat.Put(key.prefix(), encoding);
  }
  return s;
}

Status MDB::DelNode(const DirId& id, const Slice& hash, Tx* tx) {
  return __Delete<Key, Tx, WriteOptions>(id, hash, tx);
}

Status MDB::DelDirIdx(const DirId& id, Tx* tx) {
  Status s;
  Key key(KEY_INITIALIZER(id, kDirIdxType));
  if (tx == NULL) {
    WriteOptions options;
    options.sync = options_.sync;
    s = db_->Delete(options, key.prefix());
  } else {
    tx->bat.Delete(key.prefix());
  }
  return s;
}

Status MDB::DelInfo(const DirId& id, Tx* tx) {
  Status s;
  Key key(KEY_INITIALIZER(id, kDirMetaType));
  if (tx == NULL) {
    WriteOptions options;
    options.sync = options_.sync;
    s = db_->Delete(options, key.prefix());
  } else {
    tx->bat.Delete(key.prefix());
  }
  return s;
}

size_t MDB::List(const DirId& id, StatList* stats, NameList* names, Tx* tx,
                 size_t limit) {
  return __List<Key, Tx, ReadOptions>(id, stats, names, tx, limit);
}

bool MDB::Exists(const DirId& id, const Slice& hash, Tx* tx) {
  return __Exists<Key, Tx, ReadOptions>(id, hash, tx);
}

}  // namespace pdlfs
