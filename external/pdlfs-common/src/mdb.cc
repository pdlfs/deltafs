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
// Tablefs has its own MDB definitions, so we won't define it.
#if defined(DELTAFS) || defined(INDEXFS)
MDBOptions::MDBOptions()
    : fill_cache(false), verify_checksums(false), sync(false), db(NULL) {}

MDBStats::MDBStats()
    : putkeybytes(0),
      putbytes(0),
      puts(0),
      getkeybytes(0),
      getbytes(0),
      gets(0) {}

MDB::MDB(const MDBOptions& opts) : MXDB(opts.db) {}

MDB::~MDB() {}

#if defined(DELTAFS)
#define KEY_INITIALIZER(id, tp) id.reg, id.snap, id.ino, tp
#else
#define KEY_INITIALIZER(id, tp) id.ino, tp
#endif

Status MDB::GetNode(const DirId& id, const Slice& hash, Stat* stat,
                    std::string* name, Tx* tx) {
  ReadOptions read_options;
  read_options.verify_checksums = options_.verify_checksums;
  read_options.fill_cache = options_.fill_cache;
  return GET<Key>(id, hash, stat, name, &read_options, tx, (MDBStats*)NULL);
}

Status MDB::GetDirIdx(const DirId& id, DirIndex* idx, Tx* tx) {
  Status s;
  Key key(KEY_INITIALIZER(id, kDirIdxType));
  std::string tmp;
  ReadOptions read_options;
  read_options.verify_checksums = options_.verify_checksums;
  read_options.fill_cache = options_.fill_cache;
  if (tx != NULL) {
    read_options.snapshot = tx->snap;
  }
  s = dx_->Get(read_options, key.prefix(), &tmp);
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
  ReadOptions read_options;
  read_options.verify_checksums = options_.verify_checksums;
  read_options.fill_cache = options_.fill_cache;
  if (tx != NULL) {
    read_options.snapshot = tx->snap;
  }
  Slice result;
  s = dx_->Get(read_options, key.prefix(), &result, tmp, sizeof(tmp));
  if (s.ok()) {
    if (!info->DecodeFrom(&result)) {
      s = Status::Corruption(Slice());
    }
  }
  return s;
}

Status MDB::SetNode(const DirId& id, const Slice& hash, const Stat& stat,
                    const Slice& name, Tx* tx) {
  WriteOptions write_options;
  write_options.sync = options_.sync;
  return PUT<Key>(id, hash, stat, name, &write_options, tx, (MDBStats*)NULL);
}

Status MDB::SetDirIdx(const DirId& id, const DirIndex& idx, Tx* tx) {
  Status s;
  Key key(KEY_INITIALIZER(id, kDirIdxType));
  Slice encoding = idx.Encode();
  if (tx == NULL) {
    WriteOptions options;
    options.sync = options_.sync;
    s = dx_->Put(options, key.prefix(), encoding);
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
    s = dx_->Put(options, key.prefix(), encoding);
  } else {
    tx->bat.Put(key.prefix(), encoding);
  }
  return s;
}

Status MDB::DelNode(const DirId& id, const Slice& hash, Tx* tx) {
  WriteOptions write_options;
  return DELETE<Key>(id, hash, &write_options, tx);
}

Status MDB::DelDirIdx(const DirId& id, Tx* tx) {
  Status s;
  Key key(KEY_INITIALIZER(id, kDirIdxType));
  if (tx == NULL) {
    WriteOptions options;
    options.sync = options_.sync;
    s = dx_->Delete(options, key.prefix());
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
    s = dx_->Delete(options, key.prefix());
  } else {
    tx->bat.Delete(key.prefix());
  }
  return s;
}

size_t MDB::List(const DirId& id, StatList* stats, NameList* names, Tx* tx,
                 size_t limit) {
  ReadOptions read_options;
  read_options.verify_checksums = options_.verify_checksums;
  read_options.fill_cache = false;
  return LIST<Iterator, Key>(id, stats, names, &read_options, tx, limit);
}

bool MDB::Exists(const DirId& id, const Slice& hash, Tx* tx) {
  ReadOptions read_options;
  read_options.verify_checksums = options_.verify_checksums;
  read_options.fill_cache = options_.fill_cache;
  // No need to read any prefix of the value
  read_options.limit = 0;
  Status s = EXISTS<Key>(id, hash, &read_options, tx);
  return s.ok();
}

#endif
}  // namespace pdlfs
