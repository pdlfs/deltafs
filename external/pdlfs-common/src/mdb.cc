/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
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

MDB::~MDB() {}

Status MDB::Getidx(uint64_t ino, DirIndex* idx, Tx* tx) {
  Status s;
  Key key(ino, kDirIdxType);
  std::string tmp;
  ReadOptions options;
  if (tx != NULL) {
    options.snapshot = tx->snap;
  }
  s = db_->Get(options, key.prefix(), &tmp);
  if (s.ok()) {
    if (!idx->Reset(tmp)) {
      s = Status::Corruption(Slice());
    }
  }
  return s;
}

Status MDB::Getdir(uint64_t ino, DirInfo* dir, Tx* tx) {
  Status s;
  Key key(ino, kDirMetaType);
  char tmp[20];
  ReadOptions options;
  if (tx != NULL) {
    options.snapshot = tx->snap;
  }
  Slice result;
  s = db_->Get(options, key.prefix(), &result, tmp, sizeof(tmp));
  if (s.ok()) {
    if (!dir->DecodeFrom(&result)) {
      s = Status::Corruption(Slice());
    }
  }
  return s;
}

Status MDB::Getattr(uint64_t ino, const Slice& hash, Stat* stat, Slice* name,
                    Tx* tx) {
  Status s;
  Key key(ino, kDirEntType);
  key.SetHash(hash);
  std::string tmp;
  ReadOptions options;
  if (tx != NULL) {
    options.snapshot = tx->snap;
  }
  s = db_->Get(options, key.Encode(), &tmp);
  if (s.ok()) {
    Slice input(tmp);
    if (!stat->DecodeFrom(&input)) {
      s = Status::Corruption(Slice());
    } else if (!GetLengthPrefixedSlice(&input, name)) {
      s = Status::Corruption(Slice());
    }
  }
  return s;
}

Status MDB::Setidx(uint64_t ino, const DirIndex& idx, Tx* tx) {
  Status s;
  Key key(ino, kDirIdxType);
  Slice encoding = idx.Encode();
  if (tx == NULL) {
    WriteOptions options;
    s = db_->Put(options, key.prefix(), encoding);
  } else {
    tx->batch.Put(key.prefix(), encoding);
  }
  return s;
}

Status MDB::Setdir(uint64_t ino, const DirInfo& dir, Tx* tx) {
  Status s;
  Key key(ino, kDirMetaType);
  char tmp[20];
  Slice encoding = dir.EncodeTo(tmp);
  if (tx == NULL) {
    WriteOptions options;
    s = db_->Put(options, key.prefix(), encoding);
  } else {
    tx->batch.Put(key.prefix(), encoding);
  }
  return s;
}

Status MDB::Setattr(uint64_t ino, const Slice& hash, const Stat& stat,
                    const Slice& name, Tx* tx) {
  Status s;
  Key key(ino, kDirEntType);
  key.SetHash(hash);
  char tmp[4096 + sizeof(Stat)];
  Slice encoding = stat.EncodeTo(tmp);
  char* start = tmp;
  char* p = start + encoding.size();
  p = EncodeLengthPrefixedSlice(p, name);
  Slice value(start, p - start);
  if (tx == NULL) {
    WriteOptions options;
    s = db_->Put(options, key.Encode(), value);
  } else {
    tx->batch.Put(key.Encode(), value);
  }
  return s;
}

Status MDB::Delidx(uint64_t ino, Tx* tx) {
  Status s;
  Key key(ino, kDirIdxType);
  if (tx == NULL) {
    WriteOptions options;
    s = db_->Delete(options, key.prefix());
  } else {
    tx->batch.Delete(key.prefix());
  }
  return s;
}

Status MDB::Deldir(uint64_t ino, Tx* tx) {
  Status s;
  Key key(ino, kDirMetaType);
  if (tx == NULL) {
    WriteOptions options;
    s = db_->Delete(options, key.prefix());
  } else {
    tx->batch.Delete(key.prefix());
  }
  return s;
}

Status MDB::Delattr(uint64_t ino, const Slice& hash, Tx* tx) {
  Status s;
  Key key(ino, kDirEntType);
  key.SetHash(hash);
  if (tx == NULL) {
    WriteOptions options;
    s = db_->Delete(options, key.Encode());
  } else {
    tx->batch.Delete(key.Encode());
  }
  return s;
}

int MDB::List(uint64_t ino, StatList* stats, NameList* names, Tx* tx) {
  Key key(ino, kDirEntType);
  ReadOptions options;
  options.fill_cache = false;
  if (tx != NULL) {
    options.snapshot = tx->snap;
  }
  Slice prefix = key.prefix();
  Iterator* iter = db_->NewIterator(options);
  iter->Seek(prefix);
  Slice name;
  Stat stat;
  int num_entries = 0;
  for (; iter->Valid(); iter->Next()) {
    Slice key = iter->key();
    if (key.starts_with(prefix)) {
      Slice input = iter->value();
      if (stat.DecodeFrom(&input) && GetLengthPrefixedSlice(&input, &name)) {
        if (stats != NULL) {
          stats->push_back(stat);
        }
        if (names != NULL) {
          names->push_back(name.ToString());
        }
        num_entries++;
      }
    } else {
      break;
    }
  }
  delete iter;
  return num_entries;
}

bool MDB::Exists(uint64_t ino, const Slice& hash, Tx* tx) {
  Status s;
  Key key(ino, kDirEntType);
  key.SetHash(hash);
  ReadOptions options;
  options.limit = 0;
  if (tx != NULL) {
    options.snapshot = tx->snap;
  }
  Slice ignored;
  char tmp[1];
  s = db_->Get(options, key.Encode(), &ignored, tmp, sizeof(tmp));
  return s.ok();
}

}  // namespace pdlfs
