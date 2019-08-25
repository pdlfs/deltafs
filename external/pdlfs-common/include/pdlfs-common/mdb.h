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

#if defined(DELTAFS) || defined(INDEXFS)
class DirIndex;  // GIGA index
#endif

struct DirInfo;
struct DirId {
  DirId();
#if !defined(DELTAFS)
  explicit DirId(uint64_t ino);
#endif
  explicit DirId(const Stat& stat)
      : reg(stat.RegId()), snap(stat.SnapId()), ino(stat.InodeNo()) {}
  explicit DirId(const LookupStat& stat)
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
  if (x.reg != y.reg) return false;
  if (x.snap != y.snap) return false;
#endif

  return (x.ino == y.ino);
}

inline bool operator!=(const DirId& x, const DirId& y) {
  return !(x == y);  // Reuse operator==
}

template <typename DX = DB>
struct MXDBOptions {
  MXDBOptions();
  // Default: false
  bool verify_checksums;
  // Default: false
  bool sync;

  DX* db;
};

template <typename DX>
MXDBOptions<DX>::MXDBOptions()
    : verify_checksums(false), sync(false), db(NULL) {}

// This is template for access filesystem metadata as KV pairs in a KV-store.
// Providing this as template allows for DB implementations. The default DB
// implementation is the LevelDB realization of a LSM-tree.
template <typename DX = DB>
class MXDB {
 public:
  explicit MXDB(const MXDBOptions<DX>& opts) : options_(opts), db_(opts.db) {
    assert(db_ != NULL);
  }
  ~MXDB();

  template <typename TX>
  TX* __StartTx(bool with_snapshot) {
    TX* tx = new TX;
    if (with_snapshot) {
      tx->snap = db_->GetSnapshot();
    } else {
      tx->snap = NULL;
    }
    return tx;
  }

  template <typename KX, typename TX, typename OPT>
  Status __Get(const DirId& id, const Slice& suf, Stat* stat, Slice* name,
               TX* tx);
  template <typename KX, typename TX, typename OPT>
  Status __Set(const DirId& id, const Slice& suf, const Stat& stat,
               const Slice& name, TX* tx);
  template <typename KX, typename TX, typename OPT>
  Status __Delete(const DirId& id, const Slice& suf, TX* tx);

  typedef std::vector<std::string> NameList;
  typedef std::vector<Stat> StatList;
  template <typename KX, typename TX, typename OPT>
  size_t __List(const DirId& id, StatList* stats, NameList* names, TX* tx,
                size_t limit);
  template <typename KX, typename TX, typename OPT>
  bool __Exists(const DirId& id, const Slice& suf, TX* tx);

  template <typename TX, typename OPT>
  Status __Commit(TX* tx) {
    if (tx != NULL) {
      OPT options;
      options.sync = options_.sync;
      return db_->Write(options, &tx->bat);
    } else {
      return Status::OK();
    }
  }

  template <typename TX>
  void __Release(TX* tx) {
    if (tx != NULL) {
      if (tx->snap != NULL) {
        db_->ReleaseSnapshot(tx->snap);
      }
      delete tx;
    }
  }

 protected:
  MXDBOptions<DX> options_;
  DX* const db_;
};

template <typename DX>
MXDB<DX>::~MXDB() {}  // Not deleting DB as it is not owned by us

#if defined(DELTAFS)
#define KEY_INITIALIZER(id, tp) id.reg, id.snap, id.ino, tp
#else
#define KEY_INITIALIZER(id, tp) id.ino, tp
#endif

template <typename DX>
template <typename KX, typename TX, typename OPT>
Status MXDB<DX>::__Set(const DirId& id, const Slice& suf, const Stat& stat,
                       const Slice& name, TX* tx) {
  Status s;
  KX key(KEY_INITIALIZER(id, kDirEntType));
  key.SetHash(suf);
  // If value size < 200, we use an immediate buf space.
  // Otherwise, we use dynamic mem.
  std::string buf;
  char tmp[200];
  Slice value;
  Slice stat_encoding = stat.EncodeTo(tmp);
  if (name.size() < sizeof(tmp) - stat_encoding.size() - 5) {
    char* begin = tmp;
    char* end = begin + stat_encoding.size();
    end = EncodeLengthPrefixedSlice(end, name);
    value = Slice(begin, end - begin);
  } else {
    buf.append(stat_encoding.data(), stat_encoding.size());
    PutLengthPrefixedSlice(&buf, name);
    value = Slice(buf);
  }
  // If TX is present, write into the TX's internal batch.
  // Otherwise, directly apply to DB.
  if (tx == NULL) {
    OPT options;
    options.sync = options_.sync;
    s = db_->Put(options, key.Encode(), value);
  } else {
    tx->bat.Put(key.Encode(), value);
  }
  return s;
}

template <typename DX>
template <typename KX, typename TX, typename OPT>
Status MXDB<DX>::__Get(const DirId& id, const Slice& suf, Stat* stat,
                       Slice* name, TX* tx) {
  Status s;
  KX key(KEY_INITIALIZER(id, kDirEntType));
  key.SetHash(suf);
  std::string tmp;
  OPT options;
  options.verify_checksums = options_.verify_checksums;
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

template <typename DX>
template <typename KX, typename TX, typename OPT>
Status MXDB<DX>::__Delete(const DirId& id, const Slice& suf, TX* tx) {
  Status s;
  KX key(KEY_INITIALIZER(id, kDirEntType));
  key.SetHash(suf);
  if (tx == NULL) {
    OPT options;
    options.sync = options_.sync;
    s = db_->Delete(options, key.Encode());
  } else {
    tx->bat.Delete(key.Encode());
  }
  return s;
}

template <typename DX>
template <typename KX, typename TX, typename OPT>
size_t MXDB<DX>::__List(const DirId& id, StatList* stats, NameList* names,
                        TX* tx, size_t limit) {
  KX key(KEY_INITIALIZER(id, kDirEntType));
  OPT options;
  options.verify_checksums = options_.verify_checksums;
  options.fill_cache = false;
  if (tx != NULL) {
    options.snapshot = tx->snap;
  }
  Slice prefix = key.prefix();
  Iterator* iter = db_->NewIterator(options);
  iter->Seek(prefix);
  Slice name;
  Stat stat;
  size_t num_entries = 0;
  for (; iter->Valid() && num_entries < limit; iter->Next()) {
    Slice k = iter->key();
    if (k.starts_with(prefix)) {
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

template <typename DX>
template <typename KX, typename TX, typename OPT>
bool MXDB<DX>::__Exists(const DirId& id, const Slice& suf, TX* tx) {
  Status s;
  KX key(KEY_INITIALIZER(id, kDirEntType));
  key.SetHash(suf);
  OPT options;
  options.verify_checksums = options_.verify_checksums;
  options.limit = 0;
  if (tx != NULL) {
    options.snapshot = tx->snap;
  }
  Slice ignored;
  char tmp[1];
  s = db_->Get(options, key.Encode(), &ignored, tmp, sizeof(tmp));
  assert(!s.IsBufferFull());
  if (!s.ok()) {
    return false;
  } else {
    return true;
  }
}

#undef KEY_INITIALIZER

struct MDBOptions : public MXDBOptions<DB> {
  MDBOptions();
};

class MDB : public MXDB<DB> {
 public:
  explicit MDB(const MDBOptions& opts);
  ~MDB();

  struct Tx {
    Tx() {}  // Note that snap is initialized via Create Tx
    const Snapshot* snap;
    WriteBatch bat;
  };
  Tx* CreateTx(bool snap = true) {  // Start a new Tx
    return __StartTx<Tx>(snap);
  }

  Status GetNode(const DirId& id, const Slice& hash, Stat* stat, Slice* name,
                 Tx* tx);
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

  Status Commit(Tx* tx) {  // Finish a Tx by submitting all its writes
    return __Commit<Tx, WriteOptions>(tx);
  }

  void Release(Tx* tx) {  // Discard a Tx
    __Release<Tx>(tx);
  }
};

}  // namespace pdlfs
