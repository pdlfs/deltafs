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

enum MXDBFormat {
  // Filename (last component of path) is in the value part of a KV pair.
  // A hash of the filename is stored as key suffix.
  // In this format, keys have a fixed length, but may require a very large hash
  // (64 or more bits per key) to ensure key uniqueness.
  // Having hashes in keys also simplifies directory splitting. Hash space is
  // trivial to partition and having a uniform hash function makes load
  // balancing straightforward. IndexFS uses this format.
  kNameInValue,
  // Filename is stored as key suffix.
  // TableFS uses this format.
  kNameInKey
};

// This is a set of templates for access filesystem metadata as KV pairs in a
// KV-store. Providing this as templates allows for different key types and DB
// implementations. The default DB implementation is a custom LevelDB
// realization of a LSM-tree. Examples of DB replacements include the
// original LevelDB, the RocksDB, and the WiredTiger realization of a LSM-tree.
// A big assumption is that all these DB implementations have a syntactically
// equivalent interface exposing operations including Get, Put, Delete, Write,
// NewIterator, GetSnapshot, and ReleaseSnapshot, using option structs such as
// ReadOptions and WriteOptions, and using an Iterator object for range queries.
template <typename DX = DB, MXDBFormat fmt = kNameInValue>
class MXDB {
 public:
  explicit MXDB(DX* dx) : dx_(dx) {}
  ~MXDB();

  template <typename TX>
  TX* __StartTx(bool with_snapshot) {
    TX* tx = new TX;
    if (with_snapshot) {
      tx->snap = dx_->GetSnapshot();
    } else {
      tx->snap = NULL;
    }
    return tx;
  }

  template <typename KX, typename TX, typename OPT>
  Status __Get(const DirId& id, const Slice& suf, Stat* stat, Slice* name,
               OPT* opt, TX* tx);
  template <typename KX, typename TX, typename OPT>
  Status __Set(const DirId& id, const Slice& suf, const Stat& stat,
               const Slice& name, OPT* opt, TX* tx);
  template <typename KX, typename TX, typename OPT>
  Status __Delete(const DirId& id, const Slice& suf, OPT* opt, TX* tx);

  template <typename Iter>
  struct Dir {
    uint32_t n;  // Number dir entries scanned
    std::string key_prefix;
    Iter* iter;
  };
  template <typename KX, typename TX, typename OPT, typename Iter>
  Dir<Iter>* __OpenDir(const DirId& id, OPT* opt, TX* tx);
  template <typename Iter>
  Status __ReadDir(Dir<Iter>* dir, Stat* stat, std::string* name);
  template <typename Iter>
  void __CloseDir(Dir<Iter>* dir);

  typedef std::vector<std::string> NameList;
  typedef std::vector<Stat> StatList;
  template <typename KX, typename TX, typename OPT>
  size_t __List(const DirId& id, StatList* stats, NameList* names, OPT* opt,
                TX* tx, size_t limit);
  template <typename KX, typename TX, typename OPT>
  bool __Exists(const DirId& id, const Slice& suf, OPT* opt, TX* tx);

  template <typename TX, typename OPT>
  Status __Commit(OPT* opt, TX* tx) {
    if (tx != NULL) {
      return dx_->Write(*opt, &tx->bat);
    } else {
      return Status::OK();
    }
  }

  template <typename TX>
  void __Release(TX* tx) {
    if (tx != NULL) {
      if (tx->snap != NULL) {
        dx_->ReleaseSnapshot(tx->snap);
      }
      delete tx;
    }
  }

 protected:
  DX* const dx_;
};

template <typename DX, MXDBFormat fmt>
MXDB<DX, fmt>::~MXDB() {}  // Not deleting DB as it is not owned by us

#if defined(DELTAFS)
#define KEY_INITIALIZER(id, tp) id.reg, id.snap, id.ino, tp
#else
#define KEY_INITIALIZER(id, tp) id.ino, tp
#endif

template <typename DX, MXDBFormat fmt>
template <typename KX, typename TX, typename OPT>
Status MXDB<DX, fmt>::__Set(const DirId& id, const Slice& suf, const Stat& stat,
                            const Slice& name, OPT* opt, TX* tx) {
  Status s;
  KX key(KEY_INITIALIZER(id, kDirEntType));
  key.SetSuffix(suf);
  // If value size < 200, we use an immediate buf space.
  // Otherwise, we use dynamic mem.
  std::string buf;
  char tmp[200];
  Slice value;
  Slice stat_encoding = stat.EncodeTo(tmp);
  if (fmt == kNameInKey) {
    value = stat_encoding;
  } else if (5 + name.size() < sizeof(tmp) - stat_encoding.size()) {
    char* const begin = tmp;
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
    s = dx_->Put(*opt, key.Encode(), value);
  } else {
    tx->bat.Put(key.Encode(), value);
  }
  return s;
}

template <typename DX, MXDBFormat fmt>
template <typename KX, typename TX, typename OPT>
Status MXDB<DX, fmt>::__Get(const DirId& id, const Slice& suf, Stat* stat,
                            Slice* name, OPT* opt, TX* tx) {
  Status s;
  KX key(KEY_INITIALIZER(id, kDirEntType));
  key.SetSuffix(suf);
  std::string tmp;
  if (tx != NULL) {
    opt->snapshot = tx->snap;
  }
  s = dx_->Get(*opt, key.Encode(), &tmp);
  if (s.ok()) {
    Slice input(tmp);
    if (!stat->DecodeFrom(&input)) {
      s = Status::Corruption(Slice());
    } else {
      if (fmt == kNameInKey) {
        *name = suf;
      } else if (!GetLengthPrefixedSlice(&input, name)) {
        s = Status::Corruption(Slice());
      }
    }
  }
  return s;
}

template <typename DX, MXDBFormat fmt>
template <typename KX, typename TX, typename OPT>
Status MXDB<DX, fmt>::__Delete(const DirId& id, const Slice& suf, OPT* opt,
                               TX* tx) {
  Status s;
  KX key(KEY_INITIALIZER(id, kDirEntType));
  key.SetSuffix(suf);
  if (tx == NULL) {
    s = dx_->Delete(*opt, key.Encode());
  } else {
    tx->bat.Delete(key.Encode());
  }
  return s;
}

template <typename DX, MXDBFormat fmt>
template <typename KX, typename TX, typename OPT, typename Iter>
typename MXDB<DX, fmt>::template Dir<Iter>* MXDB<DX, fmt>::__OpenDir(
    const DirId& id, OPT* opt, TX* tx) {
  KX key(KEY_INITIALIZER(id, kDirEntType));
  if (tx != NULL) {
    opt->snapshot = tx->snap;
  }
  Slice prefix = key.prefix();
  Iter* const iter = dx_->NewIterator(*opt);
  if (iter == NULL) {
    return NULL;
  }

  // Seek to position.
  iter->Seek(prefix);  // Deferring status checks until ReadDir.
  Dir<Iter>* dir = new Dir<Iter>;
  dir->key_prefix = prefix.ToString();
  dir->iter = iter;
  dir->n = 0;
  return dir;
}

template <typename DX, MXDBFormat fmt>
template <typename Iter>
Status MXDB<DX, fmt>::__ReadDir(Dir<Iter>* dir, Stat* stat, std::string* name) {
  if (dir == NULL) return Status::NotFound(Slice());
  Iter* const iter = dir->iter;
  if (!iter->Valid()) {
    if (iter->status().ok()) {
      return Status::NotFound(Slice());
    } else {
      //
    }
  }

  Slice input = iter->value();
  Slice key = iter->key();
  if (!key.starts_with(dir->key_prefix))  // Hitting the end of directory
    return Status::NotFound(Slice());
  if (!stat->DecodeFrom(&input)) {
    return Status::Corruption("Cannot parse Stat");
  }

  Slice filename;

  if (fmt == kNameInKey) {
    key.remove_prefix(dir->key_prefix.length());
    filename = key;
  } else if (!GetLengthPrefixedSlice(&input, &filename)) {
    return Status::Corruption("Cannot parse filename");
  }

  *name = filename.ToString();
  dir->n++;  // +1 entries scanned
  // Seek to the next entry
  iter->Next();
}

template <typename DX, MXDBFormat fmt>
template <typename Iter>
void MXDB<DX, fmt>::__CloseDir(Dir<Iter>* dir) {
  if (dir != NULL) {
    delete dir->iter;
    delete dir;
  }
}

template <typename DX, MXDBFormat fmt>
template <typename KX, typename TX, typename OPT>
size_t MXDB<DX, fmt>::__List(const DirId& id, StatList* stats, NameList* names,
                             OPT* opt, TX* tx, size_t limit) {
  KX key(KEY_INITIALIZER(id, kDirEntType));
  if (tx != NULL) {
    opt->snapshot = tx->snap;
  }
  Slice prefix = key.prefix();
  Iterator* iter = dx_->NewIterator(*opt);
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

template <typename DX, MXDBFormat fmt>
template <typename KX, typename TX, typename OPT>
bool MXDB<DX, fmt>::__Exists(const DirId& id, const Slice& suf, OPT* opt,
                             TX* tx) {
  Status s;
  KX key(KEY_INITIALIZER(id, kDirEntType));
  key.SetSuffix(suf);
  if (tx != NULL) {
    opt->snapshot = tx->snap;
  }
  std::string ignored;
  s = dx_->Get(*opt, key.Encode(), &ignored);
  return (s.ok());
}

#undef KEY_INITIALIZER

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

  // Finish a Tx by submitting all its writes
  Status Commit(Tx* tx) {
    WriteOptions options;
    return __Commit<Tx, WriteOptions>(&options, tx);
  }

  void Release(Tx* tx) {  // Discard a Tx
    __Release<Tx>(tx);
  }

 private:
  MDBOptions options_;
  void operator=(const MDB&);  // No copying allowed
  MDB(const MDB&);
};

}  // namespace pdlfs
