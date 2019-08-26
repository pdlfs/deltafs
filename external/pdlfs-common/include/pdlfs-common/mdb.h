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
template <typename DX = DB, typename xslice = Slice,  // Foreign slice
          typename xstatus =
              Status,  // Foreign db status type to which we must port
          MXDBFormat fmt = kNameInValue>
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
    size_t n;  // Number dir entries scanned
    std::string key_prefix;
    Iter* iter;
  };
  template <typename Iter, typename KX, typename TX, typename OPT>
  Dir<Iter>* __OpenDir(const DirId& id, OPT* opt, TX* tx);
  template <typename Iter>
  Status __ReadDir(Dir<Iter>* dir, Stat* stat, std::string* name);
  template <typename Iter>
  void __CloseDir(Dir<Iter>* dir);

  typedef std::vector<std::string> NameList;
  typedef std::vector<Stat> StatList;
  template <typename Iter, typename KX, typename TX, typename OPT>
  size_t __List(const DirId& id, StatList* stats, NameList* names, OPT* opt,
                TX* tx, size_t limit);
  template <typename KX, typename TX, typename OPT>
  Status __Exists(const DirId& id, const Slice& suf, OPT* opt, TX* tx);

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

#define MXDBTEMDECL(a, b, c, d) \
  template <typename a, typename b, typename c, MXDBFormat d>
MXDBTEMDECL(DX, xslice, xstatus, fmt)
MXDB<DX, xslice, xstatus, fmt>::~MXDB() {  ////
  // Empty.
}

// Quickly convert a foreign DB status to ours.
// Map IsNotFound as-is.
// Map all other status to IOError.
#define XSTATUS(x)                 \
  (x.IsNotFound()                  \
       ? Status::NotFound(Slice()) \
       : Status::IOError(x.ToString()))  // Pretty dirty. But works!
#if defined(DELTAFS)
#define KEY_INITIALIZER(id, tp) id.reg, id.snap, id.ino, tp
#else
#define KEY_INITIALIZER(id, tp) id.ino, tp
#endif

MXDBTEMDECL(DX, xslice, xstatus, fmt)
template <typename KX, typename TX, typename OPT>
Status MXDB<DX, xslice, xstatus, fmt>::__Set(  ////
    const DirId& id, const Slice& suf, const Stat& stat, const Slice& name,
    OPT* opt, TX* tx) {
  Status s;
  KX key(KEY_INITIALIZER(id, kDirEntType));
  key.SetSuffix(suf);
  xslice keyenc = xslice(key.data(), key.size());
  // If value size < 200, we use an immediate buf space.
  // Otherwise, we use dynamic mem.
  std::string buf;
  char tmp[200];
  xslice value;
  Slice stat_encoding = stat.EncodeTo(tmp);
  if (fmt == kNameInKey) {
    value = xslice(stat_encoding.data(), stat_encoding.size());
  } else if (5 + name.size() < sizeof(tmp) - stat_encoding.size()) {
    char* const begin = tmp;
    char* end = begin + stat_encoding.size();
    end = EncodeLengthPrefixedSlice(end, name);
    value = xslice(begin, end - begin);
  } else {
    buf.append(stat_encoding.data(), stat_encoding.size());
    PutLengthPrefixedSlice(&buf, name);
    value = xslice(buf);
  }

  // If TX is present, write into the TX's internal batch.
  // Otherwise, directly apply to DB.
  if (tx == NULL) {
    xstatus st = dx_->Put(*opt, keyenc, value);
    if (!st.ok()) {
      s = XSTATUS(st);
    }
  } else {
    tx->bat.Put(keyenc, value);
  }

  return s;
}

MXDBTEMDECL(DX, xslice, xstatus, fmt)
template <typename KX, typename TX, typename OPT>
Status MXDB<DX, xslice, xstatus, fmt>::__Get(  ////
    const DirId& id, const Slice& suf, Stat* stat, Slice* name, OPT* opt,
    TX* tx) {
  Status s;
  KX key(KEY_INITIALIZER(id, kDirEntType));
  key.SetSuffix(suf);
  xslice keyenc = xslice(key.data(), key.size());
  std::string tmp;
  if (tx != NULL) {
    opt->snapshot = tx->snap;
  }
  xstatus st = dx_->Get(*opt, keyenc, &tmp);
  if (st.ok()) {
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
  } else {
    s = XSTATUS(st);
  }
  return s;
}

MXDBTEMDECL(DX, xslice, xstatus, fmt)
template <typename KX, typename TX, typename OPT>
Status MXDB<DX, xslice, xstatus, fmt>::__Delete(  ////
    const DirId& id, const Slice& suf, OPT* opt, TX* tx) {
  Status s;
  KX key(KEY_INITIALIZER(id, kDirEntType));
  key.SetSuffix(suf);
  xslice keyenc = xslice(key.data(), key.size());
  if (tx == NULL) {
    xstatus st = dx_->Delete(*opt, keyenc);
    if (!st.ok()) {
      s = XSTATUS(st);
    }
  } else {
    tx->bat.Delete(keyenc);
  }
  return s;
}

MXDBTEMDECL(DX, xslice, xstatus, fmt)
template <typename Iter, typename KX, typename TX, typename OPT>
typename MXDB<DX, xslice, xstatus, fmt>::template Dir<Iter>*
MXDB<DX, xslice, xstatus, fmt>::__OpenDir(  ////
    const DirId& id, OPT* opt, TX* tx) {
  KX key_prefix(KEY_INITIALIZER(id, kDirEntType));
  if (tx != NULL) {
    opt->snapshot = tx->snap;
  }
  xslice prefix = xslice(key_prefix.data(), key_prefix.size());
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

MXDBTEMDECL(DX, xslice, xstatus, fmt)
template <typename Iter>
Status MXDB<DX, xslice, xstatus, fmt>::__ReadDir(  ////
    Dir<Iter>* dir, Stat* stat, std::string* name) {
  if (dir == NULL) return Status::NotFound(Slice());
  Iter* const iter = dir->iter;
  if (!iter->Valid()) {  // Either we hit the bottom or an error
    xstatus st = iter->status();
    if (st.ok()) {
      return Status::NotFound(Slice());
    } else {
      return XSTATUS(st);
    }
  }

  xslice xinput = iter->value();
  xslice xkey = iter->key();

  Slice input = Slice(xinput.data(), xinput.size());
  Slice key = Slice(xkey.data(), xkey.size());
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

MXDBTEMDECL(DX, xslice, xstatus, fmt)
template <typename Iter>
void MXDB<DX, xslice, xstatus, fmt>::__CloseDir(  ////
    Dir<Iter>* dir) {
  if (dir != NULL) {
    delete dir->iter;
    delete dir;
  }
}

MXDBTEMDECL(DX, xslice, xstatus, fmt)
template <typename Iter, typename KX, typename TX, typename OPT>
size_t MXDB<DX, xslice, xstatus, fmt>::__List(  ////
    const DirId& id, StatList* stats, NameList* names, OPT* opt, TX* tx,
    size_t limit) {
  KX prefix_key(KEY_INITIALIZER(id, kDirEntType));
  if (tx != NULL) {
    opt->snapshot = tx->snap;
  }
  Slice prefix = prefix_key.prefix();
  Iter* const iter = dx_->NewIterator(*opt);
  iter->Seek(prefix);
  Slice name;
  Stat stat;
  size_t num_entries = 0;
  for (; iter->Valid() && num_entries < limit; iter->Next()) {
    xslice xinput = iter->value();
    xslice xkey = iter->key();

    Slice input = Slice(xinput.data(), xinput.size());
    Slice key = Slice(xkey.data(), xkey.size());
    if (!key.starts_with(prefix))  // Hitting end of directory
      break;
    if (!stat.DecodeFrom(&input)) {
      break;  // Error
    }

    if (fmt == kNameInKey) {
      key.remove_prefix(prefix.size());
      name = key;
    } else if (!GetLengthPrefixedSlice(&input, &name)) {
      break;  // Error
    }

    if (names != NULL) names->push_back(name.ToString());
    if (stats != NULL) stats->push_back(stat);

    num_entries++;
  }

  // Sorry but we cannot reuse cursor positions.
  // Use ReadDir instead.
  delete iter;

  return num_entries;
}

MXDBTEMDECL(DX, xslice, xstatus, fmt)
template <typename KX, typename TX, typename OPT>
Status MXDB<DX, xslice, xstatus, fmt>::__Exists(  ////
    const DirId& id, const Slice& suf, OPT* opt, TX* tx) {
  Status s;
  KX key(KEY_INITIALIZER(id, kDirEntType));
  key.SetSuffix(suf);
  xslice keyenc = xslice(key.data(), key.size());
  if (tx != NULL) {
    opt->snapshot = tx->snap;
  }
  std::string ignored;
  xstatus st = dx_->Get(*opt, keyenc, &ignored);
  if (!st.ok()) {
    s = XSTATUS(st);
  }
  return s;
}

#undef KEY_INITIALIZER
#undef XSTATUS
#undef MXDBTEM
#undef MXDBTEMDECL

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
