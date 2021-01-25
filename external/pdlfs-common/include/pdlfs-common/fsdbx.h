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

#include "pdlfs-common/fstypes.h"

#include <string>
#include <vector>

namespace pdlfs {

struct DirInfo;
struct DirId {
  DirId() {}  // Intentionally not initialized for performance.
#if defined(DELTAFS_PROTO)
  DirId(uint64_t dno, uint64_t ino) : dno(dno), ino(ino) {}
#endif
#if defined(DELTAFS)
  DirId(uint64_t reg, uint64_t snap, uint64_t ino)
      : reg(reg), snap(snap), ino(ino) {}
#endif
  explicit DirId(uint64_t ino);  // Direct initialization via inodes.
  // Initialization via LookupStat or Stat.
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || \
    defined(INDEXFS)  // Tablefs does not use LookupStat.
  explicit DirId(const LookupStat& stat);
#endif
  explicit DirId(const Stat& stat);

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "other",
  //   == 0 iff "*this" == "other",
  //   >  0 iff "*this" >  "other"
  int compare(const DirId& other) const;
  std::string DebugString() const;

  // Deltafs requires extra fields.
#if defined(DELTAFS_PROTO)
  uint64_t dno;
#endif
#if defined(DELTAFS)
  uint64_t reg;
  uint64_t snap;
#endif

  uint64_t ino;
};

inline bool operator==(const DirId& x, const DirId& y) {
#if defined(DELTAFS_PROTO)
  if (x.dno != y.dno) {
    return false;
  }
#endif
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

class DB;

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
  TX* STARTTX(bool with_snapshot) {  // Open Tx
    TX* tx = new TX;
    if (with_snapshot) {
      tx->snap = dx_->GetSnapshot();
    } else {
      tx->snap = NULL;
    }
    return tx;
  }

  template <typename KX, typename TX, typename OPT, typename PERF>
  Status GET(const DirId& id, const Slice& suf, Stat* stat, std::string* name,
             OPT* opt, TX* tx, PERF* perf);
  template <typename KX, typename TX, typename OPT, typename PERF>
  Status PUT(const DirId& id, const Slice& suf, const Stat& stat,
             const Slice& name, OPT* opt, TX* tx, PERF* perf);
  template <typename KX, typename TX, typename OPT>
  Status DELETE(const DirId& id, const Slice& suf, OPT* opt, TX* tx);

  template <typename Iter>
  struct Dir {
    size_t n;  // Number dir entries scanned
    std::string key_prefix;
    Iter* iter;
  };
  template <typename Iter, typename KX, typename TX, typename OPT>
  Dir<Iter>* OPENDIR(const DirId& id, OPT* opt, TX* tx);
  template <typename Iter>
  Status READDIR(Dir<Iter>* dir, Stat* stat, std::string* name);
  template <typename Iter>
  void CLOSEDIR(Dir<Iter>* dir);

  typedef std::vector<std::string> NameList;
  typedef std::vector<Stat> StatList;
  template <typename Iter, typename KX, typename TX, typename OPT>
  size_t LIST(const DirId& id, StatList* stats, NameList* names, OPT* opt,
              TX* tx, size_t limit);
  template <typename KX, typename TX, typename OPT>
  Status EXISTS(const DirId& id, const Slice& suf, OPT* opt, TX* tx);

  template <typename TX, typename OPT>
  Status COMMIT(OPT* opt, TX* tx) {
    if (tx != NULL) {
      return dx_->Write(*opt, &tx->bat);
    } else {
      return Status::OK();
    }
  }

  template <typename TX>
  void RELEASE(TX* tx) {
    if (tx != NULL) {
      if (tx->snap != NULL) {
        dx_->ReleaseSnapshot(tx->snap);
      }
      delete tx;
    }
  }

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
#if defined(DELTAFS_PROTO)
#define KEY_INITIALIZER(id, tp) id.dno, id.ino, tp
#elif defined(DELTAFS)
#define KEY_INITIALIZER(id, tp) id.reg, id.snap, id.ino, tp
#else
#define KEY_INITIALIZER(id, tp) id.ino, tp
#endif

MXDBTEMDECL(DX, xslice, xstatus, fmt)
template <typename KX, typename TX, typename OPT, typename PERF>
Status MXDB<DX, xslice, xstatus, fmt>::PUT(  ////
    const DirId& id, const Slice& suf, const Stat& stat, const Slice& name,
    OPT* opt, TX* tx, PERF* perf) {
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

  // Collect performance stats
  if (perf != NULL) {
    perf->putkeybytes += keyenc.size();
    perf->putbytes += value.size();
    perf->puts++;
  }

  return s;
}

MXDBTEMDECL(DX, xslice, xstatus, fmt)
template <typename KX, typename TX, typename OPT, typename PERF>
Status MXDB<DX, xslice, xstatus, fmt>::GET(  ////
    const DirId& id, const Slice& suf, Stat* stat, std::string* name, OPT* opt,
    TX* tx, PERF* perf) {
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
    Slice filename;
    if (!stat->DecodeFrom(&input)) {
      s = Status::Corruption(Slice());
    } else if (name != NULL) {  // Filename requested
      if (fmt == kNameInKey) {
        *name = suf.ToString();
      } else if (!GetLengthPrefixedSlice(&input, &filename)) {
        s = Status::Corruption(Slice());
      } else {
        *name = filename.ToString();
      }
    }
  } else {
    s = XSTATUS(st);
  }

  // Collect performance stats
  if (perf != NULL) {
    perf->getkeybytes += keyenc.size();
    perf->getbytes += tmp.size();
    perf->gets++;
  }

  return s;
}

MXDBTEMDECL(DX, xslice, xstatus, fmt)
template <typename KX, typename TX, typename OPT>
Status MXDB<DX, xslice, xstatus, fmt>::DELETE(  ////
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
MXDB<DX, xslice, xstatus, fmt>::OPENDIR(  ////
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
Status MXDB<DX, xslice, xstatus, fmt>::READDIR(  ////
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

  return Status::OK();
}

MXDBTEMDECL(DX, xslice, xstatus, fmt)
template <typename Iter>
void MXDB<DX, xslice, xstatus, fmt>::CLOSEDIR(  ////
    Dir<Iter>* dir) {
  if (dir != NULL) {
    delete dir->iter;
    delete dir;
  }
}

MXDBTEMDECL(DX, xslice, xstatus, fmt)
template <typename Iter, typename KX, typename TX, typename OPT>
size_t MXDB<DX, xslice, xstatus, fmt>::LIST(  ////
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
Status MXDB<DX, xslice, xstatus, fmt>::EXISTS(  ////
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

}  // namespace pdlfs
