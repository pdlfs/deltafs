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

#include "pdlfs-common/fsdbbase.h"

#include "pdlfs-common/port.h"
#if defined(DELTAFS) || defined(INDEXFS)
#include "pdlfs-common/gigaplus.h"
#endif

// All fs keys consist of a prefix component and a suffix component. In both
// tablefs and indexfs, the prefix of a key is a 64-bit integer with the
// leftmost 56 bits being the parent directory inode no and the rightmost 8 bits
// being the type of the key. In deltafs, the prefix of a key consists of a
// 64-bit delta no, followed by a 56-bit parent directory inode no, followed
// by an 8-bit key type. In both tablefs and deltafs, the suffix of a key is an
// intact base filename of a file. In indexfs, the suffix of a key is a
// fixed-length hash of a base filename.
namespace pdlfs {
// Ensure a filesystem definition if none is given
#if !defined(DELTAFS_PROTO) && !defined(DELTAFS) && !defined(INDEXFS) && \
    !defined(TABLEFS)
#define TABLEFS
#endif
// Number of bytes reserved for key buffers and key prefix length
#if defined(DELTAFS_PROTO)
#define FS_KEY_PREFIX_LENGTH 16
#define FS_KEY_RESERV 128
#endif
#if defined(INDEXFS) || defined(TABLEFS)
#define FS_KEY_PREFIX_LENGTH 8
#define FS_KEY_RESERV 128
#endif

// Indexfs stores hashes of filenames in key suffixes.
// These suffixes are fixed sized.
#if defined(DELTAFS) || defined(INDEXFS)
void Key::SetName(const Slice& name) {
#ifndef NDEBUG
  std::string hash;
  DirIndex::PutHash(&hash, name);
  assert(hash.size() == 8);
  memcpy(rep_ + size_ - 8, hash.data(), hash.size());
#else
  DirIndex::Hash(name, rep_ + size_ - 8);
#endif
}

void Key::SetHash(const Slice& hash) {
  assert(hash.size() == 8);
  memcpy(rep_ + size_ - 8, hash.data(), hash.size());
}

void Key::SetOffset(uint64_t off) {
  off = htobe64(off);
  memcpy(rep_ + size_ - 8, &off, 8);
}

void Key::SetSuffix(const Slice& suff) {  // Reuse SetHash.
  SetHash(suff);
}

#else  // Both deltafs and tablefs use intact base filenames as key suffixes.
void Key::SetName(const Slice& name) {
  rep_.resize(FS_KEY_PREFIX_LENGTH);
  rep_.append(name.data(), name.size());
}

void Key::SetOffset(uint64_t off) {
  rep_.resize(FS_KEY_PREFIX_LENGTH);
  off = htobe64(off);
  rep_.append(reinterpret_cast<char*>(&off), 8);
}

void Key::SetSuffix(const Slice& suff) {  // Reuse SetName.
  SetName(suff);
}
#endif

void Key::SetType(KeyType type) {
#if defined(DELTAFS)
  rep_[size_ - 8 - 1] = type;
#else
  const off_t i = FS_KEY_PREFIX_LENGTH - 1;
  rep_[i] = type;
#endif
}

namespace {
#if defined(DELTAFS)
size_t PackPrefix(char* dst, uint64_t R, uint64_t S, uint64_t D, KeyType T) {
  char* p = dst;
  p = EncodeVarint64(p, R);
  p = EncodeVarint64(p, S);
  p = EncodeVarint64(p, D);
  p[0] = T;
  p++;
  return p - dst;
}
#elif defined(DELTAFS_PROTO)
size_t PackPrefix(char* const dst, uint64_t dno, uint64_t ino, KeyType t) {
#if FS_KEY_PREFIX_LENGTH == 16
  uint64_t tmp1 = htobe64(dno);
  memcpy(dst, &tmp1, 8);
  uint64_t tmp2 = (ino << 8) | (t & 0xff);
  tmp2 = htobe64(tmp2);
  memcpy(dst + 8, &tmp2, 8);
  return 16;
#else
#error Specified unsupported key prefix
#endif
}
#else  // This is tablefs or indexfs...
size_t PackPrefix(char* dst, uint64_t D, KeyType T) {
#if FS_KEY_PREFIX_LENGTH == 8
  uint64_t composite = (D << 8) | (T & 0xff);
  composite = htobe64(composite);
  memcpy(dst, &composite, 8);
  return 8;
#else
#error Specified unsupported key prefix
#endif
}
#endif
}  // namespace

// Constructors...
#if defined(DELTAFS_PROTO)
Key::Key(uint64_t dno, uint64_t ino, KeyType type) {
  rep_.reserve(FS_KEY_RESERV);
  const size_t s = FS_KEY_PREFIX_LENGTH;
  rep_.resize(s);
  PackPrefix(&rep_[0], dno, ino, type);
}

Key::Key(uint64_t ino, KeyType type) {
  rep_.reserve(FS_KEY_RESERV);
  const size_t s = FS_KEY_PREFIX_LENGTH;
  rep_.resize(s);
  PackPrefix(&rep_[0], 0, ino, type);
}

#elif defined(INDEXFS)
Key::Key(uint64_t ino, KeyType type) {
  const size_t s = FS_KEY_PREFIX_LENGTH + 8;
  PackPrefix(rep_, ino, type);
  size_ = s;
}

#elif defined(TABLEFS)
Key::Key(uint64_t ino, KeyType type) {
  rep_.reserve(FS_KEY_RESERV);
  const size_t s = FS_KEY_PREFIX_LENGTH;
  rep_.resize(s);
  PackPrefix(&rep_[0], ino, type);
}

#else  // Then this is deltafs.
Key::Key(uint64_t reg, uint64_t snap, uint64_t ino, KeyType type) {
  size_t p = PackPrefix(rep_, reg, snap, ino, type);
  size_ = p + 8;
}

Key::Key(uint64_t snap, uint64_t ino, KeyType type) {
  size_t p = PackPrefix(rep_, 0, snap, ino, type);
  size_ = p + 8;
}

Key::Key(uint64_t ino, KeyType type) {
  size_t p = PackPrefix(rep_, 0, 0, ino, type);
  size_ = p + 8;
}
#endif

#if defined(DELTAFS_PROTO)
#define PREFIX_INITIALIZER(x, t) x.DnodeNo(), x.InodeNo(), t
#elif defined(DELTAFS)
#define PREFIX_INITIALIZER(x, t) x.RegId(), x.SnapId(), x.InodeNo(), t
#else
#define PREFIX_INITIALIZER(x, t) x.InodeNo(), t
#endif

#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
Key::Key(const LookupStat& stat, KeyType type) {
#if defined(DELTAFS) || defined(INDEXFS)
  size_t p = PackPrefix(rep_, PREFIX_INITIALIZER(stat, type));
  size_ = p + 8;
#else
  rep_.reserve(FS_KEY_RESERV);
  const size_t s = FS_KEY_PREFIX_LENGTH;
  rep_.resize(s);
  PackPrefix(&rep_[0], PREFIX_INITIALIZER(stat, type));
#endif
}
#endif

Key::Key(const Stat& stat, KeyType type) {
#if defined(DELTAFS) || defined(INDEXFS)
  size_t p = PackPrefix(rep_, PREFIX_INITIALIZER(stat, type));
  size_ = p + 8;
#else
  rep_.reserve(FS_KEY_RESERV);
  const size_t s = FS_KEY_PREFIX_LENGTH;
  rep_.resize(s);
  PackPrefix(&rep_[0], PREFIX_INITIALIZER(stat, type));
#endif
}

Key::Key(const Slice& prefix) {
#if defined(DELTAFS) || defined(INDEXFS)
  memcpy(&rep_[0], prefix.data(), prefix.size());
  size_ = prefix.size() + 8;
#else
  rep_.reserve(FS_KEY_RESERV);
  rep_.resize(0);
  assert(prefix.size() == FS_KEY_PREFIX_LENGTH);
  rep_.append(prefix.data(), prefix.size());
#endif
}

#if defined(DELTAFS)
// Return the registry id of the parent directory.
uint64_t Key::reg_id() const {
  uint64_t result;
  Slice encoding = Encode();
  GetVarint64(&encoding, &result);
  return result;
}

// Return the snapshot id of the parent directory.
uint64_t Key::snap_id() const {
  uint64_t result;
  Slice encoding = Encode();
  GetVarint64(&encoding, &result);  // ignored
  GetVarint64(&encoding, &result);
  return result;
}

#endif

#if defined(DELTAFS_PROTO)
uint64_t Key::dnode() const {
  uint64_t composite;
  memcpy(&composite, &rep_[0], 8);
  return be64toh(composite);
}
#endif

uint64_t Key::inode() const {
  uint64_t result;
#if defined(DELTAFS_PROTO) || defined(TABLEFS) || defined(INDEXFS)
  uint64_t composite;
  const off_t i = FS_KEY_PREFIX_LENGTH - 8;
  memcpy(&composite, &rep_[i], 8);
  result = be64toh(composite) >> 8;
#else
  Slice encoding = Encode();
  GetVarint64(&encoding, &result);  // ignored
  GetVarint64(&encoding, &result);  // ignored
  GetVarint64(&encoding, &result);
#endif
  return result;
}

KeyType Key::type() const {
  KeyType result;
#if defined(DELTAFS_PROTO) || defined(TABLEFS) || defined(INDEXFS)
  const off_t i = FS_KEY_PREFIX_LENGTH - 1;
  result = static_cast<KeyType>(rep_[i]);
#else
  uint64_t ignored;
  Slice encoding = Encode();
  GetVarint64(&encoding, &ignored);
  GetVarint64(&encoding, &ignored);
  GetVarint64(&encoding, &ignored);
  unsigned char tmp = encoding[0];
  result = static_cast<KeyType>(tmp);
#endif
  return result;
}

#if defined(DELTAFS) || defined(INDEXFS)
Slice Key::hash() const {  ///
  return suffix();
}
#endif

// Return suffix as an integer number.
uint64_t Key::offset() const {
  uint64_t off;
  Slice r = suffix();
  assert(r.size() >= 8);  // Additional suffix bytes are ignored
  memcpy(&off, &r[0], 8);
  return be64toh(off);
}

// Return the prefix of a key in its entirety.
Slice Key::prefix() const {
#if defined(DELTAFS) || defined(INDEXFS)  // prefix = total - suffix
  Slice r = Slice(rep_, size_ - 8);
#else  // fixed sizeof(prefix)
  const size_t s = FS_KEY_PREFIX_LENGTH;
  Slice r = Slice(&rep_[0], s);
#endif
  return r;
}

// Return the suffix of a key in its entirety.
Slice Key::suffix() const {
#if defined(DELTAFS) || defined(INDEXFS)  // sizeof(suffix) is 8
  Slice r = Slice(rep_ + size_ - 8, 8);
#else  // suffix = total - prefix
  const size_t s = FS_KEY_PREFIX_LENGTH;
  Slice r = Slice(&rep_[0] + s, rep_.size() - s);
#endif
  return r;
}

std::string DirId::DebugString() const {
#define LLU(x) static_cast<unsigned long long>(x)
  char tmp[30];
#if defined(DELTAFS_PROTO)
  snprintf(tmp, sizeof(tmp), "dirid[%llu:%llu]", LLU(dno), LLU(ino));
#elif defined(DELTAFS)
  snprintf(tmp, sizeof(tmp), "dirid[%llu:%llu:%llu]", LLU(reg), LLU(snap),
           LLU(ino));
#else
  snprintf(tmp, sizeof(tmp), "dirid[%llu]", LLU(ino));
#endif
  return tmp;
}

#if defined(DELTAFS_PROTO)
DirId::DirId(uint64_t ino) : dno(0), ino(ino) {}
#elif defined(DELTAFS)
DirId::DirId(uint64_t ino) : reg(0), snap(0), ino(ino) {}
#else
DirId::DirId(uint64_t ino) : ino(ino) {}
#endif

#if defined(DELTAFS_PROTO)
#define DIR_INITIALIZER(x) dno(x.DnodeNo()), ino(x.InodeNo())
#elif defined(DELTAFS)
#define DIR_INITIALIZER(x) reg(x.RegId()), snap(x.SnapId()), ino(x.InodeNo())
#else
#define DIR_INITIALIZER(x) ino(x.InodeNo())
#endif

#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
DirId::DirId(const LookupStat& stat)  // Initialization via LookupStat
    : DIR_INITIALIZER(stat) {}
#endif

DirId::DirId(const Stat& stat)  // Initialization via Stat
    : DIR_INITIALIZER(stat) {}

namespace {
int compare64(uint64_t a, uint64_t b) {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}
}  // namespace
int DirId::compare(const DirId& other) const {
  int r;
#if defined(DELTAFS_PROTO)
  r = compare64(dno, other.dno);
  if (r != 0) return r;
#endif
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

}  // namespace pdlfs
