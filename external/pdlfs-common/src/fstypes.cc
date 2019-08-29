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

#include "pdlfs-common/fstypes.h"
#include "pdlfs-common/gigaplus.h"
#include "pdlfs-common/port.h"

#include <assert.h>

// All keys (regardless of the filesystem type) consist of a prefix component
// and a suffix component. Tablefs keys have variable length. Their prefixes
// each encode a parent directory inode no. and the type of the key. They have a
// fixed length. Their suffixes are typically complete filenames and have
// variable length. Indexfs keys are similar to those of tablefs except that
// their suffixes each store the hash of a filename rather than the filename
// itself. For this reason indexfs keys have a fixed length. Deltafs keys ...
namespace pdlfs {
// Requires one filesystem definition
#if !defined(DELTAFS) && !defined(INDEXFS) && !defined(TABLEFS)
#define TABLEFS
#endif

#define TABLEFS_KEY_RESERV 128  // Number of bytes reserved for tablefs keys
// Deltafs and indexfs store hashes of filenames in key suffixes.
// Moreover, these suffixes are fixed sized, either 64-bit (8B)
// or 128-bit (16B) long.
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

#else  // Then this is tablefs. Tablefs stores full filenames in key suffixes.

void Key::SetName(const Slice& name) {
  rep_.resize(8);
  rep_.append(name.data(), name.size());
}

void Key::SetOffset(uint64_t off) {
  rep_.resize(8);
  off = htobe64(off);
  rep_.append(static_cast<void*>(&off), 8);
}

void Key::SetSuffix(const Slice& suff) {  // Reuse SetName.
  SetName(suff);
}

#endif

void Key::SetIntegerUniquifier(uint64_t uni) {  // Reuse SetOffset.
  SetOffset(uni);
}

void Key::SetType(KeyType type) {
#if defined(DELTAFS) || defined(INDEXFS)
  rep_[size_ - 8 - 1] = type;
#else
  rep_[7] = type;
#endif
}

namespace {
#if defined(DELTAFS)
Slice PackPrefix(char* dst, uint64_t R, uint64_t S, uint64_t D, KeyType T) {
  char* p = dst;
  p = EncodeVarint64(p, R);
  p = EncodeVarint64(p, S);
  p = EncodeVarint64(p, D);
  p[0] = T;
  p++;
  return Slice(dst, p - dst);
}
#else
Slice PackPrefix(char* dst, uint64_t D, KeyType T) {
  uint64_t composite = (D << 8) | (T & 0xff);
  composite = htobe64(composite);
  memcpy(dst, &composite, 8);
  return Slice(dst, 8);
}
#endif
}  // namespace

#if defined(TABLEFS) || defined(INDEXFS)
Key::Key(uint64_t dir, KeyType type) {
#if defined(INDEXFS)
  Slice prefix = PackPrefix(rep_, dir, type);
  size_ = prefix.size() + 8;
#else
  rep_.reserve(TABLEFS_KEY_RESERV);
  rep_.resize(8);
  PackPrefix(&rep_[0], dir, type);
#endif
}
#else  // Then this is deltafs.
Key::Key(uint64_t reg, uint64_t snap, uint64_t ino, KeyType type) {
  Slice prefix = PackPrefix(rep_, reg, snap, ino, type);
  size_ = prefix.size() + 8;
}

Key::Key(uint64_t snap, uint64_t ino, KeyType type) {
  Slice prefix = PackPrefix(rep_, 0, snap, ino, type);
  size_ = prefix.size() + 8;
}

Key::Key(uint64_t ino, KeyType type) {
  Slice prefix = PackPrefix(rep_, 0, 0, ino, type);
  size_ = prefix.size() + 8;
}
#endif

#if defined(DELTAFS)
#define PREFIX_INITIALIZER(x, t) x.RegId(), x.SnapId(), x.InodeNo(), t
#else
#define PREFIX_INITIALIZER(x, t) x.InodeNo(), t
#endif

#if defined(DELTAFS) || defined(INDEXFS)
Key::Key(const LookupStat& stat, KeyType type) {
  Slice prefix = PackPrefix(rep_, PREFIX_INITIALIZER(stat, type));
  size_ = prefix.size() + 8;
}
#endif

Key::Key(const Stat& stat, KeyType type) {
#if defined(DELTAFS) || defined(INDEXFS)
  Slice prefix = PackPrefix(rep_, PREFIX_INITIALIZER(stat, type));
  size_ = prefix.size() + 8;
#else
  rep_.reserve(TABLEFS_KEY_RESERV);
  rep_.resize(8);
  PackPrefix(&rep_[0], PREFIX_INITIALIZER(stat, type));
#endif
}

Key::Key(const Slice& prefix) {
#if defined(DELTAFS) || defined(INDEXFS)
  memcpy(&rep_[0], prefix.data(), prefix.size());
  size_ = prefix.size() + 8;
#else
  rep_.reserve(TABLEFS_KEY_RESERV);
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

uint64_t Key::inode() const {
  uint64_t result;
#if defined(TABLEFS) || defined(INDEXFS)
  uint64_t composite;
  memcpy(&composite, &rep_[0], 8);
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
#if defined(TABLEFS) || defined(INDEXFS)
  uint64_t composite;
  memcpy(&composite, &rep_[0], 8);
  result = static_cast<KeyType>(be64toh(composite) & 0xff);
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
Slice Key::hash() const { return suffix(); }
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
#else  // sizeof(prefix) is 8
  Slice r = Slice(&rep_[0], 8);
#endif
  return r;
}

// Return the suffix of a key in its entirety.
Slice Key::suffix() const {
#if defined(DELTAFS) || defined(INDEXFS)  // sizeof(suffix) is 8
  Slice r = Slice(rep_ + size_ - 8, 8);
#else  // suffix = total - prefix
  Slice r = Slice(&rep_[0] + 8, rep_.size() - 8);
#endif
  return r;
}

Slice Stat::EncodeTo(char* scratch) const {
  char* p = scratch;
#if defined(DELTAFS)
  p = EncodeVarint64(p, RegId());
  p = EncodeVarint64(p, SnapId());
#endif

  p = EncodeVarint64(p, InodeNo());
  p = EncodeVarint64(p, FileSize());
  p = EncodeVarint32(p, FileMode());
#if defined(DELTAFS) || defined(INDEXFS)
  p = EncodeVarint32(p, ZerothServer());
#endif

  p = EncodeVarint32(p, UserId());
  p = EncodeVarint32(p, GroupId());
  p = EncodeVarint64(p, ModifyTime());
  p = EncodeVarint64(p, ChangeTime());

  return Slice(scratch, p - scratch);
}

bool Stat::DecodeFrom(const Slice& encoding) {
  Slice input = encoding;
  return DecodeFrom(&input);
}

bool Stat::DecodeFrom(Slice* input) {
#if defined(DELTAFS)
  uint64_t reg;
  uint64_t snap;
  if (!GetVarint64(input, &reg) || !GetVarint64(input, &snap)) {
    return false;
  } else {
    SetRegId(reg);
    SetSnapId(snap);
  }
#endif

  uint64_t ino;
  uint64_t size;
  uint32_t mode;
  if (!GetVarint64(input, &ino) || !GetVarint64(input, &size) ||
      !GetVarint32(input, &mode)) {
    return false;
  } else {
    SetInodeNo(ino);
    SetFileSize(size);
    SetFileMode(mode);
  }

#if defined(DELTAFS) || defined(INDEXFS)
  uint32_t zeroth_server;
  if (!GetVarint32(input, &zeroth_server)) {
    return false;
  } else {
    SetZerothServer(zeroth_server);
  }
#endif

  uint32_t uid;
  uint32_t gid;
  uint64_t mtime;
  uint64_t ctime;
  if (!GetVarint32(input, &uid) || !GetVarint32(input, &gid) ||
      !GetVarint64(input, &mtime) || !GetVarint64(input, &ctime)) {
    return false;
  } else {
    SetUserId(uid);
    SetGroupId(gid);
    SetModifyTime(mtime);
    SetChangeTime(ctime);
  }

  AssertAllSet();
  return true;
}

#if defined(DELTAFS) || defined(INDEXFS)

Slice LookupStat::EncodeTo(char* scratch) const {
  char* p = scratch;
#if defined(DELTAFS)
  p = EncodeVarint64(p, RegId());
  p = EncodeVarint64(p, SnapId());
#endif

  p = EncodeVarint64(p, InodeNo());
  p = EncodeVarint32(p, ZerothServer());
  p = EncodeVarint32(p, DirMode());
  p = EncodeVarint32(p, UserId());
  p = EncodeVarint32(p, GroupId());
  p = EncodeVarint64(p, LeaseDue());

  return Slice(scratch, p - scratch);
}

bool LookupStat::DecodeFrom(const Slice& encoding) {
  Slice input = encoding;
  return DecodeFrom(&input);
}

bool LookupStat::DecodeFrom(Slice* input) {
#if defined(DELTAFS)
  uint64_t reg;
  uint64_t snap;
  if (!GetVarint64(input, &reg) || !GetVarint64(input, &snap)) {
    return false;
  } else {
    SetRegId(reg);
    SetSnapId(snap);
  }
#endif

  uint64_t ino;
  uint32_t zeroth_server;
  uint32_t mode;
  uint32_t uid;
  uint32_t gid;
  uint64_t lease_due;
  if (!GetVarint64(input, &ino) || !GetVarint32(input, &zeroth_server) ||
      !GetVarint32(input, &mode) || !GetVarint32(input, &uid) ||
      !GetVarint32(input, &gid) || !GetVarint64(input, &lease_due)) {
    return false;
  } else {
    SetInodeNo(ino);
    SetZerothServer(zeroth_server);
    SetDirMode(mode);
    SetUserId(uid);
    SetGroupId(gid);
    SetLeaseDue(lease_due);
    AssertAllSet();
    return true;
  }
}

void LookupStat::CopyFrom(const Stat& stat) {
#if defined(DELTAFS)
  SetRegId(stat.RegId());
  SetSnapId(stat.SnapId());
#endif

  SetInodeNo(stat.InodeNo());
  SetDirMode(stat.FileMode());
  SetZerothServer(stat.ZerothServer());
  SetUserId(stat.UserId());
  SetGroupId(stat.GroupId());
}

#endif

}  // namespace pdlfs
