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

namespace pdlfs {

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

static Slice PackPrefix(char* dst, uint64_t R, uint64_t S, uint64_t D,
                        KeyType T) {
#if !defined(DELTAFS)
  uint64_t composite = (D << 8) | (T & 0xff);
  composite = htobe64(composite);
  memcpy(dst, &composite, 8);
  char* p = dst + 8;
#else
  char* p = dst;
  p = EncodeVarint64(p, R);
  p = EncodeVarint64(p, S);
  p = EncodeVarint64(p, D);
  p[0] = T;
  p++;
#endif
  return Slice(dst, p - dst);
}

void Key::SetType(KeyType type) { rep_[size_ - 8 - 1] = type; }

Key::Key(uint64_t dir, KeyType type) {
  Slice prefix = PackPrefix(rep_, 0, 0, dir, type);
  size_ = prefix.size() + 8;
}

Key::Key(uint64_t snap, uint64_t dir, KeyType type) {
  Slice prefix = PackPrefix(rep_, 0, snap, dir, type);
  size_ = prefix.size() + 8;
}

Key::Key(uint64_t reg, uint64_t snap, uint64_t dir, KeyType type) {
  Slice prefix = PackPrefix(rep_, reg, snap, dir, type);
  size_ = prefix.size() + 8;
}

Key::Key(const Stat& st, KeyType type) {
  Slice prefix = PackPrefix(rep_, st.RegId(), st.SnapId(), st.InodeNo(), type);
  size_ = prefix.size() + 8;
}

Key::Key(const Slice& prefix) {
  memcpy(rep_, prefix.data(), prefix.size());
  size_ = prefix.size() + 8;
}

uint64_t Key::reg_id() const {
  uint64_t result;
#if !defined(DELTAFS)
  result = 0;
#else
  Slice encoding = Encode();
  GetVarint64(&encoding, &result);
#endif
  return result;
}

uint64_t Key::snap_id() const {
  uint64_t result;
#if !defined(DELTAFS)
  result = 0;
#else
  Slice encoding = Encode();
  GetVarint64(&encoding, &result);  // ignored
  GetVarint64(&encoding, &result);
#endif
  return result;
}

uint64_t Key::inode() const {
  uint64_t result;
#if !defined(DELTAFS)
  uint64_t composite;
  memcpy(&composite, rep_, 8);
  result = be64toh(composite) >> 8;
#else
  Slice encoding = Encode();
  GetVarint64(&encoding, &result);  // ignored
  GetVarint64(&encoding, &result);  // ignored
  GetVarint64(&encoding, &result);
#endif
  return result;
}

uint64_t Key::offset() const {
  uint64_t off;
  memcpy(&off, rep_ + size_ - 8, 8);
  return be64toh(off);
}

KeyType Key::type() const {
  KeyType result;
#if !defined(DELTAFS)
  uint64_t composite;
  memcpy(&composite, rep_, 8);
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

Slice Key::hash() const {
  Slice r = Slice(rep_ + size_ - 8, 8);
  return r;
}

Slice Key::prefix() const {
  Slice r = Slice(rep_, size_ - 8);
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
