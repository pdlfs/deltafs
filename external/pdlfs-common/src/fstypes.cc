/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>

#include "pdlfs-common/fstypes.h"
#include "pdlfs-common/gigaplus.h"
#include "pdlfs-common/port.h"

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

uint64_t Key::dir_id() const {
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
  p = EncodeVarint32(p, ZerothServer());
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
  uint32_t zeroth_server;
  uint32_t uid;
  uint32_t gid;
  uint64_t mtime;
  uint64_t ctime;
  if (!GetVarint64(input, &ino) || !GetVarint64(input, &size) ||
      !GetVarint32(input, &mode) || !GetVarint32(input, &zeroth_server) ||
      !GetVarint32(input, &uid) || !GetVarint32(input, &gid) ||
      !GetVarint64(input, &mtime) || !GetVarint64(input, &ctime)) {
    return false;
  } else {
    SetInodeNo(ino);
    SetFileSize(size);
    SetFileMode(mode);
    SetZerothServer(zeroth_server);
    SetUserId(uid);
    SetGroupId(gid);
    SetModifyTime(mtime);
    SetChangeTime(ctime);
    AssertAllSet();
    return true;
  }
}

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

}  // namespace pdlfs
