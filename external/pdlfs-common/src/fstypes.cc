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
  Slice r = DirIndex::Hash(name, rep_ + 8);
  assert(r.size() == 8);
  (void)r;
}

void Key::SetHash(const Slice& hash) {
  assert(hash.size() == 8);
  memcpy(rep_ + 8, hash.data(), 8);
}

Key::Key(uint64_t dir_id, KeyType type) {
  uint64_t pack = (dir_id << 8) | (type & 0xff);
  pack = htobe64(pack);
  memcpy(rep_, &pack, 8);
}

Key::Key(uint64_t dir_id, KeyType type, const Slice& name) {
  uint64_t pack = (dir_id << 8) | (type & 0xff);
  pack = htobe64(pack);
  memcpy(rep_, &pack, 8);
  SetName(name);
}

uint64_t Key::dir_id() const {
  uint64_t pack;
  memcpy(&pack, rep_, 8);
  uint64_t r = be64toh(pack) >> 8;
  return r;
}

KeyType Key::type() const {
  uint64_t pack;
  memcpy(&pack, rep_, 8);
  KeyType type = static_cast<KeyType>(be64toh(pack) & 0xff);
  return type;
}

Slice Key::hash() const {
  Slice r = Slice(rep_ + 8, 8);
  return r;
}

Slice Key::prefix() const {
  Slice r = Slice(rep_, 8);
  return r;
}

Slice Stat::EncodeTo(char* scratch) const {
  char* p = scratch;

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

Slice LookupEntry::EncodeTo(char* scratch) const {
  char* p = scratch;

  p = EncodeVarint64(p, InodeNo());
  p = EncodeVarint32(p, ZerothServer());
  p = EncodeVarint32(p, DirMode());
  p = EncodeVarint32(p, UserId());
  p = EncodeVarint32(p, GroupId());
  p = EncodeVarint64(p, LeaseDue());

  return Slice(scratch, p - scratch);
}

bool LookupEntry::DecodeFrom(const Slice& encoding) {
  Slice input = encoding;
  return DecodeFrom(&input);
}

bool LookupEntry::DecodeFrom(Slice* input) {
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

void LookupEntry::CopyFrom(const Stat& stat) {
  SetInodeNo(stat.InodeNo());
  SetDirMode(stat.FileMode());
  SetZerothServer(stat.ZerothServer());
  SetUserId(stat.UserId());
  SetGroupId(stat.GroupId());
}

}  // namespace pdlfs
