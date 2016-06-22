/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "mds_types.h"

namespace pdlfs {

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
  uint64_t ino;
  uint64_t size;
  uint32_t mode;
  uint32_t zeroth_server;
  uint32_t uid;
  uint32_t gid;
  uint64_t mtime;
  uint64_t ctime;
  if (!GetVarint64(&input, &ino) || !GetVarint64(&input, &size) ||
      !GetVarint32(&input, &mode) || !GetVarint32(&input, &zeroth_server) ||
      !GetVarint32(&input, &uid) || !GetVarint32(&input, &gid) ||
      !GetVarint64(&input, &mtime) || !GetVarint64(&input, &ctime)) {
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

Slice DirEntry::EncodeTo(char* scratch) const {
  char* p = scratch;

  p = EncodeVarint64(p, InodeNo());
  p = EncodeVarint32(p, ZerothServer());
  p = EncodeVarint32(p, DirMode());
  p = EncodeVarint32(p, UserId());
  p = EncodeVarint32(p, GroupId());
  p = EncodeVarint64(p, 0);

  return Slice(scratch, p - scratch);
}

bool DirEntry::DecodeFrom(const Slice& encoding) {
  Slice input = encoding;
  uint64_t ino;
  uint32_t zeroth_server;
  uint32_t mode;
  uint32_t uid;
  uint32_t gid;
  uint64_t lease_due;
  if (!GetVarint64(&input, &ino) || !GetVarint32(&input, &zeroth_server) ||
      !GetVarint32(&input, &mode) || !GetVarint32(&input, &uid) ||
      !GetVarint32(&input, &gid) || !GetVarint64(&input, &lease_due)) {
    return false;
  } else {
    SetInodeNo(ino);
    SetZerothServer(zeroth_server);
    SetDirMode(mode);
    SetUserId(uid);
    SetGroupId(gid);
    AssertAllSet();
    return true;
  }
}

void DirEntry::CopyFrom(const Stat& stat) {
  SetInodeNo(stat.InodeNo());
  SetDirMode(stat.FileMode());
  SetZerothServer(stat.ZerothServer());
  SetUserId(stat.UserId());
  SetGroupId(stat.GroupId());
}

}  // namespace pdlfs
