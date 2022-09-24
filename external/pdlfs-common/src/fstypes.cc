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
#include "pdlfs-common/coding.h"

namespace pdlfs {
// Ensure a filesystem definition if none is given
#if !defined(DELTAFS_PROTO) && !defined(DELTAFS) && !defined(INDEXFS) && \
    !defined(TABLEFS)
#define TABLEFS
#endif

Slice Stat::EncodeTo(char* scratch) const {
  char* p = scratch;
#if defined(DELTAFS_PROTO)
  p = EncodeVarint64(p, DnodeNo());
#endif
#if defined(DELTAFS)
  p = EncodeVarint64(p, RegId());
  p = EncodeVarint64(p, SnapId());
#endif

  p = EncodeVarint64(p, InodeNo());
  p = EncodeVarint64(p, FileSize());
  p = EncodeVarint32(p, FileMode());
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
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
#if defined(DELTAFS_PROTO)
  uint64_t dno;
  if (!GetVarint64(input, &dno)) {
    return false;
  } else {
    SetDnodeNo(dno);
  }
#endif

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

#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
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

#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
Slice LookupStat::EncodeTo(char* scratch) const {
  char* p = scratch;
#if defined(DELTAFS_PROTO)
  p = EncodeVarint64(p, DnodeNo());
#endif
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
#if defined(DELTAFS_PROTO)
  uint64_t dno;
  if (!GetVarint64(input, &dno)) {
    return false;
  } else {
    SetDnodeNo(dno);
  }
#endif

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
#if defined(DELTAFS_PROTO)
  SetDnodeNo(stat.DnodeNo());
#endif
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
