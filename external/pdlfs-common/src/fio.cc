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
#include "pdlfs-common/fio.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/strutil.h"

#if defined(PDLFS_PLATFORM_POSIX)
#include "posix/posix_fio.h"
#endif

namespace pdlfs {

Fio::~Fio() {}

namespace {
std::string FetchRoot(const char* input) {
  std::string root = "/tmp/deltafs_data";
  std::vector<std::string> confs;
  SplitString(&confs, input);
  for (size_t i = 0; i < confs.size(); i++) {
    Slice input = confs[i];
    if (input.size() != 0) {
      if (input.starts_with("root=")) {
        input.remove_prefix(5);
        root = input.ToString();
      }
    }
  }
#if VERBOSE >= 2
  // Verbose(__LOG_ARGS__, 2, "fio.posix.root -> %s", root.c_str());
#endif
  return root;
}
}  // namespace

Fio* Fio::Open(const char* name, const char* conf) {
  if (name == NULL) name = "";
  if (conf == NULL) conf = "";
  Slice fio_name(name), fio_conf(conf);
#if VERBOSE >= 1
  // Verbose(__LOG_ARGS__, 1, "fio.name -> %s", fio_name.c_str());
  // Verbose(__LOG_ARGS__, 1, "fio.conf -> %s", fio_conf.c_str());
#endif
  if (fio_name == "posix") {
#if defined(PDLFS_PLATFORM_POSIX)
    std::string root = FetchRoot(fio_conf.c_str());
    return new PosixFio(root.c_str());
#else
    return NULL;
#endif
  } else {
    return NULL;
  }
}

std::string Fentry::UntypedKeyPrefix() const {
  KeyType dummy = static_cast<KeyType>(0);
  Key key(stat, dummy);
  return key.prefix().ToString();
}

namespace {
bool GetDirId(Slice* input, DirId* id) {
#if defined(DELTAFS_PROTO)
  if (!GetVarint64(input, &id->dno)) {
    return false;
  }
#endif
#if defined(DELTAFS)
  if (!GetVarint64(input, &id->reg) || !GetVarint64(input, &id->snap)) {
    return false;
  }
#endif
  if (!GetVarint64(input, &id->ino)) {
    return false;
  } else {
    return true;
  }
}
}  // namespace

bool Fentry::DecodeFrom(Slice* input) {
  DirId parent_id;
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
  uint32_t parent_zserver;
#endif
  Slice my_nhash;
  DirId my_id;
  uint64_t size;
  uint32_t mode;
  uint32_t uid;
  uint32_t gid;
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
  uint32_t my_zserver;
#endif
  uint64_t ctime;
  uint64_t mtime;

  if (!GetDirId(input, &parent_id) ||
      !GetLengthPrefixedSlice(input, &my_nhash)) {
    return false;
  }
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
  if (!GetVarint32(input, &parent_zserver)) {
    return false;
  }
#endif
  if (!GetDirId(input, &my_id) || !GetVarint64(input, &size) ||
      !GetVarint32(input, &mode) || !GetVarint32(input, &uid) ||
      !GetVarint32(input, &gid)) {
    return false;
  }
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
  if (!GetVarint32(input, &my_zserver)) {
    return false;
  }
#endif
  if (!GetVarint64(input, &ctime) || !GetVarint64(input, &mtime)) {
    return false;
  } else {
    pid = parent_id;
    nhash = my_nhash.ToString();
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
    zserver = parent_zserver;
#endif
#if defined(DELTAFS_PROTO)
    stat.SetDnodeNo(my_id.dno);
#endif
#if defined(DELTAFS)
    stat.SetRegId(my_id.reg);
    stat.SetSnapId(my_id.snap);
#endif
    stat.SetInodeNo(my_id.ino);
    stat.SetFileSize(size);
    stat.SetFileMode(mode);
    stat.SetUserId(uid);
    stat.SetGroupId(gid);
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
    stat.SetZerothServer(my_zserver);
#endif
    stat.SetChangeTime(ctime);
    stat.SetModifyTime(mtime);

    return true;
  }
}

namespace {
char* EncodeDirId(char* const scratch, const DirId& id) {
  char* p = scratch;

#if defined(DELTAFS_PROTO)
  p = EncodeVarint64(p, id.dno);
#endif
#if defined(DELTAFS)
  p = EncodeVarint64(p, id.reg);
  p = EncodeVarint64(p, id.snap);
#endif

  p = EncodeVarint64(p, id.ino);
  return p;
}
}  // namespace

// The encoding has the following format:
// --------------------------------------------
//   reg_id of parent dir   varint64 (deltafs only)
//   snap_id of parent dir  varint64 (deltafs only)
//   ino_no of parent dir   varint64
//   nhash_length           varint32
//   nhash                  char[nhash_length]
//   zserver of parent dir  varint32
//   reg_id                 varint64 (deltafs only)
//   snap_id                varint64 (deltafs only)
//   ino_no                 varint64
//   size                   varint64
//   mode                   varint32
//   user_id                varint32
//   group_id               varint32
//   zserver                varint32
//   change_time            varint64
//   modify_time            varint64
Slice Fentry::EncodeTo(char* const scratch) const {
  char* p = scratch;

  p = EncodeDirId(p, pid);
  p = EncodeLengthPrefixedSlice(p, nhash);
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
  p = EncodeVarint32(p, zserver);
#endif
  p = EncodeDirId(p, DirId(stat));
  p = EncodeVarint64(p, stat.FileSize());
  p = EncodeVarint32(p, stat.FileMode());
  p = EncodeVarint32(p, stat.UserId());
  p = EncodeVarint32(p, stat.GroupId());
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
  p = EncodeVarint32(p, stat.ZerothServer());
#endif
  p = EncodeVarint64(p, stat.ChangeTime());
  p = EncodeVarint64(p, stat.ModifyTime());

  return Slice(scratch, p - scratch);
}

namespace {
inline std::string FileModeToString(mode_t mode) {
  char tmp[10];
  snprintf(tmp, sizeof(tmp), "%o", mode);
  return tmp;
}
}  // namespace

std::string Fentry::DebugString() const {
  std::string result("fentry={");
  result += "pid=";
  result += pid.DebugString();
  result += ", sid=";
  result += DirId(stat).DebugString();
  result += ", mode=";
  result += FileModeToString((stat.FileMode()));
  result += "}";
  return result;
}

}  // namespace pdlfs
