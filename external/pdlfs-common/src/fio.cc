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
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/strutil.h"

#if defined(PDLFS_RADOS)
#include "pdlfs-common/rados/rados_ld.h"
#endif

#if defined(PDLFS_PLATFORM_POSIX)
#include "posix/posix_fio.h"
#endif

namespace pdlfs {

Fio::~Fio() {}

static std::string FetchRoot(const char* input) {
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

Fio* Fio::Open(const char* name, const char* conf) {
  if (name == NULL) name = "";
  if (conf == NULL) conf = "";
  Slice fio_name(name), fio_conf(conf);
#if VERBOSE >= 1
  // Verbose(__LOG_ARGS__, 1, "fio.name -> %s", fio_name.c_str());
  // Verbose(__LOG_ARGS__, 1, "fio.conf -> %s", fio_conf.c_str());
#endif
#if defined(PDLFS_RADOS)
  if (fio_name == "rados") {
    return reinterpret_cast<Fio*>(PDLFS_Load_rados_fio(fio_conf.c_str()));
  }
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

static bool GetExpandedId(Slice* input, uint64_t* r, uint64_t* s, uint64_t* i) {
#if defined(DELTAFS)
  if (!GetVarint64(input, r) || !GetVarint64(input, s)) return false;
#else
  *r = 0;
  *s = 0;
#endif
  if (!GetVarint64(input, i)) {
    return false;
  } else {
    return true;
  }
}

bool Fentry::DecodeFrom(Slice* input) {
  uint64_t parent_reg;
  uint64_t parent_snap;
  uint64_t parent_ino;
  uint32_t parent_zserver;
  Slice my_nhash;
  uint64_t my_reg;
  uint64_t my_snap;
  uint64_t my_ino;
  uint64_t size;
  uint32_t mode;
  uint32_t uid;
  uint32_t gid;
  uint32_t my_zserver;
  uint64_t ctime;
  uint64_t mtime;

  if (!GetExpandedId(input, &parent_reg, &parent_snap, &parent_ino) ||
      !GetLengthPrefixedSlice(input, &my_nhash) ||
      !GetVarint32(input, &parent_zserver) ||
      !GetExpandedId(input, &my_reg, &my_snap, &my_ino) ||
      !GetVarint64(input, &size) || !GetVarint32(input, &mode) ||
      !GetVarint32(input, &uid) || !GetVarint32(input, &gid) ||
      !GetVarint32(input, &my_zserver) || !GetVarint64(input, &ctime) ||
      !GetVarint64(input, &mtime)) {
    return false;
  } else {
#if defined(DELTAFS)
    pid = DirId(parent_reg, parent_snap, parent_ino);
#else
    pid = DirId(parent_ino);
#endif
    nhash = my_nhash.ToString();
    zserver = parent_zserver;
#if defined(DELTAFS)
    stat.SetRegId(my_reg);
    stat.SetSnapId(my_snap);
#endif
    stat.SetInodeNo(my_ino);
    stat.SetFileSize(size);
    stat.SetFileMode(mode);
    stat.SetUserId(uid);
    stat.SetGroupId(gid);
    stat.SetZerothServer(my_zserver);
    stat.SetChangeTime(ctime);
    stat.SetModifyTime(mtime);

    return true;
  }
}

static char* EncodeDirId(char* scratch, const DirId& id) {
  char* p = scratch;

#if defined(DELTAFS)
  p = EncodeVarint64(p, id.reg);
  p = EncodeVarint64(p, id.snap);
#endif
  p = EncodeVarint64(p, id.ino);

  return p;
}

static inline char* EncodeStatId(char* scratch, const Stat& stat) {
  return EncodeDirId(scratch, DirId(stat));
}

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
Slice Fentry::EncodeTo(char* scratch) const {
  char* p = scratch;

  p = EncodeDirId(p, pid);
  p = EncodeLengthPrefixedSlice(p, nhash);
  p = EncodeVarint32(p, zserver);
  p = EncodeStatId(p, stat);
  p = EncodeVarint64(p, stat.FileSize());
  p = EncodeVarint32(p, stat.FileMode());
  p = EncodeVarint32(p, stat.UserId());
  p = EncodeVarint32(p, stat.GroupId());
  p = EncodeVarint32(p, stat.ZerothServer());
  p = EncodeVarint64(p, stat.ChangeTime());
  p = EncodeVarint64(p, stat.ModifyTime());

  return Slice(scratch, p - scratch);
}

static inline std::string FileModeToString(mode_t mode) {
  char tmp[10];
  snprintf(tmp, sizeof(tmp), "%o", mode);
  return tmp;
}

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
