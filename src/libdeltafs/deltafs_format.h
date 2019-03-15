#pragma once

/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <sys/stat.h>
#include <sys/types.h>

#include "pdlfs-common/fstypes.h"
#include "pdlfs-common/pdlfs_platform.h"

namespace pdlfs {

// The result of a successful open operation.
struct FileInfo {
  Stat stat;
  int fd;
};

#define DELTAFS_SEC(micros) ((micros) / 1000000ULL)
#define DELTAFS_NSEC(micros) (((micros) % 1000000ULL) * 1000ULL)

inline void __cpstat(const Stat& src, struct stat* buf) {
  buf->st_ino = src.InodeNo();
  buf->st_size = src.FileSize();
  buf->st_mode = src.FileMode();

  buf->st_ctime = DELTAFS_SEC(src.ChangeTime());
#if defined(PDLFS_OS_LINUX) && defined(_STATBUF_ST_NSEC)
  buf->st_ctim.tv_nsec = DELTAFS_NSEC(src.ChangeTime());
#endif

  buf->st_mtime = DELTAFS_SEC(src.ModifyTime());
#if defined(PDLFS_OS_LINUX) && defined(_STATBUF_ST_NSEC)
  buf->st_mtim.tv_nsec = DELTAFS_NSEC(src.ModifyTime());
#endif

  buf->st_atime = buf->st_mtime;
#if defined(PDLFS_OS_LINUX) && defined(_STATBUF_ST_NSEC)
  buf->st_atim.tv_nsec = buf->st_mtim.tv_nsec;
#endif

  buf->st_gid = src.GroupId();
  buf->st_uid = src.UserId();
}

#undef DELTAFS_NSEC
#undef DELTAFS_SEC

}  // namespace pdlfs
