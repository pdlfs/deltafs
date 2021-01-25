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

#include "pdlfs-common/fsdbx.h"

namespace pdlfs {

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
