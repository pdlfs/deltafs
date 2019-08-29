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
#if defined(DELTAFS)
  snprintf(tmp, sizeof(tmp), "dirid[%llu:%llu:%llu]", LLU(reg), LLU(snap),
           LLU(ino));
#else
  snprintf(tmp, sizeof(tmp), "dirid[%llu]", LLU(ino));
#endif
  return tmp;
}

DirId::DirId() : reg(0), snap(0), ino(0) {}
#if !defined(DELTAFS)
DirId::DirId(uint64_t ino) : reg(0), snap(0), ino(ino) {}
#endif

namespace {
int compare64(uint64_t a, uint64_t b) {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}
}  // namespace
int DirId::compare(const DirId& other) const {
  int r;
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
