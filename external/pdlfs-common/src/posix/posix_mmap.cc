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
#include "posix_mmap.h"

namespace pdlfs {

// Up to 1000 mmaps for 64-bit binaries; none for smaller pointer sizes.
MmapLimiter::MmapLimiter() {
  MutexLock l(&mu_);
  SetAllowed(sizeof(void*) >= 8 ? 1000 : 0);
}

}  // namespace pdlfs
