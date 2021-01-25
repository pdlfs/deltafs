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
#pragma once

#include "pdlfs-common/pdlfs_platform.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

// Faster file copy bypassing user-space buffers.
#if defined(PDLFS_OS_LINUX)
extern Status FastCopy(const char* src, const char* dst);
#endif

}  // namespace pdlfs
