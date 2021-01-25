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

#include "pdlfs-common/status.h"

namespace pdlfs {

// Copy *src to *dst. Return OK on success, or a non-OK status on errors.
extern Status Copy(const char* src, const char* dst);

}  // namespace pdlfs
