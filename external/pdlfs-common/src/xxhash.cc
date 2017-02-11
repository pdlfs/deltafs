/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "xximpl.h"

#include "pdlfs-common/xxhash.h"

namespace pdlfs {

uint32_t xxhash32(const void* data, size_t n, uint32_t seed) {
  return __pdlfs_XXH32(data, n, seed);
}

uint64_t xxhash64(const void* data, size_t n, uint64_t seed) {
  return __pdlfs_XXH64(data, n, seed);
}

}  // namespace pdlfs
