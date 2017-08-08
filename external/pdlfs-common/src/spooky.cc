/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "spooky_hash.h"

#include "pdlfs-common/spooky.h"

#include <string.h>

namespace pdlfs {

void Spooky128(const void* k, size_t n, const uint64_t seed1,
               const uint64_t seed2, void* result) {
  char* buf = static_cast<char*>(result);
  uint64_t v1 = seed1;
  uint64_t v2 = seed2;
  SpookyHash::Hash128(k, n, &v1, &v2);
  memcpy(buf + 8, &v2, 8);
  memcpy(buf, &v1, 8);
}

}  // namespace pdlfs
