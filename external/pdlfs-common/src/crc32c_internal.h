/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/crc32c.h"

namespace pdlfs {
namespace crc32c {

// A software-based crc32c implementation. Used when hardware acceleration is
// not possible.
extern uint32_t ExtendSW(uint32_t init_crc, const char* data, size_t n);

// Return the crc32c of data[0,n-1].
inline uint32_t ValueSW(const char* data, size_t n) {
  return ExtendSW(0, data, n);
}

// Return 0 if SSE4.2 instructions are not available.
extern int CanAccelerateCrc32c();

// A faster crc32c implementation with optimizations that use special Intel
// SSE4.2 hardware instructions if they are available at runtime.
extern uint32_t ExtendHW(uint32_t init_crc, const char* data, size_t n);

// Return the crc32c of data[0,n-1].
inline uint32_t ValueHW(const char* data, size_t n) {
  return ExtendHW(0, data, n);
}

}  // namespace crc32c
}  // namespace pdlfs
