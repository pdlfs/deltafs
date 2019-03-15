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

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>

namespace pdlfs {
namespace crc32c {

// Return the crc32c of concat(A, data[0,n-1]) where init_crc is the
// crc32c of some string A.  Extend() is often used to maintain the
// crc32c of a stream of data.
// If hardware acceleration (via SSE4_2) is possible during runtime, crc32c
// calculation will be dynamically switched to a hardware-assisted
// implementation. Otherwise, a pure software-based implementation will be used.
extern uint32_t Extend(uint32_t init_crc, const char* data, size_t n);

// Return the crc32c of data[0,n-1]
inline uint32_t Value(const char* data, size_t n) { return Extend(0, data, n); }

// Internal to implementation. API user should ignore.
static const uint32_t kMaskDelta = 0xa282ead8ul;

// Return a masked representation of crc.
//
// Motivation: it is problematic to compute the CRC of a string that
// contains embedded CRCs.  Therefore we recommend that CRCs stored
// somewhere (e.g., in files) should be masked before being stored.
inline uint32_t Mask(uint32_t crc) {
  // Rotate right by 15 bits and add a constant.
  return ((crc >> 15) | (crc << 17)) + kMaskDelta;
}

// Return the crc whose masked representation is masked_crc.
inline uint32_t Unmask(uint32_t masked_crc) {
  uint32_t rot = masked_crc - kMaskDelta;
  return ((rot >> 17) | (rot << 15));
}

}  // namespace crc32c
}  // namespace pdlfs
