/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "crc32c_internal.h"

namespace pdlfs {
namespace crc32c {

// If hardware acceleration (via SSE4_2) is possible during runtime, crc32c
// calculation will be dynamically switched to a hardware-assisted
// implementation. Otherwise, a pure software-based implementation will be used.
uint32_t Extend(uint32_t crc, const char* data, size_t n) {
  static const int hw = CanAccelerateCrc32c();
  return hw ? ExtendHW(crc, data, n) : ExtendSW(crc, data, n);
}

}  // namespace crc32c
}  // namespace pdlfs
