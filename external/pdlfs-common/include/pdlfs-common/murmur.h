#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdint.h>

namespace pdlfs {

// MurmurHash V3 - https://github.com/aappleby/smhasher/wiki/MurmurHash3
//
// Bulk speed test, hashing an 8-byte-aligned 256k block
// =====================================================
//
// Results are from an Intel Core 2 Quad Q9650 running at 3.0 ghz, running on a
// single core.
//
// |Hash                 | Speed      |
// |---------------------|------------|
// |FNV_x86_32           |  554 mb/sec|
// |FNV_x64_32           |  715 mb/sec|
// |SuperFastHash_x86_32 | 1224 mb/sec|
// |SuperFastHash_x64_32 | 1311 mb/sec|
// |Lookup3_x86_32       | 1234 mb/sec|
// |Lookup3_x64_32       | 1265 mb/sec|
// |MurmurHash2_x86_32   | 2577 mb/sec|
// |MurmurHash2_x86_64   | 3352 mb/sec|
// |MurmurHash2_x64_64   | 2857 mb/sec|
// |MurmurHash3_x86_32   | 3105 mb/sec|
// |MurmurHash3_x86_128  | 2684 mb/sec|
// |MurmurHash3_x64_128  | 5058 mb/sec|
//
void murmur_x64_128(const void* key, int len, uint32_t seed, void* out);

}  // namespace pdlfs
