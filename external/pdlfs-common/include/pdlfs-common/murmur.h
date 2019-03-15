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
// ----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.
void murmur_x86_32(const void* k, const int n, const uint32_t seed, void* out);
// Note - The x86 and x64 versions do _not_ produce the same results, as the
// algorithms are optimized for their respective platforms. You can still
// compile and run any of them on any platform, but your performance with the
// non-native version will be less than optimal.
void murmur_x86_128(const void* k, const int n, const uint32_t seed,
                    void* result);
void murmur_x64_128(const void* k, const int n, const uint32_t seed,
                    void* result);

}  // namespace pdlfs
