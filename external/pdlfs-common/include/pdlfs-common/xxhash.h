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

#include <stddef.h>
#include <stdint.h>

/* Notice extracted from xxHash homepage :

xxHash is an extremely fast Hash algorithm, running at RAM speed limits.
It also successfully passes all tests from the SMHasher suite.

Comparison (single thread, Windows Seven 32 bits, using SMHasher on a Core 2 Duo
@3GHz)

Name            Speed       Q.Score   Author
xxHash          5.4 GB/s     10
CrapWow         3.2 GB/s      2       Andrew
MumurHash 3a    2.7 GB/s     10       Austin Appleby
SpookyHash      2.0 GB/s     10       Bob Jenkins
SBox            1.4 GB/s      9       Bret Mulvey
Lookup3         1.2 GB/s      9       Bob Jenkins
SuperFastHash   1.2 GB/s      1       Paul Hsieh
CityHash64      1.05 GB/s    10       Pike & Alakuijala
FNV             0.55 GB/s     5       Fowler, Noll, Vo
CRC32           0.43 GB/s     9
MD5-32          0.33 GB/s    10       Ronald L. Rivest
SHA1-32         0.28 GB/s    10

Q.Score is a measure of quality of the hash function.
It depends on successfully passing SMHasher test set.
10 is a perfect score.

A 64-bits version, named XXH64, is available since r35.
It offers much better speed, but for 64-bits applications only.
Name     Speed on 64 bits    Speed on 32 bits
XXH64       13.8 GB/s            1.9 GB/s
XXH32        6.8 GB/s            6.0 GB/s
*/
namespace pdlfs {

/*
 * Can be used to fast map variable-length string identifiers to
 * 32-bit or 64-bit integer numbers.
 *
 * Code is highly portable, and hashes are identical
 * on all platforms (little or big endian).
 *
 * May also be used to do checksums.
 */

// 32-bit version
// NOTE: xxhash32(NULL, 0, 0) != 0
extern uint32_t xxhash32(const void* data, size_t n, uint32_t seed);

// 64-bit version
// WARNING: runs slowly on 32-bit platforms
// NOTE: xxhash64(NULL, 0, 0) != 0
extern uint64_t xxhash64(const void* data, size_t n, uint64_t seed);

}  // namespace pdlfs
