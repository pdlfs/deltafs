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
 * SpookyHash: a 128-bit noncryptographic hash function.
 * By Bob Jenkins, public domain.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>

namespace pdlfs {

// Compute a 128-bit hash for a given input. User provides two 64-bit seeds for
// hashing. Result is stored in *result.
extern void Spooky128(const void* k, size_t n, const uint64_t seed1,
                      const uint64_t seed2, void* result);

}  // namespace pdlfs
