/*
 * Copyright (c) 2011 The LevelDB Authors.
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

namespace pdlfs {

/*
 * Simple hash function used by many of our internal data structures
 * such as LRU caches and bloom filters.
 *
 * Current implementation uses a schema similar to
 * the murmur hash.
 */
extern uint32_t Hash(const char* data, size_t n, uint32_t seed);

}  // namespace pdlfs
