#pragma once

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stddef.h>
#include <stdint.h>

namespace pdlfs {

extern uint32_t Hash(const char* data, size_t n, uint32_t seed);

}  // namespace pdlfs
