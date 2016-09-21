#pragma once

/*
 * Copyright (c) 2015 The SILT Authors.
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
namespace ectrie {

template <typename BlockType>
class block_info {
 public:
  typedef BlockType block_type;

  static const size_t bytes_per_block = sizeof(block_type);
  static const size_t bits_per_block = bytes_per_block * 8;
  static const size_t bit_mask = bits_per_block - 1;

  static size_t size(size_t n) { return block_count(n) * bytes_per_block; }

  static size_t block_count(size_t n) {
    return (n + bits_per_block - 1) / bits_per_block;
  }
};

}  // namespace ectrie
}  // namespace pdlfs
