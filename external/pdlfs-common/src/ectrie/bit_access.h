#pragma once

/*
 * Copyright (c) 2015 The SILT Authors.
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <algorithm>

#include "block_info.h"

namespace pdlfs {
namespace ectrie {

// ---------------------------------------------------------------
// Example internal representation for uint8_t blocks
// ---------------------------------------------------------------
// block index:           0        1        2        3
// in-block offset:   76543210 76543210 76543210 76543210
//                   +--------+--------+--------+--------+
// blocks:           |01101011|00011010|10111010|110     |
//                   +--------+--------+--------+--------+
// bit_access index:  0          1          2
//                    01234567 89012345 67890123 456
// ---------------------------------------------------------------
class bit_access {
 public:
  template <typename BlockType>
  static size_t block_index(size_t i) {
    return i / block_info<BlockType>::bits_per_block;
  }

  template <typename BlockType>
  static size_t in_block_offset(size_t i) {
    return i & block_info<BlockType>::bit_mask;
  }

  // single bit operations
  template <typename BlockType>
  static void set(BlockType* v, size_t i) {
    v[block_index<BlockType>(i)] |= static_cast<BlockType>(
        BlockType(1) << (block_info<BlockType>::bits_per_block - 1 -
                         in_block_offset<BlockType>(i)));
  }

  template <typename BlockType>
  static void unset(BlockType* v, size_t i) {
    v[block_index<BlockType>(i)] &= static_cast<BlockType>(
        ~(BlockType(1) << (block_info<BlockType>::bits_per_block - 1 -
                           in_block_offset<BlockType>(i))));
  }

  template <typename BlockType>
  static void flip(BlockType* v, size_t i) {
    v[block_index<BlockType>(i)] ^= static_cast<BlockType>(
        BlockType(1) << (block_info<BlockType>::bits_per_block - 1 -
                         in_block_offset<BlockType>(i)));
  }

  template <typename BlockType>
  static bool get(const BlockType* v, size_t i) {
    return (v[block_index<BlockType>(i)] &
            static_cast<BlockType>(
                BlockType(1) << (block_info<BlockType>::bits_per_block - 1 -
                                 in_block_offset<BlockType>(i)))) != 0;
  }

  // multiple bits operations
  template <typename DestBlockType, typename SrcBlockType>
  static void copy_set(DestBlockType* dest, size_t dest_i,
                       const SrcBlockType* src, size_t src_i, size_t count) {
    dest += block_index<DestBlockType>(dest_i);
    src += block_index<SrcBlockType>(src_i);
    dest_i = in_block_offset<DestBlockType>(dest_i);
    src_i = in_block_offset<SrcBlockType>(src_i);

    while (count != 0) {
      // the remaining free space in the dest block
      size_t dest_available =
          block_info<DestBlockType>::bits_per_block - dest_i;
      // the remaining bits in the src block
      size_t src_available = block_info<SrcBlockType>::bits_per_block - src_i;
      // the actual copiable bits
      size_t copy_len =
          std::min(std::min(dest_available, src_available), count);

      // fetch a block from the source
      SrcBlockType src_v = *src;
      // trim off the higher bits before the range being copied and align bits
      // at MSB
      src_v = static_cast<SrcBlockType>(src_v << src_i);
      // trim off the lower bits after the range being copied and align bits at
      // LSB
      src_v = static_cast<SrcBlockType>(
          src_v >> (block_info<SrcBlockType>::bits_per_block - copy_len));
      // copy the value to the dest-like block
      DestBlockType dest_v = static_cast<DestBlockType>(src_v);
      // rearrange bits for the dest block
      dest_v = static_cast<DestBlockType>(
          dest_v << (block_info<DestBlockType>::bits_per_block - dest_i -
                     copy_len));
      // merge to the dest block (this does not clear out the existing bits at
      // the dest)
      *dest |= dest_v;

      // update the current block locations
      if (dest_available == copy_len) {
        dest++;
        dest_i = 0;
      } else
        dest_i += copy_len;
      if (src_available == copy_len) {
        src++;
        src_i = 0;
      } else
        src_i += copy_len;
      count -= copy_len;
    }
  }
};

}  // namespace ectrie
}  // namespace pdlfs
