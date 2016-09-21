/*
 * Copyright (c) 2015 The SILT Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <string.h>

#include "bit_vector.h"

namespace pdlfs {
namespace ectrie {

template <typename BlockType>
bit_vector<BlockType>::bit_vector() : buf_(NULL), size_(0), capacity_(8) {
  resize();
}

template <typename BlockType>
bit_vector<BlockType>::~bit_vector() {
  clear();
}

template <typename BlockType>
void bit_vector<BlockType>::clear() {
  if (buf_ != NULL) free(buf_);
  buf_ = NULL;
  size_ = 0;
  capacity_ = 0;
}

template <typename BlockType>
void bit_vector<BlockType>::compact() {
  if (size_ != capacity_) {
    capacity_ = size_;
    resize();
  }
}

template <typename BlockType>
void bit_vector<BlockType>::resize() {
  size_t old_byte_size = block_info<block_type>::size(size_);
  size_t new_byte_size = block_info<block_type>::size(capacity_);

  if (new_byte_size == 0) {
    assert(size_ == 0 && capacity_ == 0);
    if (buf_ != NULL) free(buf_);
    buf_ = NULL;
    return;
  }

  block_type* new_buf = reinterpret_cast<block_type*>(
      realloc(reinterpret_cast<void*>(buf_), new_byte_size));

  if (!new_buf) {
    new_buf = reinterpret_cast<block_type*>(malloc(new_byte_size));
    memcpy(new_buf, buf_, old_byte_size);
    free(buf_);
  }

  buf_ = new_buf;

  if (new_byte_size > old_byte_size)
    memset(reinterpret_cast<uint8_t*>(new_buf) + old_byte_size, 0,
           new_byte_size - old_byte_size);
}

template class bit_vector<uint8_t>;
template class bit_vector<uint16_t>;
template class bit_vector<uint32_t>;
template class bit_vector<uint64_t>;

}  // namespace ectrie
}  // namespace pdlfs
