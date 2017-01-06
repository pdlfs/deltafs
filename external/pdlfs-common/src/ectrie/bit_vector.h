#pragma once

/*
 * Copyright (c) 2015 The SILT Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "bit_access.h"
#include "block_info.h"

namespace pdlfs {
namespace ectrie {

template <typename BlockType = uint32_t>
class bit_vector {
 public:
  typedef BlockType block_type;

  bit_vector();
  ~bit_vector();
  void clear();

  size_t size() const { return size_; }

  // single bit operations

  void push_back(bool b) {
    if (size_ == capacity_) {
      if (capacity_ == 0)
        capacity_ = 1;
      else
        capacity_ <<= 1;
      resize();
    }
    if (b) bit_access::set(buf_, size_);
    size_++;
  }

  void pop_back() {
    size_--;
    bit_access::unset(buf_, size_);
  }

  bool operator[](size_t i) const { return bit_access::get(buf_, i); }

  bool operator[](size_t i) { return bit_access::get(buf_, i); }

  // multiple bit operations

  template <typename SrcBlockType>
  void append(const bit_vector<SrcBlockType>& r, size_t i, size_t len) {
    size_t target_size = size_ + r.size();
    if (target_size > capacity_) {
      if (capacity_ == 0) capacity_ = 1;
      while (target_size > capacity_) capacity_ <<= 1;
      resize();
    }

    bit_access::copy_set(buf_, size_, r.buf_, i, len);
    size_ = target_size;
  }

  template <typename SrcBlockType>
  void append(const SrcBlockType* r, size_t i, size_t len) {
    size_t target_size = size_ + len;
    if (target_size > capacity_) {
      if (capacity_ == 0) capacity_ = 1;
      while (target_size > capacity_) capacity_ <<= 1;
      resize();
    }

    bit_access::copy_set(buf_, size_, r, i, len);
    size_ = target_size;
  }

  template <typename T>
  void append(T v, size_t len) {
    size_t target_size = size_ + len;
    if (target_size > capacity_) {
      if (capacity_ == 0) capacity_ = 1;
      while (target_size > capacity_) capacity_ <<= 1;
      resize();
    }

    bit_access::copy_set(buf_, size_, &v, block_info<T>::bits_per_block - len,
                         len);
    size_ = target_size;
  }

  template <typename T>
  T get(size_t i, size_t len) const {
    T v = 0;
    bit_access::copy_set<T, block_type>(&v, block_info<T>::bits_per_block - len,
                                        buf_, i, len);
    return v;
  }

  void compact();

 protected:
  void resize();

 private:
  block_type* buf_;
  size_t size_;
  size_t capacity_;
};

}  // namespace ectrie
}  // namespace pdlfs
