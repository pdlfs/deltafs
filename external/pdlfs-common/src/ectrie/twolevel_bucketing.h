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
#include <vector>

namespace pdlfs {
namespace ectrie {

template <typename ValueType = uint16_t, typename UpperValueType = uint32_t>
class twolevel_bucketing {
 public:
  typedef ValueType value_type;
  typedef UpperValueType upper_value_type;

 public:
  twolevel_bucketing(size_t size = 0, size_t keys_per_bucket = 1,
                     size_t keys_per_block = 1);

  void resize(size_t size, size_t keys_per_bucket, size_t keys_per_block);
  void insert(size_t index_offset, size_t dest_offset);
  void finalize() {}

  size_t index_offset(size_t i) const;
  size_t dest_offset(size_t i) const;

  size_t size() const;
  size_t bit_size() const;

 protected:
  size_t upper_index_offset(size_t i) const;
  size_t upper_dest_offset(size_t i) const;

 private:
  size_t size_;
  size_t keys_per_bucket_;
  size_t upper_bucket_size_;

  std::vector<std::pair<value_type, value_type> > bucket_info_;
  std::vector<std::pair<upper_value_type, upper_value_type> >
      upper_bucket_info_;

  size_t current_i_;
};

}  // namespace ectrie
}  // namespace pdlfs
