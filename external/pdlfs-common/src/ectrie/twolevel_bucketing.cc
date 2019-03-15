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

#include <assert.h>

#include "twolevel_bucketing.h"

namespace pdlfs {
namespace ectrie {

template <typename ValueType, typename UpperValueType>
twolevel_bucketing<ValueType, UpperValueType>::twolevel_bucketing(
    size_t size, size_t keys_per_bucket, size_t keys_per_block) {
  resize(size, keys_per_bucket, keys_per_block);
}

template <typename ValueType, typename UpperValueType>
void twolevel_bucketing<ValueType, UpperValueType>::resize(
    size_t size, size_t keys_per_bucket, size_t keys_per_block) {
  size_ = size;
  keys_per_bucket_ = keys_per_bucket;

  size_t bits_per_key_bound = 4;
  upper_bucket_size_ = (static_cast<size_t>(1) << (sizeof(ValueType) * 8)) /
                       (bits_per_key_bound * keys_per_bucket_);
  if (upper_bucket_size_ == 0) upper_bucket_size_ = 1;

  bucket_info_.resize(size);
  upper_bucket_info_.resize(size / upper_bucket_size_ + 2);
  upper_bucket_info_[0].first = 0;
  upper_bucket_info_[0].second = 0;
  current_i_ = 0;
}

template <typename ValueType, typename UpperValueType>
void twolevel_bucketing<ValueType, UpperValueType>::insert(size_t index_offset,
                                                           size_t dest_offset) {
  assert(current_i_ < size_);

  size_t base_index_offset =
      upper_index_offset(current_i_ / upper_bucket_size_);
  size_t base_dest_offset = upper_dest_offset(current_i_ / upper_bucket_size_);

  bucket_info_[current_i_].first =
      static_cast<value_type>(index_offset - base_index_offset);
  bucket_info_[current_i_].second =
      static_cast<value_type>(dest_offset - base_dest_offset);
  upper_bucket_info_[current_i_ / upper_bucket_size_ + 1].first =
      static_cast<upper_value_type>(index_offset);
  upper_bucket_info_[current_i_ / upper_bucket_size_ + 1].second =
      static_cast<upper_value_type>(dest_offset);

  assert(this->index_offset(current_i_) == index_offset);
  assert(this->dest_offset(current_i_) == dest_offset);

  current_i_++;
}

template <typename ValueType, typename UpperValueType>
size_t twolevel_bucketing<ValueType, UpperValueType>::index_offset(
    size_t i) const {
  assert(i < size_);
  return upper_index_offset(i / upper_bucket_size_) + bucket_info_[i].first;
}

template <typename ValueType, typename UpperValueType>
size_t twolevel_bucketing<ValueType, UpperValueType>::dest_offset(
    size_t i) const {
  assert(i < size_);
  return upper_dest_offset(i / upper_bucket_size_) + bucket_info_[i].second;
}

template <typename ValueType, typename UpperValueType>
size_t twolevel_bucketing<ValueType, UpperValueType>::upper_index_offset(
    size_t i) const {
  assert(i < size_ / upper_bucket_size_ + 1);
  return upper_bucket_info_[i].first;
}

template <typename ValueType, typename UpperValueType>
size_t twolevel_bucketing<ValueType, UpperValueType>::upper_dest_offset(
    size_t i) const {
  assert(i < size_ / upper_bucket_size_ + 1);
  return upper_bucket_info_[i].second;
}

template <typename ValueType, typename UpperValueType>
size_t twolevel_bucketing<ValueType, UpperValueType>::size() const {
  return size_;
}

template <typename ValueType, typename UpperValueType>
size_t twolevel_bucketing<ValueType, UpperValueType>::bit_size() const {
  size_t bit_size = bucket_info_.size() * 2 * sizeof(value_type) * 8;
  bit_size += upper_bucket_info_.size() * 2 * sizeof(upper_value_type) * 8;
  return bit_size;
}

template class twolevel_bucketing<>;

}  // namespace ectrie
}  // namespace pdlfs
