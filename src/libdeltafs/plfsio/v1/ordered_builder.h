/*
 * Copyright (c) 2020 Carnegie Mellon University,
 * Copyright (c) 2020 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */

#pragma once

#include "builder.h"
#include "range.h"

#include <float.h>

namespace pdlfs {
namespace plfsio {

class OrderedBlockBuilder : public AbstractBlockBuilder {
 public:
  explicit OrderedBlockBuilder(const DirOptions& options)
      : AbstractBlockBuilder(BytewiseComparator()),
        value_size_(options.value_size),
        key_size_(options.key_size),
        bytes_written_(0),
        updcnt_(0),
        num_items_(0),
        num_items_oob_(0),
        rank_(options.rank) {
    // TODO: what is this used for again?
    cmp_ = NULL;
    assert(sizeof(float) == key_size_);
  }

  void Add(const Slice& key, const Slice& value);

  Slice Finish();

  // Return an estimate of the size of the block we are building.
  size_t CurrentSizeEstimate() const;

  void Reset();

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return bytes_written_ == 0; }

  bool Inside(float prop) { return expected_.Inside(prop); }

  Range GetExpectedRange() { return expected_; }

  Range GetObservedRange() { return observed_; }

  uint32_t GetUpdateCount() const { return updcnt_; }

  void GetNumItems(uint32_t& num_items, uint32_t& num_oob) const {
    num_items = num_items_;
    num_oob = num_items_oob_;
  }

  void CopyFrom(OrderedBlockBuilder* other) {
    UpdateExpectedRange(other->GetExpectedRange());
    updcnt_ = other->updcnt_;
  }

  void UpdateExpectedRange(Range range) {
    updcnt_++;
    expected_ = range;
    assert(expected_.IsValid());
    assert(num_items_ == 0);
  }

  void UpdateExpectedRange(float rmin, float rmax) {
    UpdateExpectedRange(Range(rmin, rmax));
  }

 private:
  /* Writing properties */
  std::string buffer_staging_;
  typedef std::pair<float, size_t> key_ptr;
  std::vector<key_ptr> keys_staging_;
  const size_t value_size_;
  const size_t key_size_;
  size_t bytes_written_;

  /* Range properties */
  Range expected_;
  Range observed_;
  uint32_t updcnt_;
  uint32_t num_items_ = 0;
  uint32_t num_items_oob_ = 0;

  const int rank_;

  static bool KeyPtrComparator(const key_ptr& lhs, const key_ptr& rhs) {
    return lhs.first < rhs.first;
  }
};
}  // namespace plfsio
}  // namespace pdlfs
