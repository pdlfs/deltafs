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

#include "pdlfs-common/leveldb/skiplist.h"

#include <float.h>

namespace pdlfs {
namespace plfsio {
class PartitionManifestWriter;

struct Range {
  float range_min = FLT_MAX;
  float range_max = FLT_MIN;

  Range& operator=(Range& r) {
    range_min = r.range_min;
    range_max = r.range_max;
    return *this;
  }

  void Reset() {
    range_min = FLT_MAX;
    range_max = FLT_MIN;
  }

  bool Inside(float f) const { return (f >= range_min && f <= range_max); }

  bool IsValid() const {
    return ((range_min == FLT_MAX && range_max == FLT_MIN) or
            (range_min < range_max));
  }

  void Extend(float f) {
    range_min = std::min(range_min, f);
    range_max = std::max(range_min, f);
  }
};

template <typename KeyType>
class OrderedBlockBuilder : public AbstractBlockBuilder {
 public:
  explicit OrderedBlockBuilder(const DirOptions& options)
      : AbstractBlockBuilder(BytewiseComparator()),
        value_size_(options.value_size),
        key_size_(options.key_size),
        n_(0),
        bytes_written_(0),
        num_items_(0),
        num_items_oob_(0) {
    // TODO: what is this used for again?
    cmp_ = NULL;
    assert(sizeof(KeyType) == key_size_);
  }

  void Add(const Slice& key, const Slice& value);

  Slice Finish();

  // Return the number of entries inserted.
  size_t NumEntries() const { return n_; }

  // Return an estimate of the size of the block we are building.
  size_t CurrentSizeEstimate() const;

  void Reset();

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return bytes_written_ == 0; }

  bool Inside(float prop) { return expected_.Inside(prop); }

  Range GetExpectedRange() { return expected_; }

  Range GetObservedRange() { return observed_; }

  void GetWriteStats(KeyType& range_min, KeyType& range_max,
                     uint32_t& num_items, uint32_t& num_oob) const {
    range_min = expected_.range_min;
    range_max = expected_.range_max;
    num_items = num_items_;
    num_oob = num_items_oob_;
  }

  void UpdateExpectedRange(Range range) { expected_ = range; }

  void UpdateExpectedRange(float rmin, float rmax) {
    assert(rmin <= rmax);

    expected_.range_min = rmin;
    expected_.range_max = rmax;
  }

 private:
  /* Writing properties */
  std::string buffer_staging_;
  typedef std::pair<KeyType, size_t> key_ptr;
  std::vector<key_ptr> keys_staging_;
  const size_t value_size_;
  const size_t key_size_;
  size_t n_;
  size_t bytes_written_;

  /* Range properties */
  Range expected_;
  Range observed_;
  uint32_t num_items_ = 0;
  uint32_t num_items_oob_ = 0;

  static bool KeyPtrComparator(const key_ptr& lhs, const key_ptr& rhs) {
    return lhs.first < rhs.first;
  }
};
}  // namespace plfsio
}  // namespace pdlfs