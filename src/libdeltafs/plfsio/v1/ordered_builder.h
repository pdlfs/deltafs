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

#include <assert.h>
#include "builder.h"
#include "range.h"

namespace pdlfs {
namespace plfsio {

//
// ordered building blocks allow us to insert key/value pairs in a block.
// keys must be a float, and all values should have the same size.
// k/v data is staged separately in keys_staging_ and buffer_staging_
// (for the values).  at Finish() time we sort data by key and copy
// the sorted keys and then sorted values into AbstractBlockBuilder's buffer_
// for processing (e.g. saving to storage via compaction).  we track our
// key range as data is inserted, including out of expected bounds (oob)
// inserts.  we expect higher-level code to provide locking.  higher-level
// code should also track how large we are, and if we are full (or
// there is a flush operation) it should Finish() our block and use
// a compaction to write data our data to backing store.
//
class OrderedBlockBuilder : public AbstractBlockBuilder {
 public:
  explicit OrderedBlockBuilder(const DirOptions& options)
      : AbstractBlockBuilder(NULL),        // not using a low-level comparator
        key_size_(options.key_size),
        value_size_(options.value_size),
        rank_(options.rank),
        skip_sort_(options.skip_sort),
        bytes_written_(0),
        updcnt_(0) {
    assert(sizeof(float) == key_size_);    // all keys must be floats
  }

  // add a key/value pair to our block
  void Add(const Slice& key, const Slice& value);

  // sort our staged data into buffer_ (we currently don't compress)
  Slice Finish();

  // return an estimate of the size of the block we are building
  size_t CurrentSizeEstimate() const { return bytes_written_; }

  // clears buffered data; preserves our expected range and updcnt_
  void Reset();

  // true if no entries have been added since the last Reset()
  bool empty() const { return bytes_written_ == 0; }

  // return copy of range info
  ObservedRange GetRangeInfo() { return obrange_; }

  // is "prop" in the expected range?
  bool Inside(float prop) { return obrange_.Inside(prop); }

  // number of times UpdateExpectedRange() was called
  uint32_t GetUpdateCount() const { return updcnt_; }

  // copy expected range and updcnt_ from other (our observations are cleared).
  // using during compaction when our empty block is replacing a full one
  // that is getting compacted.
  void CopyFrom(OrderedBlockBuilder* other) {
    obrange_.Set(other->obrange_.rmin(), other->obrange_.rmax());
    updcnt_ = other->updcnt_;
  }

  // install new expected range in our (currently) empty block.
  void UpdateExpectedRange(float rmin, float rmax) {
    assert(obrange_.num_items() == 0);   /* we should be empty */
    updcnt_++;
    obrange_.Set(rmin, rmax);   /* asserts valid */
  }

 private:
  /* fixed at construction time */
  const size_t key_size_;          // must be sizeof(float)
  const size_t value_size_;        // from DirOptions
  const int rank_;                 // from DirOptions
  const bool skip_sort_;           // from DirOptions (disable sort)

  /* Writing properties */
  typedef std::pair<float, size_t> key_ptr; // <key, val offset in staging buf>
  std::vector<key_ptr> keys_staging_;       // current set of keys
  std::string buffer_staging_;              // concat'd buf of current values
  size_t bytes_written_;                    // # key and value bytes added

  /* range related properties */
  ObservedRange obrange_;                   // our block's observed range
  uint32_t updcnt_;                         // #calls to UpdateExpectedRange()


  // comparator used by Finish() to sort our staged keys
  static bool KeyPtrComparator(const key_ptr& lhs, const key_ptr& rhs) {
    return lhs.first < rhs.first;
  }
};
}  // namespace plfsio
}  // namespace pdlfs
