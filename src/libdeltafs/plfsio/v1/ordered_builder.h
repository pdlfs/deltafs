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

namespace pdlfs {
namespace plfsio {

//
// the ordered block builder allows us to insert key/value pairs
// into a block.  keys must be floats.  all values should have
// the same size.  we stage added k/v data separately in keys_staging_
// and buffer_staging_ (the latter for the values).  at Finish()
// time we sort the data by keys and copy the sorted keys and
// then the sorted values out into the AbstractBlockBuilder's buffer_
// for processing (e.g. saving to storage via compaction).  we
// track our key Range as k/v data is inserted.  this includes
// k/v adds that are outside of our expected Range (i.e. out of
// bounds or oob).   we expect higher-level code store its
// k/v data in a set of ordered building blocks, tracking how
// full they are.  when an ordered building block is full
// (or needs to be flushed), the higher level code will Finish()
// the block and use a compaction to write the data to storage.
//
class OrderedBlockBuilder : public AbstractBlockBuilder {
 public:
  explicit OrderedBlockBuilder(const DirOptions& options)
      : AbstractBlockBuilder(NULL),        // not using a low-level comparator
        key_size_(options.key_size),
        value_size_(options.value_size),
        rank_(options.rank),
        bytes_written_(0),
        updcnt_(0),
        num_items_(0),
        num_items_oob_(0) {
    assert(sizeof(float) == key_size_);    // all keys must be floats
  }

  // add a key/value pair to our block
  void Add(const Slice& key, const Slice& value);

  // sort our staged data into buffer_ (we currently don't compress)
  Slice Finish();

  // return an estimate of the size of the block we are building
  size_t CurrentSizeEstimate() const { return bytes_written_; }

  // clears buffered data; preserves expected_ and updcnt_
  void Reset();

  // true if no entries have been added since the last Reset()
  bool empty() const { return bytes_written_ == 0; }

  // is "prop" in the expected_ range?
  bool Inside(float prop) { return expected_.Inside(prop); }

  Range GetExpectedRange() { return expected_; }

  Range GetObservedRange() { return observed_; }

  // number of times UpdateExpectedRange() was called
  uint32_t GetUpdateCount() const { return updcnt_; }

  // # of k/v pair we are holding and # them that are out of expected_ range
  void GetNumItems(uint32_t& num_items, uint32_t& num_oob) const {
    num_items = num_items_;
    num_oob = num_items_oob_;
  }

  // copy expected_ range and updcnt_ from other (our num_items_ should be 0).
  // using during compaction when our empty block is replacing a full one
  // that is getting compacted.
  void CopyFrom(OrderedBlockBuilder* other) {
    UpdateExpectedRange(other->GetExpectedRange());
    updcnt_ = other->updcnt_;
  }

  // install new expected_ range in our (currently) empty block.
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
  /* fixed at construction time */
  const size_t key_size_;          // must be sizeof(float)
  const size_t value_size_;        // from DirOptions
  const int rank_;                 // from DirOptions

  /* Writing properties */
  typedef std::pair<float, size_t> key_ptr; // <key, val offset in staging buf>
  std::vector<key_ptr> keys_staging_;       // current set of keys
  std::string buffer_staging_;              // concat'd buf of current values
  size_t bytes_written_;                    // # key and value bytes added

  /* Range properties */
  Range expected_;                          // set by UpdateExpectedRange()
  Range observed_;                          // range observed by Add()
  uint32_t updcnt_;                         // #calls to UpdateExpectedRange()
  uint32_t num_items_ = 0;                  // k/v pairs we've Add()'d
  uint32_t num_items_oob_ = 0;              // added, not in expected_ range


  // comparator used by Finish() to sort our staged keys
  static bool KeyPtrComparator(const key_ptr& lhs, const key_ptr& rhs) {
    return lhs.first < rhs.first;
  }
};
}  // namespace plfsio
}  // namespace pdlfs
