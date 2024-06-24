/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
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
#include "ordered_builder.h"

#include "coding_float.h"

#include <algorithm>

namespace pdlfs {
namespace plfsio {

//
// add key/value pair to staging area, updating our accounting.
// caller should have already done error checking, but we'll assert
// in non-NDEBUG builds to be safe...
//
void OrderedBlockBuilder::Add(const Slice& key, const Slice& value) {
  assert(!finished_);
  assert(key.size() == key_size_ && value.size() == value_size_);
  float keyNum = DecodeFloat32(key.data());

  // we'll use saved value of num_items in Finish() to locate the value data
  keys_staging_.push_back(key_ptr(keyNum, obrange_.num_items()));
  buffer_staging_.append(value.data(), value_size_);

  bytes_written_ += key_size_ + value_size_;
  obrange_.Observe(keyNum); /* incr num_items(), maybe num_items_oob() */
}

//
// sort staged k/v data in keys_staging_ and buffer_staging_ into buffer_
// and then pass to abstract builder to finish (optional prefix removal
// and compression... currently not used).
//
Slice OrderedBlockBuilder::Finish() {
  assert(!finished_);
  assert(sizeof(float) == 4u);

  if (!skip_sort_) {
    // do an in-place sort of keys_staging_ by float key
    std::sort(keys_staging_.begin(), keys_staging_.end(),
              OrderedBlockBuilder::KeyPtrComparator);
  }

  // buffer_ should be empty (haven't used it yet and buf_start_ is always
  // zero for us).  resize to target.
  size_t buf_offset = buffer_.size();
  assert(buf_offset == 0u);
  buffer_.resize(buf_offset + obrange_.num_items() * (key_size_ + value_size_));

  // put keys in buffer - same regardless of whether they're sorted or not
  size_t num_keys = keys_staging_.size();
  for (size_t i = 0; i < num_keys; i++) {
    float key = keys_staging_[i].first;
    EncodeFloat32(&buffer_[buf_offset], key);
    buf_offset += key_size_;
  }

  // use sorted list of keys to append sorted values to buffer_
  const char* rawbuf_staging = buffer_staging_.c_str();

  if (skip_sort_) {
    // if we're not sorting, we can just copy the values in order
    size_t valblk_sz = value_size_ * num_keys;
    memcpy(&buffer_[buf_offset], rawbuf_staging, valblk_sz);
    buf_offset += valblk_sz;
  } else {
    // otherwise, we need to copy the values in the order of the sorted keys
    for (size_t i = 0; i < num_keys; i++) {
      size_t val_offset = keys_staging_[i].second;
      memcpy(&buffer_[buf_offset], rawbuf_staging + val_offset * value_size_,
             value_size_);
      buf_offset += value_size_;
    }
  }

  // abstract builder will set finished_  (currently not compressing)
  return AbstractBlockBuilder::Finish(kNoCompression, false);
}

//
// reset buffering for reuse, but don't clear fields used by
// CopyFrom()  (i.e. expected range and updcnt_ ).
//
void OrderedBlockBuilder::Reset() {
  AbstractBlockBuilder::Reset();  // clears buffer_ and finished_
  keys_staging_.clear();
  buffer_staging_.clear();
  bytes_written_ = 0;
  // XXX to be safe?  likely re-cleared when new user calls obrange_.Set()
  //     to set a new expected range
  obrange_.ClearObservations();
}

}  // namespace plfsio
}  // namespace pdlfs
