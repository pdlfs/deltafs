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
template <typename KeyType>
void OrderedBlockBuilder<KeyType>::Add(const Slice& key, const Slice& value) {
  KeyType keyNum = DecodeFloat32(key.data());
  observed_.Extend(keyNum);
  keys_staging_.push_back(key_ptr(keyNum, bytes_written_));

  buffer_staging_.append(key.data(), key_size_);
  buffer_staging_.append(value.data(), value_size_);

  bytes_written_ += key_size_ + value_size_;
  num_items_++;
  if (not expected_.Inside(keyNum)) {
    num_items_oob_++;
  }
}

template <typename KeyType>
Slice OrderedBlockBuilder<KeyType>::Finish() {
  assert(!finished_);

  std::sort(keys_staging_.begin(), keys_staging_.end(),
            OrderedBlockBuilder::KeyPtrComparator);
  const char* rawbuf_staging = buffer_staging_.c_str();

  for (size_t i = 0; i < keys_staging_.size(); i++) {
    size_t key_offset = keys_staging_[i].second;
    buffer_.append(rawbuf_staging + key_offset, key_size_ + value_size_);
  }

  PutFixed32(&buffer_, value_size_);
  PutFixed32(&buffer_, key_size_);

  return AbstractBlockBuilder::Finish(kNoCompression, false);
}

template <typename KeyType>
size_t OrderedBlockBuilder<KeyType>::CurrentSizeEstimate() const {
  size_t result = bytes_written_;
  if (!finished_) {
    return result + 8;
  } else {
    return result;
  }
}

template <typename KeyType>
void OrderedBlockBuilder<KeyType>::Reset() {
  AbstractBlockBuilder::Reset();
  keys_staging_.clear();
  buffer_staging_.clear();
  observed_.Reset();
  n_ = 0;
  bytes_written_ = 0;
  num_items_ = 0;
  num_items_oob_ = 0;
}

/* Make compiler happy */
template class OrderedBlockBuilder<float>;
}  // namespace plfsio
}  // namespace pdlfs