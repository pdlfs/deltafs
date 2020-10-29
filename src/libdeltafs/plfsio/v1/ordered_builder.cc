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

namespace pdlfs {
namespace plfsio {
template <typename KeyType>
void OrderedBlockBuilder<KeyType>::Add(const Slice& key, const Slice& value) {
  char* buf = arena_->Allocate(key_size_ + value_size_);
  memcpy(buf, key.data(), key_size_);
  memcpy(buf + key_size_, value.data(), value_size_);
  memtable_->Insert(buf);
  bytes_written_ += key_size_ + value_size_;
}

template <typename KeyType>
Slice OrderedBlockBuilder<KeyType>::Finish() {
  assert(!finished_);

  Table::Iterator iter(memtable_);
  iter.SeekToFirst();
  assert(iter.Valid());
  assert(bytes_written_ > 0);

  for (; iter.Valid(); iter.Next()) {
    buffer_.append(iter.key(), key_size_ + value_size_);
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
  delete arena_;
  delete memtable_;
  arena_ = new Arena();
  memtable_ = new Table(comparator_, arena_);
  bytes_written_ = 0;
  n_ = 0;
  AbstractBlockBuilder::Reset();
}

/* Make compiler happy */
template class OrderedBlockBuilder<float>;
}  // namespace plfsio
}  // namespace pdlfs