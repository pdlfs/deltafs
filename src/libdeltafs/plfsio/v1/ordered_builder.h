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

namespace pdlfs {
namespace plfsio {

template <typename KeyType>
class TypePrefixedComparator {
 public:
  int operator()(const char* a, const char* b) const {
    const KeyType key_a = *reinterpret_cast<const KeyType*>(a);
    const KeyType key_b = *reinterpret_cast<const KeyType*>(b);
    // XXX: check
    return key_a < key_b ? -1 : key_a > key_b;
  }
};

template <typename KeyType>
class OrderedBlockBuilder : public AbstractBlockBuilder {
 public:
  explicit OrderedBlockBuilder(const DirOptions& options)
      : AbstractBlockBuilder(BytewiseComparator()),
        arena_(nullptr),
        memtable_(nullptr),
        value_size_(options.value_size),
        key_size_(options.key_size),
        bytes_written_(0),
        n_(0) {
    // TODO: what is this used for again?
    cmp_ = NULL;
    arena_ = new Arena();
    memtable_ = new Table(comparator_, arena_);
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

 private:
  typedef SkipList<const char*, TypePrefixedComparator<float>> Table;

  TypePrefixedComparator<KeyType> comparator_;
  Arena* arena_;
  Table* memtable_;
  const size_t value_size_;
  const size_t key_size_;
  size_t bytes_written_;
  size_t n_;
};
}  // namespace plfsio
}  // namespace pdlfs