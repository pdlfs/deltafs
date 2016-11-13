#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

// XXX: Reuse formats defined by leveldb

#include "../../external/pdlfs-common/src/leveldb/block.h"
#include "../../external/pdlfs-common/src/leveldb/block_builder.h"
#include "../../external/pdlfs-common/src/leveldb/format.h"

#include "pdlfs-common/leveldb/comparator.h"
#include "pdlfs-common/leveldb/iterator.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/env.h"

namespace pdlfs {
namespace plfsio {
// Formats used by keys in the epoch block.
extern std::string EpochKey(uint32_t epoch, uint32_t table);
extern Status ParseEpochKey(const Slice& input, uint32_t* epoch,
                            uint32_t* table);

// TableHandle is a pointer to the extend of a file that stores the index block
// of a table. Each table handle also stores the smallest key and
// the largest key of that table.
class TableHandle {
 public:
  TableHandle();

  // The offset of the index block in the file.
  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t offset) { offset_ = offset; }

  // The size of the stored index block.
  uint64_t size() const { return size_; }
  void set_size(uint64_t size) { size_ = size; }

  // The smallest key within the table.
  Slice smallest_key() const { return smallest_key_; }
  void set_smallest_key(const Slice& key) { smallest_key_ = key.ToString(); }

  // The largest key within the table.
  Slice largest_key() const { return largest_key_; }
  void set_largest_key(const Slice& key) { largest_key_ = key.ToString(); }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

 private:
  std::string smallest_key_;
  std::string largest_key_;
  uint64_t offset_;
  uint64_t size_;
};

}  // namespace plfsio
}  // namespace pdlfs
