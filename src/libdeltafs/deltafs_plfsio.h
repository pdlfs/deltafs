#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <string>
#include <vector>

#include "pdlfs-common/leveldb/iterator.h"
#include "pdlfs-common/slice.h"

namespace pdlfs {
namespace plfsio {

// Append-only in-memory table implementation.
class WriteBuffer {
 public:
  explicit WriteBuffer() : num_entries_(0), finished_(false) {}
  ~WriteBuffer() {}

  void Reserve(uint32_t num_entries, size_t size_per_entry);
  uint32_t NumEntries() const { return num_entries_; }
  void Append(const Slice& key, const Slice& value);
  Iterator* NewIterator() const;
  void Finish();

 private:
  std::vector<uint32_t> offsets_;  // Starting offsets of inserted keys
  std::string buffer_;
  uint32_t num_entries_;
  bool finished_;

  // No copying allowed
  void operator=(const WriteBuffer&);
  WriteBuffer(const WriteBuffer&);

  class Iter;
};

class TableLogger {
 public:
 private:
  // XXX: TODO
};

// Deltafs plfs-style io api.
class Writer {
 public:
 private:
  // XXX: TODO
};

}  // namespace plfsio
}  // namespace pdlfs
