#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdint.h>

#include "pdlfs-common/slice.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

class Table;

class TableStats {
 public:
  TableStats() { Clear(); }
  ~TableStats();

  void SetFirstKey(const Slice& key) {
    first_key_.assign(key.data(), key.size());
  }

  void SetLastKey(const Slice& key) {
    last_key_.assign(key.data(), key.size());
  }

  void AddSeq(uint64_t seq) {
    if (seq < min_seq_) {
      min_seq_ = seq;
    }
    if (seq > max_seq_) {
      max_seq_ = seq;
    }
  }

  // Return true iff the table has all stats loaded.
  static bool HasStats(const Table* table);

  // Return the first key in the table.
  // REQUIRES: TableStats::HasStats(table) == true.
  static Slice FirstKey(const Table* table);
  // Return the last key in the table.
  // REQUIRES: TableStats::HasStats(table) == true.
  static Slice LastKey(const Table* table);
  // Return the maximum sequence number among all keys in the table.
  // REQUIRES: TableStats::HasStats(table) == true.
  static uint64_t MaxSeq(const Table* table);
  // Return the minimum sequence number among all keys in the table.
  // REQUIRES: TableStats::HasStats(table) == true.
  static uint64_t MinSeq(const Table* table);

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);
  std::string DebugString() const;
  void Clear();

 private:
  std::string first_key_;
  std::string last_key_;
  uint64_t min_seq_;
  uint64_t max_seq_;
};

}  // namespace pdlfs
