/*
 * Copyright (c) 2015-2019 Carnegie Mellon University and
 *         Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/slice.h"
#include "pdlfs-common/status.h"

#include <stdint.h>

namespace pdlfs {

class TableProperties {
 public:
  TableProperties() { Clear(); }
  ~TableProperties();

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

  Slice first_key() const { return first_key_; }
  Slice last_key() const { return last_key_; }
  uint64_t min_seq() const { return min_seq_; }
  uint64_t max_seq() const { return max_seq_; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);
  void Clear();

 private:
  std::string first_key_;
  std::string last_key_;
  uint64_t min_seq_;
  uint64_t max_seq_;
};

}  // namespace pdlfs
