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

#include "pdlfs-common/leveldb/table_properties.h"
#include "pdlfs-common/coding.h"
#include "pdlfs-common/leveldb/db/dbformat.h"

namespace pdlfs {

TableProperties::~TableProperties() {}

void TableProperties::Clear() {
  min_seq_ = kMaxSequenceNumber;
  max_seq_ = 0;
  first_key_.clear();
  last_key_.clear();
}

void TableProperties::EncodeTo(std::string* dst) const {
  PutVarint64(dst, min_seq_);
  PutVarint64(dst, max_seq_);
  PutLengthPrefixedSlice(dst, first_key_);
  PutLengthPrefixedSlice(dst, last_key_);
}

Status TableProperties::DecodeFrom(const Slice& src) {
  Clear();
  Slice input = src;
  Slice first_key;
  Slice last_key;
  if (!GetVarint64(&input, &min_seq_) || !GetVarint64(&input, &max_seq_) ||
      !GetLengthPrefixedSlice(&input, &first_key) ||
      !GetLengthPrefixedSlice(&input, &last_key)) {
    return Status::Corruption(Slice());
  }
  SetFirstKey(first_key);
  SetLastKey(last_key);
  return Status::OK();
}

}  // namespace pdlfs
