/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdio.h>
#include <string.h>

#include "deltafs_plfsio_format.h"

namespace pdlfs {
namespace plfsio {

Status ParseEpochKey(const Slice& input, uint32_t* epoch, uint32_t* table) {
  int parsed_epoch;
  int parsed_table;
  int r = sscanf(input.data(), "%04d-%04d", &parsed_epoch, &parsed_table);
  if (r != 2) {
    return Status::Corruption("bad epoch key");
  } else {
    *epoch = parsed_epoch;
    *table = parsed_table;
    return Status::OK();
  }
}

std::string EpochKey(uint32_t epoch, uint32_t table) {
  assert(epoch < 9999);
  assert(table < 9999);
  char tmp[10];
  snprintf(tmp, sizeof(tmp), "%04d-%04d", int(epoch), int(table));
  return tmp;
}

void TableHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  assert(!smallest_key_.empty());
  assert(!largest_key_.empty());

  PutLengthPrefixedSlice(dst, smallest_key_);
  PutLengthPrefixedSlice(dst, largest_key_);
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status TableHandle::DecodeFrom(Slice* input) {
  Slice smallest_key;
  Slice largest_key;
  if (!GetLengthPrefixedSlice(input, &smallest_key) ||
      !GetLengthPrefixedSlice(input, &largest_key) ||
      !GetVarint64(input, &offset_) || !GetVarint64(input, &size_)) {
    return Status::Corruption("bad table handle");
  } else {
    smallest_key_ = smallest_key.ToString();
    largest_key_ = largest_key.ToString();
    return Status::OK();
  }
}

inline TableHandle::TableHandle()
    : offset_(~static_cast<uint64_t>(0) /* Invalid offset */),
      size_(~static_cast<uint64_t>(0) /* Invalid size */) {
  // Empty
}

}  // namespace plfsio
}  // namespace pdlfs
