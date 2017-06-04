/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
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
    return Status::Corruption("Bad epoch key");
  } else {
    *epoch = static_cast<unsigned>(parsed_epoch);
    *table = static_cast<unsigned>(parsed_table);
    return Status::OK();
  }
}

std::string EpochKey(uint32_t epoch, uint32_t table) {
  assert(epoch <= kMaxEpoches);
  assert(table <= kMaxTablesPerEpoch);
  char tmp[10];
  snprintf(tmp, sizeof(tmp), "%04d-%04d", int(epoch), int(table));
  return tmp;
}

void TableHandle::EncodeTo(std::string* dst) const {
  assert(filter_offset_ != ~static_cast<uint64_t>(0));
  assert(filter_size_ != ~static_cast<uint64_t>(0));
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  assert(!smallest_key_.empty());
  assert(!largest_key_.empty());

  PutLengthPrefixedSlice(dst, smallest_key_);
  PutLengthPrefixedSlice(dst, largest_key_);
  PutVarint64(dst, filter_offset_);
  PutVarint64(dst, filter_size_);
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status TableHandle::DecodeFrom(Slice* input) {
  Slice smallest_key;
  Slice largest_key;
  if (!GetLengthPrefixedSlice(input, &smallest_key) ||
      !GetLengthPrefixedSlice(input, &largest_key) ||
      !GetVarint64(input, &filter_offset_) ||
      !GetVarint64(input, &filter_size_) || !GetVarint64(input, &offset_) ||
      !GetVarint64(input, &size_)) {
    return Status::Corruption("Bad table handle");
  } else {
    smallest_key_ = smallest_key.ToString();
    largest_key_ = largest_key.ToString();
    return Status::OK();
  }
}

void Footer::EncodeTo(std::string* dst) const {
  assert(num_epoches_ != ~static_cast<uint32_t>(0));
  assert(lg_parts_ != ~static_cast<uint32_t>(0));
  assert(skip_checksums_ != ~static_cast<unsigned char>(0));
  assert(unique_keys_ != ~static_cast<unsigned char>(0));

  epoch_index_handle_.EncodeTo(dst);
  dst->resize(BlockHandle::kMaxEncodedLength, 0);
  PutFixed32(dst, num_epoches_);
  PutFixed32(dst, lg_parts_);
  dst->push_back(static_cast<char>(skip_checksums_));
  dst->push_back(static_cast<char>(unique_keys_));
}

Status Footer::DecodeFrom(Slice* input) {
  const char* start = input->data();
  size_t size = input->size();
  if (size >= kEncodeLength) {
    num_epoches_ = DecodeFixed32(start + kEncodeLength - 10);
    lg_parts_ = DecodeFixed32(start + kEncodeLength - 6);
    skip_checksums_ = static_cast<unsigned char>(start[kEncodeLength - 2]);
    unique_keys_ = static_cast<unsigned char>(start[kEncodeLength - 1]);
  } else {
    return Status::Corruption("Bad footer");
  }

  Status result = epoch_index_handle_.DecodeFrom(input);
  if (result.ok()) {
    Slice source(start, size);
    source.remove_prefix(kEncodeLength);
    // This skips over any leftover data
    *input = source;
  }
  return result;
}

}  // namespace plfsio
}  // namespace pdlfs
