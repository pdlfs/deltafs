/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_format.h"

namespace pdlfs {
namespace plfsio {

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
