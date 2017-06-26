/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "block_builder.h"
#include "format.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/crc32c.h"

#include <assert.h>
#include <algorithm>

// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.
namespace pdlfs {

BlockBuilder::BlockBuilder(int restart_interval, const Comparator* cmp)
    : restart_interval_(restart_interval),
      cmp_(cmp),
      buffer_start_(0),
      counter_(0),
      finished_(false) {
  restarts_.push_back(0);  // First restart point is at offset 0
  if (restart_interval_ < 1) {
    restart_interval_ = 1;
  }
  if (cmp_ == NULL) {
    cmp_ = BytewiseComparator();
  }
}

void BlockBuilder::SwitchBuffer(std::string* buffer) {
  if (buffer != NULL && buffer != &buffer_) buffer->swap(buffer_);
  buffer_start_ = buffer_.size();
}

void BlockBuilder::Pad(size_t n) {
  buffer_.resize(buffer_.size() + n, 0);
  buffer_start_ = buffer_.size();
}

void BlockBuilder::Reset() {
  buffer_.resize(buffer_start_);
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  size_t result = buffer_.size() - buffer_start_;
  if (!finished_) {
    // Plus restart array contents and its length
    return result + restarts_.size() * sizeof(uint32_t) + sizeof(uint32_t);
  } else {
    return result;
  }
}

Slice BlockBuilder::Finish() {
  assert(!finished_);
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, restarts_.size());  // Remember the array size
  finished_ = true;
  Slice result = buffer_;
  result.remove_prefix(buffer_start_);
  return result;
}

Slice BlockBuilder::Finalize(bool crc32c, uint32_t padding_target) {
  assert(finished_);
  Slice contents = buffer_;  // Contents without the trailer and padding
  contents.remove_prefix(buffer_start_);
  char trailer[kBlockTrailerSize];
  trailer[0] = kNoCompression;
  if (crc32c) {
    uint32_t crc = crc32c::Value(contents.data(), contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
  } else {
    EncodeFixed32(trailer + 1, 0);
  }
  buffer_.append(trailer, sizeof(trailer));
  if (padding_target != 0 && buffer_.size() < buffer_start_ + padding_target) {
    buffer_.resize(buffer_start_ + padding_target, 0);
  }
  Slice result = buffer_;
  result.remove_prefix(buffer_start_);
  return result;
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= restart_interval_);
  assert(empty() || cmp_->Compare(key, last_key_piece) >= 0);
  size_t shared = 0;
  if (counter_ < restart_interval_) {
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // Restart compression
    const ssize_t offset = buffer_.size() - buffer_start_;
    assert(offset > 0);
    restarts_.push_back(static_cast<uint32_t>(offset));
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;
  const size_t vlen = value.size();

  // Add "<shared><non_shared><value_size>" to buffer_
  PutVarint32(&buffer_, static_cast<uint32_t>(shared));
  PutVarint32(&buffer_, static_cast<uint32_t>(non_shared));
  PutVarint32(&buffer_, static_cast<uint32_t>(vlen));

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // Update state
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  counter_++;
}

}  // namespace pdlfs
