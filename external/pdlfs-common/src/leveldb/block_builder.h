/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <stdint.h>
#include <vector>

#include "pdlfs-common/slice.h"

namespace pdlfs {

class Comparator;

class BlockBuilder {
 public:
  BlockBuilder(int restart_interval, const Comparator* cmp = NULL);

  // Optionally, caller may specify a direct buffer space to switch with
  // the one currently used by the builder.
  // This allows the builder to append data to an external buffer space and
  // potentially avoids an extra copy of data.
  void SwitchBuffer(std::string* buffer);

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // Reserve a certain amount of buffer space.
  void Reserve(size_t size) { buffer_.reserve(buffer_start_ + size); }

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Set a new restart interval.
  void ChangeRestartInterval(int interval) { restart_interval_ = interval; }

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until either Reset() or
  // Finalize() is called
  Slice Finish();

  // Put trailer to the buffer end which contains the block type and checksum.
  // If "padding_target" is not 0, append zeros to the buffer
  // until data length reaches the target.
  // Return a slice that refers to the new raw block contents that includes
  // the trailer.  The returned slice will remain valid for the lifetime of
  // this builder or until Reset() is called.
  // REQUIRES: Finish() has been called since the last call to Reset().
  Slice Finalize(uint64_t padding_target = 0);

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Return the comparator being used by the builder.
  const Comparator* comparator() const { return cmp_; }

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.size() <= buffer_start_; }

 private:
  int restart_interval_;
  const Comparator* cmp_;
  std::string buffer_;              // Destination buffer
  size_t buffer_start_;             // Start offset of the buffer space
  std::vector<uint32_t> restarts_;  // Restart points
  int counter_;                     // Number of entries emitted since restart
  bool finished_;                   // Has Finish() been called?
  std::string last_key_;

  // No copying allowed
  void operator=(const BlockBuilder&);
  BlockBuilder(const BlockBuilder&);
};

}  // namespace pdlfs
