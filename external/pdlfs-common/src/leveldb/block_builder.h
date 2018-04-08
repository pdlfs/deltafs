/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/slice.h"

#include <stdint.h>
#include <vector>

namespace pdlfs {

class Comparator;

class AbstractBlockBuilder {
 public:
  explicit AbstractBlockBuilder(const Comparator* cmp);

  // Add zero padding to the underlying buffer space.
  void Pad(size_t n);

  // Optionally, caller may specify a direct buffer space to switch with
  // the one currently used by the builder.
  // This allows the builder to append data to an external buffer space and
  // potentially avoids an extra copy of data.
  void ResetBuffer(std::string* buffer);

  // Reset block contents.
  void Reset();

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() or Finalize() is called.
  Slice Finish();

  // Reserve a certain amount of buffer space.
  void Reserve(size_t size) { buffer_.reserve(buffer_start_ + size); }

  // Finalize the block by putting a trailer that contains the type of the block
  // and the checksum of it. If "padding_target" is not 0, append "padding_char"
  // to the buffer until the block size reaches the target. Skip checksum if
  // crc32c is false. Return a slice that refers to the block contents and the
  // newly inserted block trailer. The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  // REQUIRES: Finish() has been called since the last Reset().
  Slice Finalize(bool crc32c = true, uint32_t padding_target = 0,
                 char padding_char = 0);

  // Return the comparator being used by the builder.
  const Comparator* comparator() const { return cmp_; }

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.size() <= buffer_start_; }

  // Return a pointer to the underlying buffer space
  std::string* buffer_store() { return &buffer_; }

 protected:
  const Comparator* cmp_;
  std::string buffer_;   // Destination buffer
  size_t buffer_start_;  // Start offset of the buffer space
  bool finished_;        // Has Finish() been called?

 private:
  // No copying allowed
  void operator=(const AbstractBlockBuilder&);
  AbstractBlockBuilder(const AbstractBlockBuilder&);
};

class BlockBuilder : public AbstractBlockBuilder {
 public:
  BlockBuilder(int restart_interval, const Comparator* cmp = NULL);

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Set a new restart interval.
  void ChangeRestartInterval(int interval) { restart_interval_ = interval; }

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() or Finalize() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

 private:
  int restart_interval_;
  std::vector<uint32_t> restarts_;  // Restart points
  int counter_;                     // Number of entries emitted since restart
  std::string last_key_;

  // No copying allowed
  void operator=(const BlockBuilder&);
  BlockBuilder(const BlockBuilder&);
};

}  // namespace pdlfs
