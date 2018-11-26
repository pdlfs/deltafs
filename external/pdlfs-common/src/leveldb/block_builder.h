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

#include "pdlfs-common/compression_type.h"
#include "pdlfs-common/slice.h"

#include <stdint.h>
#include <vector>

namespace pdlfs {

class Comparator;

class AbstractBlockBuilder {
 public:
  // Each builder is associated with a buffer for holding temporary block
  // contents before Finish() or Finalize() is called. After Finish() or
  // Finalize(), the builder can be reset and buffer reused for building a new
  // block.
  explicit AbstractBlockBuilder(const Comparator* cmp);

  // Append padding to the underlying buffer destination.
  // Padding must be inserted before block contents are inserted.
  // Added padding is not cleared by future Reset()s.
  void Pad(size_t n);

  // Specify a new buffer destination to switch with the one currently used by
  // the builder. This allows the builder to append data to an external buffer
  // destination and potentially avoids an extra copy of data.  If buffer is
  // NULL, the current buffer destination will be used so new block contents
  // will be appended after existing contents.
  // Must be called after Finish() and before the next Reset().
  // Existing contents in *buffer are not cleared by future Reset()s.
  void ResetBuffer(std::string* buffer);

  // Reset block contents.
  void Reset();

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() or Finalize() is called.
  Slice Finish(CompressionType compression = kNoCompression,
               bool force_compression = false);

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

  // Report memory usage
  size_t memory_usage() const { return buffer_.capacity(); }

  // Return a pointer to the underlying buffer space
  std::string* buffer_store() { return &buffer_; }

 protected:
  const Comparator* cmp_;  // NULL if keys are not inserted in-order
  std::string buffer_;     // Destination buffer
  size_t buffer_start_;    // Start offset of the buffer space
  CompressionType compression_;
  bool finished_;  // Has Finish() been called?

 private:
  // No copying allowed
  void operator=(const AbstractBlockBuilder&);
  AbstractBlockBuilder(const AbstractBlockBuilder&);
};

class BlockBuilder : public AbstractBlockBuilder {
 public:
  BlockBuilder(int restart_interval, const Comparator* cmp);
  explicit BlockBuilder(int restart_interval);

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
  Slice Finish(CompressionType compression = kNoCompression,
               bool force_compression = false);

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
