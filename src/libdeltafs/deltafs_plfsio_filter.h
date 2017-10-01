/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/hash.h"
#include "pdlfs-common/slice.h"

#include <stddef.h>
#include <stdint.h>

namespace pdlfs {

namespace plfsio {

inline uint32_t BloomHash(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);  // Magic
}

// Return false iff the target key is guaranteed to not exist in a given bloom
// filter.
extern bool BloomKeyMayMatch(const Slice& key, const Slice& input);

// A simple bloom filter implementation
class BloomBlock {
 public:
  // Create a bloom filter block and set the number of bits to allocate
  // for each incoming key. This allows the bloom filter to decide how many hash
  // functions it should use on each inserted key to set the bits.
  // When creating a bloom filter block, the caller also specifies the total
  // amount of memory to reserve for the underlying bitmap.
  BloomBlock(size_t bits_per_key, size_t max_bytes);
  ~BloomBlock();

  // A bloom filter must be reset before keys may be inserted.
  // When resetting a bloom filter, the caller specifies the total number of
  // keys it will be inserting into the bloom filter. This allows the bloom
  // filter to decide how many bits to allocate for the underlying bitmap.
  // Note that the underlying bitmap won't be resized before the next reset.
  void Reset(uint32_t num_keys);

  // Insert a key into the bloom filter.
  // REQUIRES: Reset(num_keys) has been called.
  // REQUIRES: Finish() has not been called.
  void AddKey(const Slice& key);

  // Finalize the block data and return its contents.
  Slice Finish();

  // Return the underlying buffer space.
  std::string* buffer_store() { return &space_; }

 private:
  // No copying allowed
  void operator=(const BloomBlock&);
  BloomBlock(const BloomBlock&);
  const size_t bits_per_key_;  // Number of bits for each key
  const size_t max_bytes_;     // Max filter size in bytes

  bool finished_;  // If Finish() has been called
  std::string space_;
  // Size of the underlying bitmap
  uint32_t bits_;
  // Number of hash functions
  uint32_t k_;
};

}  // namespace plfsio
}  // namespace pdlfs
