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

#pragma once

#include "pdlfs-common/slice.h"
#include "pdlfs-common/xxhash.h"

#include <stddef.h>
#include <stdint.h>

namespace pdlfs {
namespace plfsio {

// Return true if a key may match a given filter.
typedef bool (*FilterTester)(const Slice& key, const Slice& input);

inline uint64_t BloomHash(const Slice& key) {
  // Less Hashing, Same Performance: Building a Better Bloom Filter (RSA '08)
  return xxhash64(key.data(), key.size(), 0);  // gi(x) = h1(x) + i*h2(x)
}

struct DirOptions;

// Return false iff the target key is guaranteed to not exist in a given bloom
// filter.
extern bool BloomKeyMayMatch(const Slice& key, const Slice& input);

// A simple bloom filter implementation
class BloomBlock {
 public:
  // Create a bloom filter block using a given set of options.
  // When creating the block, the caller also specifies the total amount of
  // memory to reserve for storing the underlying bitmap.
  // Insufficient memory reservation may cause dynamic memory allocation
  // at a later time.
  BloomBlock(const DirOptions& options, size_t bytes_to_reserve = 0);
  ~BloomBlock();

  // A bloom filter must be reset before keys may be inserted.
  // When resetting a bloom filter, the caller specifies the total number of
  // keys it will be inserting into the bloom filter. This allows the bloom
  // filter to decide how many bits to use for the underlying bitmap.
  // The underlying bitmap won't be re-sized before the next reset.
  void Reset(uint32_t num_keys);

  // Insert a key into the bloom filter.
  // REQUIRES: Reset(num_keys) has been called.
  // REQUIRES: Finish() has not been called.
  void AddKey(const Slice& key);

  // Finalize the filter and return its contents.
  Slice Finish();

  // Finalize the filter and return a copy of its contents.
  std::string TEST_Finish();

  // Return the underlying buffer space.
  size_t memory_usage() const { return space_.capacity(); }
  static int chunk_type();  // Return the corresponding chunk type
  size_t num_victims() const { return 0; }

 private:
  // No copying allowed
  void operator=(const BloomBlock&);
  BloomBlock(const BloomBlock&);
  const size_t bits_per_key_;  // Number of bits for each key

  bool finished_;  // If Finish() has been called
  std::string space_;
  // Size of the underlying bitmap in bits
  uint32_t bits_;
  // Number of hash functions
  uint32_t k_;
};

// Return true if the target key matches a given bitmap filter.
bool BitmapKeyMustMatch(const Slice& key, const Slice& input);

// Bitmap compression formats.
class UncompressedFormat;
class CompressedFormat;  // Parent class for all compressed formats
// A fast, bit-level scheme that compresses a group of deltas at a time
class FastPfDeltaFormat;
class PfDeltaFormat;
// A fast, bucketized bitmap representation
class FastRoaringFormat;
class RoaringFormat;
// A simple varint-based scheme
class FastVbPlusFormat;
class VbPlusFormat;
class VbFormat;

template <typename T>
int BitmapFormatFromType();  // Return the corresponding bitmap format.

// A simple filter backed by a bitmap.
template <typename T = UncompressedFormat>
class BitmapBlock {
 public:
  // Create a bitmap filter block using a given set of options.
  // When creating the block, the caller also specifies the total amount of
  // memory to reserve for storing the bitmap.
  // The bitmap may be stored in a compressed format.
  // Insufficient memory reservation may cause dynamic memory allocation
  // at a later time.
  BitmapBlock(const DirOptions& options, size_t bytes_to_reserve);
  ~BitmapBlock();

  // A bitmap filter must be reset before keys may be inserted.
  // When resetting a bitmap filter, the caller specifies the total number of
  // keys it will be inserting into the bitmap filter. This allows the bitmap
  // filter to estimate the density of its bit array and to prepare for
  // incoming keys.
  void Reset(uint32_t num_keys);

  // Insert a key into the bitmap filter.
  // REQUIRES: Reset(num_keys) has been called.
  // REQUIRES: Finish() has not been called.
  void AddKey(const Slice& key);

  // Finalize the block data and return its contents.
  Slice Finish();

  // Return the underlying buffer space.
  std::string* buffer_store() { return &space_; }

  // Report total filter memory usage
  size_t memory_usage() const;
  static int chunk_type();  // Return the corresponding LOG chunk type

 private:
  // No copying allowed
  void operator=(const BitmapBlock&);
  BitmapBlock(const BitmapBlock&);
  const size_t key_bits_;  // Key size in bits
  const int bm_fmt_;

  bool finished_;  // If Finish() has been called
  std::string space_;
  // Pre-computed mask for incoming keys
  uint32_t mask_;
  // Compression format
  T* fmt_;
};

// An empty filter that achieves nothing.
class EmptyFilterBlock {
 public:
  // Empty filter. Does not reserve memory.
  EmptyFilterBlock(const DirOptions& options, size_t bytes_to_reserve);
  ~EmptyFilterBlock() {}

  // Reset filter state.
  void Reset(uint32_t num_keys) {}
  // Insert a key into the filter. Does nothing.
  void AddKey(const Slice& key) {}
  // Finalize filter contents.
  Slice Finish() { return Slice(); }

  static int chunk_type();  // Return the corresponding chunk type
  // Report total filter memory usage
  size_t memory_usage() const { return space_.capacity(); }
  // Return the underlying buffer space.
  std::string* buffer_store() { return &space_; }

 private:
  std::string space_;
};

}  // namespace plfsio
}  // namespace pdlfs
