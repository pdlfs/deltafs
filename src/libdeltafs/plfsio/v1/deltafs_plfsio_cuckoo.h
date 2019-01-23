/*
 * Copyright (c) 2018-2019 Carnegie Mellon University and
 *         Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/coding.h"
#include "pdlfs-common/random.h"
#include "pdlfs-common/slice.h"
#include "pdlfs-common/xxhash.h"

#include <stddef.h>
#include <stdint.h>

#include <vector>

namespace pdlfs {
namespace plfsio {

template <size_t, size_t>
class CuckooTable;

inline uint64_t CuckooHash(const Slice& key) {
  return xxhash64(key.data(), key.size(), 0);
}

inline uint32_t CuckooFingerprint(uint64_t ha, size_t bits_per_key) {
  uint32_t h = static_cast<uint32_t>(ha >> 32);
  if (bits_per_key < 32) {
    h = h & ((1u << bits_per_key) - 1);
  }
  h += (h == 0);
  return h;
}

inline size_t CuckooAlt(size_t i, uint32_t fp) {  // MurmurHash2
  return i ^ (fp * 0x5bd1e995);
}

struct DirOptions;

// Return false iff the target key does not exist in the given filter.
extern bool CuckooKeyMayMatch(const Slice& key, const Slice& input);

// A simple cuckoo hash filter implementation.
template <size_t k = 16, size_t v = 16>
class CuckooBlock {
 public:
  CuckooBlock(const DirOptions& options, size_t bytes_to_reserve);
  ~CuckooBlock();

  void Reset(uint32_t num_keys);

  // Insert a key into the cuckoo filter.
  // REQUIRES: Reset(num_keys) has been called.
  // REQUIRES: Finish() has NOT been called.
  void AddKey(const Slice& key);

  // Insert a key into the cuckoo filter.
  // Return true if the insertion is success, or false otherwise.
  bool TEST_AddKey(const Slice& key);

  // Finalize the filter and return its contents.
  Slice Finish();

  // Finalize the filter and return a copy of its contents.
  std::string TEST_Finish();

  size_t num_victims()
      const;  // Number of keys not inserted into the cuckoo table
  size_t bytes_per_bucket() const;
  size_t num_buckets() const;

 private:
  bool full_;
  std::vector<uint32_t> key_starts;  // Starting offsets of all overflow keys
  std::string keys_;
  size_t victim_index_;  // There is only one victim at most
  uint32_t victim_fp_;
  int max_cuckoo_moves_;
  bool finished_;  // If Finish() has been called
  Random rnd_;

  void AddMore(const Slice& key);
  typedef CuckooTable<k, v> Rep;
  void operator=(const CuckooBlock&);  // No copying allowed
  CuckooBlock(const CuckooBlock&);
  Rep* rep_;
};

}  // namespace plfsio
}  // namespace pdlfs
