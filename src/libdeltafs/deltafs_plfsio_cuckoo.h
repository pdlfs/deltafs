/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/coding.h"
#include "pdlfs-common/hash.h"
#include "pdlfs-common/random.h"
#include "pdlfs-common/slice.h"

#include <stddef.h>
#include <stdint.h>

#include <set>

namespace pdlfs {
namespace plfsio {

template <size_t>
class CuckooTable;

inline uint32_t CuckooHash(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}

inline uint32_t CuckooFingerprint(const Slice& key, size_t bits_per_key) {
  uint32_t h = Hash(key.data(), key.size(), 301);
  if (bits_per_key < 32) {
    h = h & ((1u << bits_per_key) - 1);
  }
  h += (h == 0);
  return h;
}

inline size_t CuckooAlt(size_t i, uint32_t f) {  // MurmurHash2
  return i ^ (f * 0x5bd1e995);
}

struct DirOptions;

// Return false iff the target key does not exist in the given filter.
extern bool CuckooKeyMayMatch(const Slice& key, const Slice& input);

// A simple cuckoo hash filter implementation.
template <size_t bits_per_key>
class CuckooBlock {
 public:
  CuckooBlock(const DirOptions& options, size_t bytes_to_reserve);
  ~CuckooBlock();

  void Reset(uint32_t num_keys);

  // Insert a key into the cuckoo filter.
  // REQUIRES: Reset(num_keys) has been called.
  // REQUIRES: Finish() has not been called.
  void AddKey(const Slice& key);

  // Finalize the block data and return its contents.
  Slice Finish();

  size_t num_victims() const { return victims_.size(); }
  size_t bytes_per_bucket() const;
  size_t num_buckets() const;

 private:
  std::set<uint32_t> victims_;
  int max_cuckoo_moves_;
  bool finished_;  // If Finish() has been called
  Random rnd_;

  typedef CuckooTable<bits_per_key> Rep;
  void operator=(const CuckooBlock&);  // No copying allowed
  CuckooBlock(const CuckooBlock&);
  Rep* rep_;
};

}  // namespace plfsio
}  // namespace pdlfs
