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
  const size_t bits_to_move = 64 - bits_per_key;
  uint32_t fp = static_cast<uint32_t>(ha >> bits_to_move);
  fp += (fp == 0);
  return fp;
}

inline size_t CuckooAlt(size_t i, uint32_t fp) {  // MurmurHash2
  return i ^ (fp * 0x5bd1e995);
}

struct DirOptions;

// Return false iff the target key is absent from the given filter.
extern bool CuckooKeyMayMatch(const Slice& key, const Slice& input);

// A simple cuckoo hash filter implementation.
template <size_t k = 16, size_t v = 0>
class CuckooBlock {
 public:
  CuckooBlock(const DirOptions& options, size_t bytes_to_reserve);
  ~CuckooBlock();

  void Reset(uint32_t num_keys);

  // Insert a key into the cuckoo filter. Key is first inserted into the
  // main table. If the main table is full, key is inserted into
  // one or more auxiliary tables.
  // REQUIRES: Reset(num_keys) has been called.
  // REQUIRES: Finish() has NOT been called.
  void AddKey(const Slice& key, uint32_t value = 0);

  // Insert a key into the cuckoo filter. Key is only inserted to the main
  // table. The auxiliary table is ignored.
  // Return true if the insertion is success, or false otherwise.
  // REQUIRES: Reset(num_keys) has been called.
  // REQUIRES: Finish() has NOT been called.
  bool TEST_AddKey(const Slice& key, uint32_t value = 0);

  // Finalize the filter and return its contents.
  Slice Finish();

  // Finalize the filter and return a copy of its contents.
  std::string TEST_Finish();

  size_t num_victims() const;  // #keys not inserted to the main table

  size_t TEST_BytesPerCuckooBucket() const;
  size_t TEST_NumCuckooTables() const;
  size_t TEST_NumBuckets() const;

 private:
  std::vector<uint32_t> key_sizes_;  // The size of each overflow key
  std::vector<uint32_t> values_;
  std::string keys_;
  const int max_cuckoo_moves_;
  bool finished_;  // If Finish() has been called
  Random rnd_;

  void MaybeBuildMoreTables();
  void AddMore(const Slice& key, uint32_t value);
  typedef CuckooTable<k, v> Rep;
  void operator=(const CuckooBlock& cuckoo);  // No copying allowed
  CuckooBlock(const CuckooBlock&);
  void AddTo(uint64_t ha, uint32_t fp, uint32_t data, Rep* rep);
  bool Exists(uint64_t ha, uint32_t fp, const Rep* rep);
  std::vector<Rep*> morereps_;  // Auxiliary tables
  // The main table
  Rep* rep_;
};

}  // namespace plfsio
}  // namespace pdlfs
