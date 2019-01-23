/*
 * Copyright (c) 2018-2019 Carnegie Mellon University and
 *         Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_cuckoo.h"
#include "deltafs_plfsio_types.h"

#include <math.h>

namespace pdlfs {
namespace plfsio {

template <size_t k = 16, size_t v = 16>
struct CuckooBucket {  // Fixed 4 items per bucket
  unsigned long long x0_ : k + v;
  unsigned long long x1_ : k + v;
  unsigned long long x2_ : k + v;
  unsigned long long x3_ : k + v;
};

template <size_t k = 16, size_t v = 16>
struct CuckooReader {
  explicit CuckooReader(const Slice& input) : input_(input) {}

  uint32_t Read(size_t i, size_t j) const {
    const CuckooBucket<k, v>* const b =
        reinterpret_cast<const CuckooBucket<k, v>*>(&input_[0]);
    if (j == 0) return b[i].x0_;
    if (j == 1) return b[i].x1_;
    if (j == 2) return b[i].x2_;
    if (j == 3) return b[i].x3_;
    return 0;
  }

  Slice input_;
};

template <size_t k = 16, size_t v = 16>
struct CuckooTable {
  explicit CuckooTable(const DirOptions& options)
      : num_buckets_(0), frac_(options.cuckoo_frac) {}

  static uint64_t UpperPower2(uint64_t x) {
    x--;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x |= x >> 32;
    x++;
    return x;
  }

  // REQUIRES: Resize() must have been CALLed.
  void Write(size_t i, size_t j, uint32_t x) {
    assert(i < num_buckets_ && j < 4);
    CuckooBucket<k, v>* const b =
        reinterpret_cast<CuckooBucket<k, v>*>(&space_[0]);
    assert(x != 0);
    if (j == 0) b[i].x0_ = x;
    if (j == 1) b[i].x1_ = x;
    if (j == 2) b[i].x2_ = x;
    if (j == 3) b[i].x3_ = x;
  }

  // REQUIRES: Resize() must have been CALLed.
  uint32_t Read(size_t i, size_t j) const {
    assert(i < num_buckets_ && j < 4);
    const CuckooBucket<k, v>* const b =
        reinterpret_cast<const CuckooBucket<k, v>*>(&space_[0]);
    if (j == 0) return b[i].x0_;
    if (j == 1) return b[i].x1_;
    if (j == 2) return b[i].x2_;
    if (j == 3) return b[i].x3_;
    return 0;
  }

  void Resize(uint32_t num_keys);
  // Total number of hash buckets, over-allocated by frac_
  size_t num_buckets_;  // Must be a power of 2
  std::string space_;
  // Target occupation rate
  double frac_;
};

template <size_t k, size_t v>
void CuckooTable<k, v>::Resize(uint32_t num_keys) {
  const static size_t bucket_sz = sizeof(CuckooBucket<k, v>);
  space_.resize(0);
  if (frac_ > 0)
    num_buckets_ = static_cast<size_t>(ceil(1.0 / frac_ * (num_keys + 3) / 4));
  else
    num_buckets_ = (num_keys + 3) / 4;
  if (num_buckets_ != 0)  // Always round up to a nearest power of 2
    num_buckets_ = UpperPower2(num_buckets_);
  else
    num_buckets_ = 1;
  space_.resize(num_buckets_ * bucket_sz, 0);
}

template <size_t k, size_t v>
CuckooBlock<k, v>::CuckooBlock(const DirOptions& options,
                               size_t bytes_to_reserve)
    : full_(false),
      victim_index_(0),
      victim_fp_(0),
      max_cuckoo_moves_(options.cuckoo_max_moves),
      finished_(true),  // Reset(num_keys) must be called before inserts
      rnd_(options.cuckoo_seed) {
  rep_ = new Rep(options);
  if (bytes_to_reserve != 0) {
    rep_->space_.reserve(bytes_to_reserve + 8);
  }
}

template <size_t k, size_t v>
CuckooBlock<k, v>::~CuckooBlock() {
  delete rep_;
}

template <size_t k, size_t v>
void CuckooBlock<k, v>::Reset(uint32_t num_keys) {
  rep_->Resize(num_keys);
  victim_index_ = 0;
  victim_fp_ = 0;
  finished_ = false;
  full_ = false;
}

template <size_t k, size_t v>
Slice CuckooBlock<k, v>::Finish() {
  assert(!finished_);
  finished_ = true;
  Rep* const r = rep_;
  PutFixed32(&r->space_, r->num_buckets_);
  PutFixed32(&r->space_, victim_index_);
  PutFixed32(&r->space_, victim_fp_);
  PutFixed32(&r->space_, k);
  return r->space_;
}

template <size_t k, size_t v>
std::string CuckooBlock<k, v>::TEST_Finish() {
  Rep* const r = rep_;
  Finish();
  return r->space_;
}

template <size_t k, size_t v>
void CuckooBlock<k, v>::AddMore(const Slice& key) {
  key_starts.push_back(static_cast<uint32_t>(keys_.size()));
  keys_.append(key.data(), key.size());
}

template <size_t k, size_t v>
bool CuckooBlock<k, v>::TEST_AddKey(const Slice& key) {
  assert(!finished_);
  if (full_) return false;
  AddKey(key);
  return true;
}

template <size_t k, size_t v>
void CuckooBlock<k, v>::AddKey(const Slice& key) {
  assert(!finished_);
  uint64_t ha = CuckooHash(key);
  uint32_t fp = CuckooFingerprint(ha, k);
  if (full_) {  // If filter is full, directly insert into the overflow space
    AddMore(key);
    return;
  }

  Rep* const r = rep_;
  size_t i = ha & (r->num_buckets_ - 1);
  // Our goal is to put fp into bucket i
  for (int count = 0; count < max_cuckoo_moves_; count++) {
    if (k < 32) assert((fp & (~((1u << k) - 1))) == 0);
    for (size_t j = 0; j < 4; j++) {
      uint32_t cur = r->Read(i, j);
      if (cur == fp) return;  // Done
      if (cur == 0) {
        r->Write(i, j, fp);
        return;
      }
    }
    if (count != 0) {  // Kick out a victim so we can put fp in
      size_t victim = rnd_.Next() & 3;
      uint32_t old = r->Read(i, victim);
      assert(old != 0 && old != fp);
      r->Write(i, victim, fp);
      fp = old;
    }

    i = CuckooAlt(i, fp) & (r->num_buckets_ - 1);
  }

  victim_index_ = i;
  victim_fp_ = fp;
  full_ = true;
}

template <size_t k, size_t v>
size_t CuckooBlock<k, v>::num_victims() const {
  return keys_.size();
}

template <size_t k, size_t v>
size_t CuckooBlock<k, v>::bytes_per_bucket() const {
  return static_cast<size_t>(sizeof(CuckooBucket<k, v>));
}

template <size_t k, size_t v>
size_t CuckooBlock<k, v>::num_buckets() const {
  return rep_->num_buckets_;
}

template <size_t k, size_t v>
class CuckooKeyTester {
 public:
  bool operator()(const Slice& key, const Slice& input) {
    const char* const tail = input.data() + input.size();
    if (input.size() < 16) return true;
#ifndef NDEBUG
    size_t bits = DecodeFixed32(tail - 4);
    assert(bits == k);
#endif
    const size_t num_bucket = DecodeFixed32(tail - 16);
    if (input.size() - 16 < sizeof(CuckooBucket<k, v>) * num_bucket ||
        num_bucket == 0) {
      return true;
    }

    uint64_t ha = CuckooHash(key);
    uint32_t fp = CuckooFingerprint(ha, k);
    size_t i1 = ha & (num_bucket - 1);
    size_t i2 = CuckooAlt(i1, fp) & (num_bucket - 1);

    uint32_t victim_fp = DecodeFixed32(tail - 8);
    if (victim_fp == fp) {
      uint32_t i = DecodeFixed32(tail - 12);
      if (i == i1 || i == i2) {
        return true;
      }
    }

    CuckooReader<k, v> reader(Slice(input.data(), input.size() - 16));
    for (size_t j = 0; j < 4; j++) {
      if (reader.Read(i1, j) == fp) {
        return true;
      } else if (reader.Read(i2, j) == fp) {
        return true;
      }
    }

    return false;
  }
};

template class CuckooBlock<32, 0>;
template class CuckooBlock<24, 0>;
template class CuckooBlock<20, 0>;
template class CuckooBlock<18, 0>;
template class CuckooBlock<16, 0>;
template class CuckooBlock<14, 0>;
template class CuckooBlock<12, 0>;
template class CuckooBlock<10, 0>;

bool CuckooKeyMayMatch(const Slice& key, const Slice& input) {
  const size_t len = input.size();
  if (len < 4) {
    return true;
  }

  const char* const tail = input.data() + input.size();
  size_t bits = DecodeFixed32(tail - 4);
  switch (int(bits)) {
#define CASE(n) \
  case n:       \
    return CuckooKeyTester<n, 0>()(key, input)
    CASE(32);
    CASE(24);
    CASE(20);
    CASE(18);
    CASE(16);
    CASE(14);
    CASE(12);
    CASE(10);
#undef CASE
    default:
      return true;
  }
}

}  // namespace plfsio
}  // namespace pdlfs
