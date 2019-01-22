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
  unsigned x0_ : k;
  //  unsigned y0_ : v;
  unsigned x1_ : k;
  //  unsigned y1_ : v;
  unsigned x2_ : k;
  //  unsigned y2_ : v;
  unsigned x3_ : k;
  //  unsigned y3_ : v;
};

template <size_t k = 16, size_t v = 16>
struct CuckooReader {
  explicit CuckooReader(const Slice& input) : input_(input) {}

  uint32_t Read(size_t i, size_t j) const {
    const CuckooBucket<k, v>* const b =
        reinterpret_cast<const CuckooBucket<k, v>*>(input_.data());
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

  void Reset(uint32_t num_keys) {
    space_.resize(0);
#if 0
    num_buckets_ =
        static_cast<size_t>(ceil(1.0 / frac_ * (num_keys + 3) / 4));
#else
    num_buckets_ = (num_keys + 3) / 4;
#endif
    num_buckets_ = UpperPower2(num_buckets_);
    space_.resize(num_buckets_ * sizeof(CuckooBucket<k, v>), 0);
  }

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

  // Total number of hash buckets, over-allocated by frac_
  size_t num_buckets_;  // Must be a power of 2
  std::string space_;
  // Target occupation rate
  double frac_;
};

template <size_t k, size_t v>
CuckooBlock<k, v>::CuckooBlock(const DirOptions& options,
                               size_t bytes_to_reserve)
    : max_cuckoo_moves_(options.cuckoo_max_moves),
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
  rep_->Reset(num_keys);
  finished_ = false;
}

template <size_t k, size_t v>
Slice CuckooBlock<k, v>::Finish() {
  assert(!finished_);
  finished_ = true;
  Rep* const r = rep_;
  const uint32_t n = static_cast<uint32_t>(r->num_buckets_);
  PutFixed32(&r->space_, n);
  PutFixed32(&r->space_, k);
  return r->space_;
}

template <size_t k, size_t v>
void CuckooBlock<k, v>::AddKey(const Slice& key) {
  uint64_t ha = CuckooHash(key);
  uint32_t fp = CuckooFingerprint(ha, k);

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

  victims_.insert(fp);
}

template <size_t k, size_t v>
size_t CuckooBlock<k, v>::num_victims() const {
  return victims_.size();
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
#ifndef NDEBUG
    assert(input.size() >= 8);
    size_t bits = DecodeFixed32(tail - 4);
    assert(bits == k);
#endif
    size_t num_bucket = DecodeFixed32(tail - 8);
    uint64_t ha = CuckooHash(key);
    uint32_t fp = CuckooFingerprint(ha, k);

    CuckooReader<k, v> reader(Slice(input.data(), input.size() - 8));
    size_t i1 = ha & (num_bucket - 1);
    size_t i2 = CuckooAlt(i1, fp) & (num_bucket - 1);

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

template class CuckooBlock<32, 32>;
template class CuckooBlock<24, 24>;
template class CuckooBlock<20, 20>;
template class CuckooBlock<16, 16>;
template class CuckooBlock<12, 12>;

bool CuckooKeyMayMatch(const Slice& key, const Slice& input) {
  const size_t len = input.size();
  if (len < 8) {
    return true;
  }

  const char* tail = input.data() + input.size();
  size_t bits = DecodeFixed32(tail - 4);
  switch (int(bits)) {
#define CASE(n) \
  case n:       \
    return CuckooKeyTester<n, n>()(key, input)
    CASE(32);
    CASE(24);
    CASE(20);
    CASE(16);
    CASE(12);
#undef CASE
    default:
      return true;
  }
}

}  // namespace plfsio
}  // namespace pdlfs
