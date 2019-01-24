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
#if k + v > 32
  unsigned long long x0_ : k + v;
  unsigned long long x1_ : k + v;
  unsigned long long x2_ : k + v;
  unsigned long long x3_ : k + v;
#else
  unsigned x0_ : k + v;
  unsigned x1_ : k + v;
  unsigned x2_ : k + v;
  unsigned x3_ : k + v;
#endif
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
  explicit CuckooTable(double frac)
      : frac_(frac),
        num_buckets_(0),  // Use Resize(num_keys) to allocate space
        victim_index_(0),
        victim_fp_(0),
        full_(false) {}

  static inline uint64_t UpperPower2(uint64_t x) {
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
    Resize(num_keys);  // Allocate buckets
    victim_index_ = 0;
    victim_fp_ = 0;
    full_ = false;
  }

  void Write(size_t i, size_t j, uint32_t x) {
    assert(!full_);
    assert(x != 0);
    CuckooBucket<k, v>* const b =
        reinterpret_cast<CuckooBucket<k, v>*>(&space_[0]);
    assert(i < num_buckets_);
    assert(j < 4);
    if (j == 0) b[i].x0_ = x;
    if (j == 1) b[i].x1_ = x;
    if (j == 2) b[i].x2_ = x;
    if (j == 3) b[i].x3_ = x;
  }

  uint32_t Read(size_t i, size_t j) const {
    const CuckooBucket<k, v>* const b =
        reinterpret_cast<const CuckooBucket<k, v>*>(&space_[0]);
    assert(i < num_buckets_);
    assert(j < 4);
    if (j == 0) return b[i].x0_;
    if (j == 1) return b[i].x1_;
    if (j == 2) return b[i].x2_;
    if (j == 3) return b[i].x3_;
    return 0;
  }

  const double frac_;  // Target table occupation rate, or -1 for exact match
  void Resize(uint32_t num_keys);
  std::string space_;
  // Total number of hash buckets, over-allocated by frac_
  size_t num_buckets_;  // Must be a power of 2
  size_t victim_index_;
  uint32_t victim_fp_;
  bool full_;
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
    : max_cuckoo_moves_(options.cuckoo_max_moves),
      finished_(true),  // Reset(num_keys) must be called before inserts
      rnd_(options.cuckoo_seed),
      rep_(NULL) {
  rep_ = new Rep(options.cuckoo_frac);
  if (bytes_to_reserve != 0) {
    rep_->space_.reserve(bytes_to_reserve);
  }
}

template <size_t k, size_t v>
CuckooBlock<k, v>::~CuckooBlock() {
  size_t i = 0;
  for (; i < morereps_.size(); i++) {
    delete morereps_[i];
  }
  delete rep_;
}

template <size_t k, size_t v>
void CuckooBlock<k, v>::Reset(uint32_t num_keys) {
  size_t i = 0;
  for (; i < morereps_.size(); i++) {
    delete morereps_[i];
  }
  morereps_.resize(0);
  rep_->Reset(num_keys);
  key_sizes.resize(0);
  keys_.resize(0);
  finished_ = false;
}

template <size_t k, size_t v>
void CuckooBlock<k, v>::MaybeBuildMoreTables() {
  uint32_t limit = static_cast<uint32_t>(key_sizes.size());
  const char* start = &keys_[0];

  uint32_t i = 0;
  while (i != limit) {
    Rep* r = new Rep(0.95);  // This is likely going to accommodate all keys
    r->Resize(limit - i);

    for (; i < limit; i++) {
      uint64_t ha = CuckooHash(Slice(start, key_sizes[i]));
      uint32_t fp = CuckooFingerprint(ha, k);
      start += key_sizes[i];

      AddTo(ha, fp, r);
      if (r->full_) {
        break;
      }
    }

    PutFixed32(&r->space_, r->num_buckets_);
    PutFixed32(&r->space_, r->victim_index_);
    PutFixed32(&r->space_, r->victim_fp_);

    morereps_.push_back(r);
  }
}

template <size_t k, size_t v>
Slice CuckooBlock<k, v>::Finish() {
  assert(!finished_);
  finished_ = true;

  Rep* const r = rep_;
  PutFixed32(&r->space_, r->num_buckets_);
  PutFixed32(&r->space_, r->victim_index_);
  PutFixed32(&r->space_, r->victim_fp_);
  MaybeBuildMoreTables();
  size_t i = 0;
  for (; i < morereps_.size(); i++) {
    r->space_.append(morereps_[i]->space_);
  }

  PutFixed32(&r->space_, 1 + morereps_.size());  // Remember #tables
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
  key_sizes.push_back(static_cast<uint32_t>(key.size()));
  keys_.append(key.data(), key.size());
}

template <size_t k, size_t v>
bool CuckooBlock<k, v>::TEST_AddKey(const Slice& key) {
  assert(!finished_);
  if (rep_->full_) return false;
  AddKey(key);
  return true;
}

template <size_t k, size_t v>
void CuckooBlock<k, v>::AddKey(const Slice& key) {
  assert(!finished_);
  uint64_t ha = CuckooHash(key);
  uint32_t fp = CuckooFingerprint(ha, k);
  // If the main table is full, stage the key at an overflow space
  if (rep_->full_) {
    AddMore(key);
    return;
  }

  AddTo(ha, fp, rep_);
}

template <size_t k, size_t v>
void CuckooBlock<k, v>::AddTo(uint64_t ha, uint32_t fp, Rep* r) {
  assert(!r->full_);

  size_t i = ha & (r->num_buckets_ - 1);
  // Our goal is to put fp into bucket i
  for (int count = 0; count < max_cuckoo_moves_; count++) {
    if (k < 32) assert((fp & (~((1u << k) - 1))) == 0);
    for (size_t j = 0; j < 4; j++) {
      uint32_t cur = r->Read(i, j);
      if (cur == fp) return;  // Done
      if (cur == 0) {
        r->Write(i, j, fp);
        return;  // Done
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

  r->victim_index_ = i;
  r->victim_fp_ = fp;
  r->full_ = true;
}

template <size_t k, size_t v>
size_t CuckooBlock<k, v>::num_victims() const {
  return keys_.size();
}

template <size_t k, size_t v>
size_t CuckooBlock<k, v>::TEST_BytesPerCuckooBucket() const {
  return static_cast<size_t>(sizeof(CuckooBucket<k, v>));
}

template <size_t k, size_t v>
size_t CuckooBlock<k, v>::TEST_NumCuckooTables() const {
  return 1 + morereps_.size();
}

template <size_t k, size_t v>
size_t CuckooBlock<k, v>::TEST_NumBuckets() const {
  size_t sum = 0, i = 0;
  for (; i < morereps_.size(); i++) {
    sum += morereps_[i]->num_buckets_;
  }
  sum += rep_->num_buckets_;
  return sum;
}

template <size_t k, size_t v>
class CuckooKeyTester {
 public:
  bool operator()(const Slice& key, const Slice& input) {
    const char* tail = input.data() + input.size();
    if (input.size() < 8) return true;  // Filter invalid, considered a match
#ifndef NDEBUG
    size_t bits = DecodeFixed32(tail - 4);
    assert(bits == k);
#endif
    uint32_t num_tables = DecodeFixed32(tail - 8);
    if (num_tables == 0) {  // No cuckoo tables found !!!
      return true;
    }

    uint64_t ha = CuckooHash(key);
    uint32_t fp = CuckooFingerprint(ha, k);

    size_t remaining_size = input.size();
    size_t table_sz = 8;
    for (; num_tables != 0; num_tables--) {
      assert(remaining_size >= table_sz);
      remaining_size -= table_sz;
      if (remaining_size < 12) {  // Cannot read the next table
        return true;
      }

      tail -= table_sz;
      uint32_t num_buckets = DecodeFixed32(tail - 12);
      table_sz = num_buckets * sizeof(CuckooBucket<k, v>) + 12;
      if (num_buckets == 0) {  // No buckets found
        return true;
      } else if (remaining_size < table_sz) {  // Cannot read table
        return true;
      }

      Slice cuckoo_table(tail - table_sz, table_sz);
      if (Test(ha, fp, num_buckets, cuckoo_table)) {
        return true;
      }
    }

    // All tables consulted
    return false;
  }

 private:
  int Test(uint64_t ha, uint32_t fp, uint32_t num_buckets, const Slice& input) {
    const char* const tail = input.data() + input.size();
    assert(input.size() >= 12);

    assert(num_buckets != 0);
    size_t i1 = ha & (num_buckets - 1);
    size_t i2 = CuckooAlt(i1, fp) & (num_buckets - 1);

    uint32_t victim_fp = DecodeFixed32(tail - 4);
    if (victim_fp == fp) {
      uint32_t i = DecodeFixed32(tail - 8);
      if (i == i1 || i == i2) {
        return true;
      }
    }

    CuckooReader<k, v> reader(Slice(input.data(), input.size() - 12));
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
