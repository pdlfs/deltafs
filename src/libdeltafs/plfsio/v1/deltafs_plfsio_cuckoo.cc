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
#include <map>

namespace pdlfs {
namespace plfsio {

template <size_t k = 16, size_t v = 16>
struct CuckooBucket {  // Fixed 4 items per bucket
  uint64_t x0_ : k + v;
  uint64_t x1_ : k + v;
  uint64_t x2_ : k + v;
  uint64_t x3_ : k + v;
};

template <size_t k = 16, size_t v = 16>
struct CuckooReader {
  explicit CuckooReader(const Slice& input)
      : b_(reinterpret_cast<const CuckooBucket<k, v>*>(&input[0])),
        num_buckets_(input.size() / sizeof(b_[0])) {}

  uint64_t Read(size_t i, size_t j) const {
    assert(i < num_buckets_);
    assert(j < 4);
    if (j == 0) return b_[i].x0_;
    if (j == 1) return b_[i].x1_;
    if (j == 2) return b_[i].x2_;
    if (j == 3) return b_[i].x3_;
    return 0;
  }

  std::pair<uint32_t, uint32_t> pair(size_t i, size_t j) const {
    const uint64_t x = Read(i, j);
    if (v != 0)
      return std::pair<uint32_t, uint32_t>(x >> v, x & ((1ull << v) - 1));
    else
      return std::pair<uint32_t, uint32_t>(x, 0);
  }

  uint32_t key(size_t i, size_t j) const {
    const uint64_t x = Read(i, j);
    if (v != 0)
      return x >> v;
    else
      return x;
  };

  const CuckooBucket<k, v>* const b_;
  const uint32_t num_buckets_;
};

template <size_t k = 16, size_t v = 16>
struct CuckooTable {
  explicit CuckooTable(double frac) : frac_(frac) { Reset(0); }

  void Reset(uint32_t num_keys) {
    Resize(num_keys);  // Make room for buckets
    victim_index_ = 0;
    victim_data_ = 0;
    victim_fp_ = 0;
    full_ = false;
  }

  void Write(size_t i, size_t j, uint32_t fp, uint32_t data) {
    assert(!full_);
    assert(fp != 0);
    CuckooBucket<k, v>* const b =
        reinterpret_cast<CuckooBucket<k, v>*>(&space_[0]);
    assert(i < num_buckets_);
    assert(j < 4);
    uint64_t x = fp;
    if (v != 0)  // Fuse kv into a single composite value
      x = (x << v) | (data & ((1ull << v) - 1));
    if (j == 0) b[i].x0_ = x;
    if (j == 1) b[i].x1_ = x;
    if (j == 2) b[i].x2_ = x;
    if (j == 3) b[i].x3_ = x;
  }

  uint64_t Read(size_t i, size_t j) const {
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

  std::pair<uint32_t, uint32_t> pair(size_t i, size_t j) const {
    const uint64_t x = Read(i, j);
    if (v != 0)
      return std::pair<uint32_t, uint32_t>(x >> v, x & ((1ull << v) - 1));
    else
      return std::pair<uint32_t, uint32_t>(x, 0);
  }

  uint32_t key(size_t i, size_t j) const {
    const uint64_t x = Read(i, j);
    if (v != 0)
      return x >> v;
    else
      return x;
  };

  const double frac_;  // Target table occupation rate, or -1 for exact match
  void Resize(uint32_t num_keys);
  std::string space_;
  // Total number of hash buckets, over-allocated by frac_
  size_t num_buckets_;  // Must be a power of 2
  size_t victim_index_;
  uint32_t victim_data_;
  uint32_t victim_fp_;
  bool full_;
};

namespace {
inline uint64_t UpperPower2(uint64_t x) {
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
}  // namespace

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
  key_sizes_.resize(0);
  values_.resize(0);
  keys_.resize(0);
  finished_ = false;
}

template <size_t k, size_t v>
void CuckooBlock<k, v>::MaybeBuildMoreTables() {
  const uint32_t limit = static_cast<uint32_t>(key_sizes_.size());
  const char* start = &keys_[0];

  uint32_t i = 0;
  while (i != limit) {
    Rep* r = new Rep(0.95);  // This is likely going to accommodate all keys
    r->Resize(limit - i);

    for (; i < limit; i++) {
      uint64_t ha = CuckooHash(Slice(start, key_sizes_[i]));
      uint32_t fp = CuckooFingerprint(ha, k);
      start += key_sizes_[i];

      if (v != 0) {  // Skip values when v is disabled
        AddTo(ha, fp, values_[i], r);
      } else {
        AddTo(ha, fp, 0, r);
      }
      if (r->full_) {
        break;
      }
    }

    PutFixed32(&r->space_, r->num_buckets_);
    PutFixed32(&r->space_, r->victim_index_);
    PutFixed32(&r->space_, r->victim_data_);
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
  PutFixed32(&r->space_, r->victim_data_);
  PutFixed32(&r->space_, r->victim_fp_);
  MaybeBuildMoreTables();
  size_t i = 0;
  for (; i < morereps_.size(); i++) {
    r->space_.append(morereps_[i]->space_);
  }

  PutFixed32(&r->space_, 1 + morereps_.size());  // Remember #Tables
  PutFixed32(&r->space_, v);
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
void CuckooBlock<k, v>::AddMore(const Slice& key, uint32_t value) {
  key_sizes_.push_back(static_cast<uint32_t>(key.size()));
  keys_.append(key.data(), key.size());
  if (v != 0) {  // Ignore data when v is disabled
    values_.push_back(value);
  }
}

template <size_t k, size_t v>
bool CuckooBlock<k, v>::TEST_AddKey(const Slice& key, uint32_t value) {
  assert(!finished_);
  if (rep_->full_) return false;
  AddKey(key, value);
  return true;
}

template <size_t k, size_t v>
void CuckooBlock<k, v>::AddKey(const Slice& key, uint32_t value) {
  assert(!finished_);
  uint64_t ha = CuckooHash(key);
  uint32_t fp = CuckooFingerprint(ha, k);
  // If the main table is full, stage the key at an overflow space
  if (rep_->full_) {
    AddMore(key, value);
    return;
  }

  AddTo(ha, fp, value, rep_);
}

template <size_t k, size_t v>
bool CuckooBlock<k, v>::Exists(uint64_t ha, uint32_t fp, const Rep* r) {
  assert(r->num_buckets_ != 0);
  size_t i1 = ha & (r->num_buckets_ - 1);
  size_t i2 = CuckooAlt(i1, fp) & (r->num_buckets_ - 1);
  assert(fp != 0);

  if (r->full_) {
    if (r->victim_fp_ == fp) {
      size_t i = r->victim_index_;
      if (i == i1 || i == i2) {
        return true;
      }
    }
  }

  for (size_t j = 0; j < 4; j++) {
    if (r->key(i1, j) == fp || r->key(i2, j) == fp) {
      return true;
    }
  }

  return false;
}

template <size_t k, size_t v>
void CuckooBlock<k, v>::AddTo(uint64_t ha, uint32_t fp, uint32_t data, Rep* r) {
  assert(!r->full_);
  size_t i = ha & (r->num_buckets_ - 1);

  // Our goal is to put "fp" into bucket "i"
  for (int moves = 0; moves < max_cuckoo_moves_; moves++) {
    for (size_t j = 0; j < 4; j++) {
      std::pair<uint32_t, uint32_t> kv = r->pair(i, j);
      if (kv.first == 0) {  // Direct insert if cell is empty
        r->Write(i, j, fp, data);
        return;
      } else if (kv.first == fp) {  // Fingerprint matches the input
        // If v is disabled we are done
        if (v == 0) {
          return;
        }
        // Otherwise we are done only if data
        // happens to match as well
        if (kv.second == (data & ((1ull << v) - 1))) {
          return;
        }
      }
    }
    if (moves != 0) {  // Kick out a victim so we can put "fp" in
      const size_t victim = rnd_.Next() & 3;
      std::pair<uint32_t, uint32_t> kv = r->pair(i, victim);
#ifndef NDEBUG
      if (v != 0)
        assert(kv.first != 0 && !(kv.first == fp && kv.second == data));
      else
        assert(kv.first != 0 && !(kv.first == fp));
#endif
      r->Write(i, victim, fp, data);
      data = kv.second;
      fp = kv.first;
    }

    i = CuckooAlt(i, fp) & (r->num_buckets_ - 1);
  }

  r->full_ = true;  // Full, no more inserts
  r->victim_index_ = i;
  r->victim_data_ = data;
  r->victim_fp_ = fp;
}

template <size_t k, size_t v>
size_t CuckooBlock<k, v>::num_victims() const {
  return key_sizes_.size();
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
  bool operator()(const Slice& key, const Slice& input,
                  std::vector<uint32_t>* values) {
    const char* tail = input.data() + input.size();
    if (input.size() < 12) return true;  // Not enough data for a header
#ifndef NDEBUG
    size_t valbits = DecodeFixed32(tail - 8);
    assert(valbits == v);
    size_t keybits = DecodeFixed32(tail - 4);
    assert(keybits == k);
#endif
    uint32_t num_tables = DecodeFixed32(tail - 12);
    if (num_tables == 0) {  // No tables found
      return true;
    }

    uint64_t ha = CuckooHash(key);
    const uint32_t fp = CuckooFingerprint(ha, k);

    size_t remaining_size = input.size();
    size_t table_size = 12;  // The 12-byte header to be removed
    for (; num_tables != 0; num_tables--) {
      assert(remaining_size >= table_size);
      remaining_size -= table_size;
      if (remaining_size < 16) {  // Not enough data for a table header
        return true;
      }

      tail -= table_size;
      uint32_t num_buckets = DecodeFixed32(tail - 16);
      table_size = num_buckets * sizeof(CuckooBucket<k, v>) + 16;
      if (num_buckets == 0) {  // An empty table has no use
        return true;
      } else if (remaining_size < table_size) {  // Premature end of table
        return true;
      }

      Slice cuckoo_table(tail - table_size, table_size);
      if (Fetch(ha, fp, num_buckets, cuckoo_table, values)) {
        if (v == 0 || !values) {
          return true;
        }
      }
    }

    if (v != 0 && values) {  // All tables consulted
      return !values->empty();
    } else {
      return false;
    }
  }

 private:
  static bool Fetch(uint64_t ha, uint32_t fp, uint32_t num_buckets,
                    const Slice& input, std::vector<uint32_t>* values) {
    const char* const tail = input.data() + input.size();
    assert(input.size() >= 16);

    assert(num_buckets != 0);
    size_t i1 = ha & (num_buckets - 1);
    size_t i2 = CuckooAlt(i1, fp) & (num_buckets - 1);

    uint32_t victim_fp = DecodeFixed32(tail - 4);
    if (victim_fp == fp) {
      uint32_t i = DecodeFixed32(tail - 12);
      if (i == i1 || i == i2) {
        if (v != 0 && values) {
          values->push_back(DecodeFixed32(tail - 8));
        } else {  // Immediately return
          return true;
        }
      }
    }

    Slice cuckoo_buckets = input;
    cuckoo_buckets.remove_suffix(16);  // Remove the 16-byte header
    const CuckooReader<k, v> reader(cuckoo_buckets);
    for (size_t j = 0; j < 4; j++) {
      if (v != 0 && values) {  // Test all locations to gather all values
        std::pair<uint32_t, uint32_t> kv1 = reader.pair(i1, j);
        if (kv1.first == fp) {
          values->push_back(kv1.second);
        }
        std::pair<uint32_t, uint32_t> kv2 = reader.pair(i2, j);
        if (kv2.first == fp) {
          values->push_back(kv2.second);
        }
      } else {  // Immediately return on first match
        if (reader.key(i1, j) == fp) {
          return true;
        } else if (reader.key(i2, j) == fp) {
          return true;
        }
      }
    }

    if (v != 0 && values) {
      return !values->empty();
    } else {
      return false;
    }
  }
};

#define TEMPLATE(K)                  \
  template class CuckooBlock<K, 32>; \
  template class CuckooBlock<K, 0>

TEMPLATE(32);
TEMPLATE(30);
TEMPLATE(24);
TEMPLATE(22);
TEMPLATE(20);
TEMPLATE(18);
TEMPLATE(16);
TEMPLATE(14);
TEMPLATE(12);
TEMPLATE(10);

bool CuckooKeyMayMatch(const Slice& key, const Slice& input) {
  return CuckooValues(key, input, NULL);  // Test key existence only
}

bool CuckooValues(const Slice& key, const Slice& input,
                  std::vector<uint32_t>* values) {
  const size_t len = input.size();
  if (len < 8) {  // Not enough data for a header, consider it to be a match
    return true;
  }

  const char* const tail = input.data() + input.size();
  size_t valbits = DecodeFixed32(tail - 8);
  size_t keybits = DecodeFixed32(tail - 4);
  if (valbits == 0) {
    switch (int(keybits)) {
#define CASE(n) \
  case n:       \
    return CuckooKeyTester<n, 0>()(key, input, values)
      CASE(32);
      CASE(30);
      CASE(24);
      CASE(22);
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
  } else if (valbits == 32) {
    switch (int(keybits)) {
#define CASE(n) \
  case n:       \
    return CuckooKeyTester<n, 32>()(key, input, values)
      CASE(32);
      CASE(30);
      CASE(24);
      CASE(22);
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
  } else {
    return true;
  }
}

}  // namespace plfsio
}  // namespace pdlfs
