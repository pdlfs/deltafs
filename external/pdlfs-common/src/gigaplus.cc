/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <math.h>
#include <string.h>
#include <algorithm>

#include "pdlfs-common/coding.h"
#include "pdlfs-common/gigaplus.h"
#include "xxhash.h"

namespace pdlfs {

DirIndexOptions::DirIndexOptions() : paranoid_checks(false) {}

// Largest bitmao radix.
static const int kMaxRadix = 16;
// Max number of partitions.
static const size_t kMaxPartitions = 1 << kMaxRadix;

// -------------------------------------------------------------
// Internal Helper Methods
// -------------------------------------------------------------
/* clang-format off */

static inline int ToLog2(int value) {
  return static_cast<int>(floor(log2(value)));
}

// Bit constants for fast bitwise calculation.
static unsigned char kBits[] = {
  1 << 0, 1 << 1, 1 << 2, 1 << 3, 1 << 4, 1 << 5, 1 << 6, 1 << 7
};

// Reverse the bits in a byte with 4 operations.
static inline unsigned char Reverse(unsigned char byte) {
  return ((byte * 0x80200802ULL) & 0x0884422110ULL) * 0x0101010101ULL >> 32;
}

/* clang-format on */

// The number of bits necessary to hold the given index.
//
// ------------------------
//   sample input/output
// ------------------------
//   0           -->  0
//   1           -->  1
//   2,3         -->  2
//   4,5,6,7     -->  3
//   128,129,255 -->  8
// ------------------------
static inline int ToRadix(int index) {
  assert(index >= 0 && index < kMaxPartitions);
  return index == 0 ? 0 : 1 + ToLog2(index);
}

// Return the child index for a given index at a given radix.
//
// ------------------------
//   sample input/output
// ------------------------
//   i=0,   r=0 -->  1
//   i=1,   r=1 -->  3
//   i=3,   r=2 -->  7
//   i=7,   r=3 -->  15
//   i=127, r=7 -->  255
// ------------------------
static inline int ToChildIndex(int index, int radix) {
  assert(index >= 0 && index < kMaxPartitions / 2);
  assert(radix >= 0 && radix < kMaxRadix);
  return index + (1 << radix);
}

// Deduce the parent index from a specified child index.
//
// ------------------------
//   sample input/output
// ------------------------
//   0,1,2,4,128 -->  0
//   3,5,9,129   -->  1
//   6,10,130    -->  2
//   7,11,131    -->  3
//   255         -->  127
// ------------------------
static inline int ToParentIndex(int index) {
  assert(index >= 0 && index < kMaxPartitions);
  return index == 0 ? 0 : index - (1 << ToLog2(index));
}

// Convert a variable-length string to a 8-byte hash.
// Current implementation employs XXHash.
// Alternatively, we could also use MurmurHash or CityHash.
static inline void GIGAHash(const Slice& buffer, char* result) {
  uint64_t h = XXH64(buffer.data(), buffer.size(), 0) - 17241709254077376921LLU;
  memcpy(result, &h, 8);
}

// Use the first "n" bits from the hash to compute the index using
// the following calculation.
//
// |<---------------  hash  --------------->|
// [ - 1st  byte - ][ - 2nd  byte - ][] .. []  << hash (input)
// |<------- n bits -------->|
//
// [ - 2nd  byte - ][ - 1st  byte - ]
// [x x x x 4 3 2 1][8 7 6 5 4 3 2 1] << index (output)
//         |<------- n bits ------->|
//
// REQUIRES: *hash must contain at least n bits.
static int ComputeIndexFromHash(const char* hash, int n) {
  int result = 0;

  assert(n >= 0);
  assert(n <= kMaxRadix);

  size_t idx;
  size_t nbytes = n / 8;
  for (idx = 0; idx < nbytes; idx++) {
    result += Reverse(hash[idx]) << (idx * 8);
  }
  size_t nbits = n % 8;
  if (nbits > 0) {
    result += (Reverse(hash[idx]) & ((1 << nbits) - 1)) << (idx * 8);
  }

  assert(result >= 0 && result < kMaxPartitions);
  return result;
}

// ====================
// GIGA+ Implementation
// ====================

// The header for each directory index has the form
//     zeroth_server: uint16_t
//     radix: uint16_t
static const size_t kHeadSize = 4;

// Read-only view to an existing directory index.
struct DirIndex::View {
  uint16_t zeroth_server() const { return DecodeFixed16(rep_); }
  uint16_t radix() const { return DecodeFixed16(rep_ + 2); }
  size_t bitmap_size() const { return bitmap_.size(); }

  bool bit(size_t index) const {
    assert(index < kMaxPartitions);
    if (index == 0 && empty()) {
      return true;
    } else if (index < bitmap_size() * 8) {
      return 0 != (bitmap_[index / 8] & kBits[index % 8]);
    } else {
      return false;
    }
  }

  unsigned char byte(size_t index) const {
    assert(index < kMaxPartitions / 8);
    if (index == 0 && empty()) {
      return kBits[0];
    } else if (index < bitmap_size()) {
      return bitmap_[index];
    } else {
      return 0;
    }
  }

  int HighestBit() const {
    if (empty()) {
      return 0;
    } else {
      assert(bit(0));
      size_t i = bitmap_size() - 1;
      size_t off = 7;
      for (; bitmap_[i] == 0; --i)
        ;
      for (; (bitmap_[i] & kBits[off]) == 0; --off)
        ;
      return off + (i * 8);
    }
  }

 private:
  bool empty() const { return bitmap_.empty(); }

  friend class DirIndex;
  const char* rep_;
  Slice bitmap_;
};

bool DirIndex::ParseDirIndex(const Slice& input, bool paranoid_checks,
                             View* view) {
  if (input.size() < kHeadSize) {
    return false;
  } else {
    view->rep_ = input.data();
    int r = view->radix();
    size_t bitmap_size = input.size() - kHeadSize;
    view->bitmap_ = Slice(view->rep_ + kHeadSize, bitmap_size);
    if (bitmap_size < ((1 << r) + 7) / 8) {
      return false;
    } else if (!view->bit(0)) {
      return false;
    } else if (paranoid_checks && r != ToRadix(view->HighestBit())) {
      return false;
    }
    return true;
  }
}

struct DirIndex::Rep {
  Rep(uint16_t zeroth_server);

  void Reserve(size_t size) { ScaleToSize(size); }

  size_t bitmap_size() const { return ((1 << radix()) + 7) / 8; }

  Slice ToSlice() const {
    assert(bitmap_size() <= bitmap_capacity_);
    return Slice(rep_, kHeadSize + bitmap_size());
  }

  virtual ~Rep() {
    if (rep_ != static_buf_) {
      delete[] rep_;
    }
  }

  bool bit(size_t index) const {
    assert(index < kMaxPartitions);
    if (index < bitmap_size() * 8) {
      return 0 != (bitmap_[index / 8] & kBits[index % 8]);
    } else {
      return false;
    }
  }

  unsigned char byte(size_t index) const {
    assert(index < kMaxPartitions / 8);
    if (index < bitmap_size()) {
      return bitmap_[index];
    } else {
      return 0;
    }
  }

  int HighestBit() const {
    size_t i = bitmap_size() - 1;
    size_t off = 7;
    assert(i < bitmap_capacity_);
    assert(bit(0));
    for (; bitmap_[i] == 0; --i)
      ;
    for (; (bitmap_[i] & kBits[off]) == 0; --off)
      ;
    return off + (i * 8);
  }

  void TurnOffBit(size_t index) {
    assert(index != 0 && index < kMaxPartitions);
    if (index < bitmap_size() * 8) {
      size_t i = index / 8;
      size_t off = index % 8;
      assert(i < bitmap_capacity_);
      // Won't try to shrink memory when bits are turned off.
      bitmap_[i] &= (~kBits[off]);
      // Update radix if necessary
      if (radix() == ToRadix(index)) {
        SetRadix(ToRadix(HighestBit()));
      }
    } else {
      // Do nothing
    }
  }

  void TurnOnBit(size_t index) {
    assert(index < kMaxPartitions);
    if (index >= bitmap_capacity_ * 8) {
      assert(index < 2 * bitmap_capacity_ * 8);
      ScaleUp(2);
    }
    int r = ToRadix(index);
    if (radix() < r) {
      assert(radix() == r - 1);
      SetRadix(r);
    }

    size_t i = index / 8;
    size_t off = index % 8;
    assert(i < bitmap_capacity_);
    bitmap_[i] |= kBits[off];
    assert(radix() == ToRadix(HighestBit()));
  }

  template <class T>
  void DoMerge(const T& other) {
    assert(zeroth_server() == other.zeroth_server());
    size_t new_capacity = std::max(bitmap_size(), other.bitmap_size());
    ScaleToSize(new_capacity);
    SetRadix(std::max(radix(), other.radix()));
    assert(new_capacity >= bitmap_size());
    for (size_t i = 0; i < new_capacity; i++) {
      bitmap_[i] |= other.byte(i);
    }
  }

  void Merge(const DirIndex::View& other) {
    DoMerge(other);
    assert(radix() == ToRadix(HighestBit()));
  }

  void Merge(const Rep& other) {
    DoMerge(other);
    assert(radix() == ToRadix(HighestBit()));
  }

  uint16_t zeroth_server() const { return DecodeFixed16(rep_); }
  uint16_t radix() const { return DecodeFixed16(rep_ + 2); }

 private:
  enum { kInitBitmapCapacity = (1 << 7) / 8 };

  char* rep_;
  char* bitmap_;
  size_t bitmap_capacity_;

  // Avoid allocating space for small indices.
  char static_buf_[kHeadSize + kInitBitmapCapacity];

  // Reset in-mem rep.
  void Reset(char* rep, size_t bitmap_capacity) {
    char* old = rep_;
    rep_ = rep;
    bitmap_ = rep + kHeadSize;
    bitmap_capacity_ = bitmap_capacity;
    assert(bitmap_capacity_ <= kMaxPartitions / 8);

    if (old != static_buf_) {
      delete[] old;
    }
  }

  // Expand in-memory space to accommodate more bits
  void ScaleUp(int factor) {
    if (factor > 1) {
      size_t old_size = kHeadSize + bitmap_capacity_;
      size_t new_size = kHeadSize + bitmap_capacity_ * factor;
      char* buf = new char[new_size];
      memcpy(buf, rep_, old_size);
      memset(buf + old_size, 0, new_size - old_size);
      Reset(buf, new_size - kHeadSize);
    }
  }

  // Ensure bitmap has at least the specified capacity.
  void ScaleToSize(size_t capacity) {
    int factor = 1;
    size_t current_capacity = bitmap_capacity_;
    while (current_capacity < capacity) {
      current_capacity <<= 1;
      factor <<= 1;
    }
    ScaleUp(factor);
  }

  void SetZerothServer(uint16_t server_id) { EncodeFixed16(rep_, server_id); }
  void SetRadix(uint16_t radix) { EncodeFixed16(rep_ + 2, radix); }

  // No copying allowed
  void operator=(const Rep&);
  Rep(const Rep&);
};

DirIndex::Rep::Rep(uint16_t zeroth_server) {
  memset(static_buf_, 0, sizeof(static_buf_));
  rep_ = NULL;
  Reset(static_buf_, sizeof(static_buf_) - kHeadSize);
  SetZerothServer(zeroth_server);
  SetRadix(0);
  TurnOnBit(0);
}

bool DirIndex::IsSet(int index) const {
  assert(rep_ != NULL);
  return rep_->bit(index);
}

Slice DirIndex::Encode() const {
  assert(rep_ != NULL);
  return rep_->ToSlice();
}

int DirIndex::ZerothServer() const {
  assert(rep_ != NULL);
  return rep_->zeroth_server();
}

int DirIndex::Radix() const {
  assert(rep_ != NULL);
  return rep_->radix();
}

// Exchange the contents of two DirIndex objects.
void DirIndex::Swap(DirIndex& other) {
  assert(options_ == other.options_);
  Rep* my_rep = rep_;
  rep_ = other.rep_;
  other.rep_ = my_rep;
}

// Update the directory index by merging another directory index
// for the same directory.
bool DirIndex::Update(const Slice& other) {
  if (rep_ == NULL) {
    return TEST_Reset(other);
  } else {
    View view;
    bool checks = options_->paranoid_checks;
    if (!ParseDirIndex(other, checks, &view)) {
      return false;
    } else if (rep_->zeroth_server() != view.zeroth_server()) {
      return false;
    } else {
      rep_->Merge(view);
      return true;
    }
  }
}

// Update the directory index by merging another directory index
// for the same directory.
void DirIndex::Update(const DirIndex& other) {
  const Rep& other_rep = *other.rep_;
  if (rep_ == NULL) {
    Rep* new_rep = new Rep(other_rep.zeroth_server());
    new_rep->Merge(other_rep);
    rep_ = new_rep;
  } else {
    rep_->Merge(other_rep);
  }
}

// Reset index states.
bool DirIndex::TEST_Reset(const Slice& other) {
  View view;
  bool checks = options_->paranoid_checks;
  if (!ParseDirIndex(other, checks, &view)) {
    return false;
  } else {
    Rep* new_rep = new Rep(view.zeroth_server());
    new_rep->Merge(view);
    delete rep_;
    rep_ = new_rep;
    return true;
  }
}

void DirIndex::Set(int index) {
  assert(rep_ != NULL);
  assert(index >= 0 && index < options_->num_virtual_servers);
  rep_->TurnOnBit(index);
}

void DirIndex::SetAll() {
  assert(rep_ != NULL);
  rep_->Reserve(options_->num_virtual_servers);
  for (int i = 0; i < options_->num_virtual_servers; ++i) {
    rep_->TurnOnBit(i);
  }
}

void DirIndex::TEST_Unset(int index) {
  assert(rep_ != NULL);
  assert(index > 0 && index < options_->num_virtual_servers);
  rep_->TurnOffBit(index);
}

void DirIndex::TEST_RevertAll() {
  assert(rep_ != NULL);
  for (int i = rep_->HighestBit(); i > 0; --i) {
    rep_->TurnOffBit(i);
  }
}

// Return true if the partition marked by the specified index
// can be further divided to generate a child partition.
// This depends a lot on the current status of the directory mapping.
// Note that non-existing partitions are always non-splittable.
//
// Current Implementation does not consider the actual number of
// servers. Only the constant max number of virtual servers are considered.
// This, however, makes it flexible enough to facilitate virtual servers.
bool DirIndex::IsSplittable(int index) const {
  assert(rep_ != NULL);
  if (!rep_->bit(index)) {
    return false;
  } else {
    int i = index;
    int r = ToRadix(i);
    while (r < kMaxRadix) {
      i = ToChildIndex(index, r);
      if (!rep_->bit(i)) {
        return (i < options_->num_virtual_servers);
      }
      r++;
      assert(r == ToRadix(i));
    }
    return false;
  }
}

// Return the next available child index for a given parent index.
// The parent index must mark an existing partition and must
// be splittable in the first place.
int DirIndex::NewIndexForSplitting(int index) const {
  assert(rep_ != NULL);
  assert(IsSplittable(index));
  int i = index;
  int r = ToRadix(index);
  while (rep_->bit(i)) {
    i = ToChildIndex(index, r++);
  }
  assert(i != index && i < options_->num_virtual_servers);
  return i;
}

// Determine the partition responsible for the given name from the
// current state of the directory index.
int DirIndex::GetIndex(const Slice& name) const {
  char tmp[8];
  Slice hash = DirIndex::Hash(name, tmp);
  return HashToIndex(hash);
}

// Determine the partition responsible for the given hash from the
// current state of the directory index.
int DirIndex::HashToIndex(const Slice& hash) const {
  assert(rep_ != NULL);
  assert(rep_->bit(0));
  int i = ComputeIndexFromHash(hash.data(), rep_->radix());
  assert(i < options_->num_virtual_servers);
  while (!rep_->bit(i)) {
    i = ToParentIndex(i);
  }
  return i;
}

// Pickup a server to take care of the given name.
int DirIndex::SelectServer(const Slice& name) const {
  return GetServerForIndex(GetIndex(name));
}

// Pickup a server to take care of the give hash.
int DirIndex::HashToServer(const Slice& hash) const {
  return GetServerForIndex(HashToIndex(hash));
}

// Return true if a file represented by the specified hash will be
// migrated to the given child partition once its parent partition splits.
// The given index marks this child partition. It is easy to deduce the
// parent partition from a child partition.
bool DirIndex::ToBeMigrated(int index, const char* hash) {
  return ComputeIndexFromHash(hash, ToRadix(index)) == index;
}

// Insert the corresponding hash value into *dst.
void DirIndex::PutHash(std::string* dst, const Slice& name) {
  char tmp[8];
  GIGAHash(name, tmp);
  dst->append(tmp, 8);
}

// Calculate the hash for a given string.
Slice DirIndex::Hash(const Slice& name, char* scratch) {
  GIGAHash(name, scratch);
  return Slice(scratch, 8);
}

// Return the server responsible for a specific partition.
int DirIndex::GetServerForIndex(int index) const {
  assert(rep_ != NULL);
  return MapIndexToServer(index, rep_->zeroth_server(), options_->num_servers);
}

// Return the server responsible for a specific partition.
int DirIndex::MapIndexToServer(int index, int zeroth_server, int num_servers) {
  return (index + zeroth_server) % num_servers;
}

DirIndex::DirIndex(int zserver, const DirIndexOptions* options) {
  rep_ = new Rep(zserver);
  options_ = options;
}

DirIndex::DirIndex(const DirIndexOptions* options) {
  rep_ = NULL;
  options_ = options;
}

DirIndex::~DirIndex() { delete rep_; }

// Return a random server for a specified directory.
int DirIndex::RandomServer(const Slice& dir, int seed) {
  return XXH32(dir.data(), dir.size(), seed);
}

// Return a pair of random servers for a specified directory.
std::pair<int, int> DirIndex::RandomServers(const Slice& dir, int seed) {
  uint64_t h = XXH64(dir.data(), dir.size(), seed);
  char* tmp = reinterpret_cast<char*>(&h);
  int s1;
  int s2;
  memcpy(&s1, tmp, 4);
  memcpy(&s2, tmp + 4, 4);
  std::pair<int, int> r = std::make_pair(s1, s2);
  return r;
}

}  // namespace pdlfs
