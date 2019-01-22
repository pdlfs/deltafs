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

#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {
namespace plfsio {

class CuckooTest {
 public:
  CuckooTest() {
    cf_ = new CF(options_, 0);  // Do not reserve memory
  }

  ~CuckooTest() {
    if (cf_ != NULL) {
      delete cf_;
    }
  }

  static uint32_t KeyFringerprint(uint64_t ha) {
    return CuckooFingerprint(ha, kBitsPerKey);
  }

  static uint64_t KeyHash(uint32_t k) {
    char tmp[4];
    EncodeFixed32(tmp, k);
    return CuckooHash(Slice(tmp, sizeof(tmp)));
  }

  bool KeyMayMatch(uint32_t k) {
    char tmp[4];
    EncodeFixed32(tmp, k);
    return CuckooKeyMayMatch(Slice(tmp, sizeof(tmp)), data_);
  }

  void AddKey(uint32_t k) {
    char tmp[4];
    EncodeFixed32(tmp, k);
    cf_->AddKey(Slice(tmp, sizeof(tmp)));
  }

  void Finish() { data_ = cf_->Finish().ToString(); }
  void Reset(uint32_t num_keys) { cf_->Reset(num_keys); }
  enum { kBitsPerKey = 16 };
  typedef CuckooBlock<kBitsPerKey> CF;
  DirOptions options_;
  std::string data_;  // Final filter data
  CF* cf_;
};

TEST(CuckooTest, BytesPerBucket) {
  fprintf(stderr, "%d\n", int(cf_->bytes_per_bucket()));
}

TEST(CuckooTest, AltIndex) {
  for (uint32_t ki = 1; ki <= 1024; ki *= 2) {
    uint32_t num_keys = ki << 10;
    fprintf(stderr, "%4u Ki keys\n", ki);
    Reset(num_keys);
    size_t num_buckets = cf_->num_buckets();
    uint32_t k = 0;
    for (; k < num_keys; k++) {
      uint64_t hash = KeyHash(k);
      uint32_t fp = KeyFringerprint(hash);
      size_t i1 = hash % num_buckets;
      size_t i2 = CuckooAlt(i1, fp) % num_buckets;
      size_t i3 = CuckooAlt(i2, fp) % num_buckets;
      ASSERT_TRUE(i1 == i3);
    }
  }
}

TEST(CuckooTest, Empty) {
  for (uint32_t ki = 1; ki <= 1024; ki *= 2) {
    uint32_t num_keys = ki << 10;
    fprintf(stderr, "%4u Ki keys\n", ki);
    Reset(num_keys);
    Finish();
    uint32_t k = 0;
    for (; k < num_keys; k++) {
      ASSERT_FALSE(KeyMayMatch(k));
    }
  }
}

TEST(CuckooTest, CF) {
  for (uint32_t ki = 1; ki <= 1024; ki *= 2) {
    uint32_t num_keys = ki << 10;
    fprintf(stderr, "%4u Ki keys: ", ki);
    Reset(num_keys);
    uint32_t k = 0;
    for (; k < num_keys; k++) {
      AddKey(k);
    }
    Finish();
    size_t v = cf_->num_victims();
    fprintf(stderr, "%d victim keys (%.2f%%)\n", int(v), 100.0 * v / num_keys);
    k = 0;
    for (; k < num_keys; k++) {
      // ASSERT_TRUE(KeyMayMatch(k));
    }
  }
}

}  // namespace plfsio
}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return pdlfs::test::RunAllTests(&argc, &argv);
}
