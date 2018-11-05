/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
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

  static uint32_t KeyFringerprint(uint32_t k) {
    char tmp[4];
    EncodeFixed32(tmp, k);
    return CuckooFingerprint(Slice(tmp, sizeof(tmp)), kBitsPerKey);
  }

  static uint32_t KeyHash(uint32_t k) {
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

  int BytesPerBucket() const { return int(cf_->bytes_per_bucket()); }
  size_t NumBuckets() const { return cf_->num_buckets(); }
  void Finish() { data_ = cf_->Finish().ToString(); }
  void Reset(uint32_t num_keys) { cf_->Reset(num_keys); }
  static const int kBitsPerKey = 10;
  typedef CuckooBlock<kBitsPerKey> CF;
  DirOptions options_;
  std::string data_;  // Final filter data
  CF* cf_;
};

TEST(CuckooTest, BytesPerBucket) {
  fprintf(stderr, "Bytes per bucket: %d\n", BytesPerBucket());
}

TEST(CuckooTest, NumBuckets) {
  for (uint32_t num_keys = (1u << 10); num_keys <= (1u << 20); num_keys *= 2) {
    Reset(num_keys);
    if (NumBuckets() >= 1024) {
      fprintf(stderr, "%4u Ki keys: %3u Ki buckets\n", (num_keys >> 10),
              unsigned(NumBuckets() >> 10));
    } else {
      fprintf(stderr, "%4u Ki keys: %3u buckets\n", (num_keys >> 10),
              unsigned(NumBuckets()));
    }
  }
}

TEST(CuckooTest, AltIndex) {
  for (uint32_t num_keys = (1u << 10); num_keys <= (1u << 20); num_keys *= 2) {
    Reset(num_keys);
    uint32_t k = 0;
    for (; k < num_keys; k++) {
      uint32_t fp = KeyFringerprint(k);
      uint32_t hash = KeyHash(k);
      size_t i1 = hash % NumBuckets();
      size_t i2 = CuckooAlt(i1, fp) % NumBuckets();
      size_t i3 = CuckooAlt(i2, fp) % NumBuckets();
      ASSERT_TRUE(i1 == i3);
    }
    fprintf(stderr, "%4u Ki keys: OK\n", (num_keys >> 10));
  }
}

TEST(CuckooTest, Empty) {
  for (uint32_t num_keys = (1u << 10); num_keys <= (1u << 20); num_keys *= 2) {
    Reset(num_keys);
    Finish();
    uint32_t k = 0;
    for (; k < num_keys; k++) {
      ASSERT_FALSE(KeyMayMatch(k));
    }
    fprintf(stderr, "%4u Ki keys: OK\n", (num_keys >> 10));
  }
}

TEST(CuckooTest, CF) {
  for (uint32_t num_keys = (1u << 10); num_keys <= (1u << 20); num_keys *= 2) {
    Reset(num_keys);
    uint32_t k = 0;
    for (; k < num_keys; k++) {
      AddKey(k);
    }
    Finish();
    k = 0;
    for (; k < num_keys; k++) {
      ASSERT_TRUE(KeyMayMatch(k));
    }
    fprintf(stderr, "%4u Ki keys: OK\n", (num_keys >> 10));
  }
}

}  // namespace plfsio
}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return pdlfs::test::RunAllTests(&argc, &argv);
}
