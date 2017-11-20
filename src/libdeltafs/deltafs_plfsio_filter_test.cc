/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_filter.h"
#include "deltafs_plfsio.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

#include <set>
#include <string>

namespace pdlfs {
namespace plfsio {

template <typename T, FilterTester tester, size_t key_bits = 24>
class FilterTest {
 public:
  FilterTest() : key_bits_(key_bits), ft_(NULL) {
    options_.bf_bits_per_key = 10;  // Override the defaults
    options_.bm_key_bits = key_bits_;
  }

  ~FilterTest() {
    delete ft_;  // Done
  }

  void Reset(uint32_t num_keys) {
    if (ft_ == NULL) ft_ = new T(options_, 0);  // Does not reserve memory
    ft_->Reset(num_keys);
  }

  // REQUIRES: Reset() must have been called.
  void AddKey(uint32_t seq) {
    std::string key;
    PutFixed32(&key, seq);
    ft_->AddKey(key);
  }

  Slice Finish() {
    data_.clear();
    if (ft_ != NULL) {
      data_ = ft_->Finish().ToString();
      delete ft_;
      ft_ = NULL;
    }
    return data_;
  }

  // REQUIRES: Finish() must have been called.
  bool KeyMayMatch(uint32_t seq) const {
    std::string key;
    PutFixed32(&key, seq);
    return tester(key, data_);
  }

  std::string data_;  // Final filter contents
  const size_t key_bits_;
  DirOptions options_;
  T* ft_;
};

template <typename T>
static void TEST_LogAndApply(T* t, Random* rnd, uint32_t num_keys,
                             bool no_fp = true) {
  t->Reset(num_keys);
  const int key_bits = t->key_bits_;  // Obtain key space
  std::set<uint32_t> keys;
  while (keys.size() != num_keys) {
    keys.insert(rnd->Uniform(1 << key_bits));
  }
  std::set<uint32_t>::iterator it = keys.begin();
  for (; it != keys.end(); ++it) {
    t->AddKey(*it);
  }
  Slice contents = t->Finish();
  fprintf(stderr, "%8u keys (%f%% full) %27s]\t%12.2f bits/key\n", num_keys,
          100.0 * double(num_keys) / (1u << key_bits),
          PrettySize(contents.size()).c_str(),
          8.0 * double(contents.size()) / num_keys);
  // All keys previously inserted must match
  for (it = keys.begin(); it != keys.end(); ++it) {
    ASSERT_TRUE(t->KeyMayMatch(*it));
  }

  // If no_fp is true (no false-positive), all non-existent
  // keys must never match (filter is accurate)
  if (no_fp) {
    std::set<uint32_t> non_keys;  // Randomly generate non-keys
    while (non_keys.size() != keys.size()) {
      uint32_t key = rnd->Uniform(1 << key_bits);
      if (keys.count(key) == 0) {
        non_keys.insert(key);
      }
    }
    for (it = non_keys.begin(); it != non_keys.end(); ++it) {
      ASSERT_FALSE(t->KeyMayMatch(*it));
    }

    // Test keys not in the defined key space
    for (uint32_t i = 0; i < num_keys; i++) {
      ASSERT_FALSE(t->KeyMayMatch((1u << key_bits) + i));
    }
  }
}

// Bloom filter
typedef FilterTest<BloomBlock, BloomKeyMayMatch> BloomFilterTest;

TEST(BloomFilterTest, BloomFmt) {
  Random rnd(301);
  uint32_t num_keys = 0;
  while (num_keys <= (64 << 10)) {
    TEST_LogAndApply(this, &rnd, num_keys, false);
    if (num_keys == 0) {
      num_keys = 1;
    } else {
      num_keys *= 4;
    }
  }
}

// Uncompressed bitmap filter
typedef FilterTest<BitmapBlock<UncompressedFormat>, BitmapKeyMustMatch>
    UncompressedBitmapFilterTest;

TEST(UncompressedBitmapFilterTest, UncompressedBitmapFmt) {
  Random rnd(301);
  uint32_t num_keys = 0;
  while (num_keys <= (16 << 10)) {
    TEST_LogAndApply(this, &rnd, num_keys);
    if (num_keys == 0) {
      num_keys = 1;
    } else {
      num_keys *= 4;
    }
  }
}

// Varint bitmap filter
typedef FilterTest<BitmapBlock<VarintFormat>, BitmapKeyMustMatch>
    VarintBitmapFilterTest;

TEST(VarintBitmapFilterTest, VarintBitmapFmt) {
  Random rnd(301);
  uint32_t num_keys = 0;
  while (num_keys <= (4 << 10)) {
    TEST_LogAndApply(this, &rnd, num_keys);
    if (num_keys == 0) {
      num_keys = 1;
    } else {
      num_keys *= 4;
    }
  }
}

// Varint plus bitmap filter
typedef FilterTest<BitmapBlock<VarintPlusFormat>, BitmapKeyMustMatch>
    VarintPlusBitmapFilterTest;

TEST(VarintPlusBitmapFilterTest, VarintPlusBitmapFmt) {
  Random rnd(301);
  uint32_t num_keys = 0;
  while (num_keys <= (4 << 10)) {
    TEST_LogAndApply(this, &rnd, num_keys);
    if (num_keys == 0) {
      num_keys = 1;
    } else {
      num_keys *= 4;
    }
  }
}

// PForDelta bitmap filter
typedef FilterTest<BitmapBlock<PForDeltaFormat>, BitmapKeyMustMatch>
    PForDeltaBitmapFilterTest;

TEST(PForDeltaBitmapFilterTest, PForDeltaBitmapFmt) {
  Random rnd(301);
  uint32_t num_keys = 0;
  while (num_keys <= (4 << 10)) {
    TEST_LogAndApply(this, &rnd, num_keys);
    if (num_keys == 0) {
      num_keys = 1;
    } else {
      num_keys *= 4;
    }
  }
}

// Roaring bitmap filter
typedef FilterTest<BitmapBlock<RoaringFormat>, BitmapKeyMustMatch>
    RoaringBitmapFilterTest;

TEST(RoaringBitmapFilterTest, RoaringBitmapFmt) {
  Random rnd(301);
  uint32_t num_keys = 0;
  while (num_keys <= (4 << 10)) {
    TEST_LogAndApply(this, &rnd, num_keys);
    if (num_keys == 0) {
      num_keys = 1;
    } else {
      num_keys *= 4;
    }
  }
}

}  // namespace plfsio
}  // namespace pdlfs

#if defined(PDLFS_GFLAGS)
#include <gflags/gflags.h>
#endif
#if defined(PDLFS_GLOG)
#include <glog/logging.h>
#endif

static void BM_Usage() {
  fprintf(stderr, "Use --bench=wo or --bench=wr to select a benchmark.\n");
}

static void BM_Main(int* argc, char*** argv) {
#if defined(PDLFS_GFLAGS)
  google::ParseCommandLineFlags(argc, argv, true);
#endif
#if defined(PDLFS_GLOG)
  google::InitGoogleLogging((*argv)[0]);
  google::InstallFailureSignalHandler();
#endif
  pdlfs::Slice bench_name;
  if (*argc > 1) {
    bench_name = pdlfs::Slice((*argv)[*argc - 1]);
  }
  if (*argc <= 1) {
    BM_Usage();
  } else if (bench_name == "--bench=wo") {
    BM_Usage();
  } else if (bench_name == "--bench=wr") {
    BM_Usage();
  } else {
    BM_Usage();
  }
}

int main(int argc, char* argv[]) {
  pdlfs::Slice token;
  if (argc > 1) {
    token = pdlfs::Slice(argv[argc - 1]);
  }
  if (!token.starts_with("--bench")) {
    return pdlfs::test::RunAllTests(&argc, &argv);
  } else {
    BM_Main(&argc, &argv);
    return 0;
  }
}
