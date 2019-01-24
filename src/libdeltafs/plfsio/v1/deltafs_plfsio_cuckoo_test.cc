/*
 * Copyright (c) 2018-2019 Carnegie Mellon University and
 *         Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_cuckoo.h"
#include "deltafs_plfsio_filter.h"
#include "deltafs_plfsio_types.h"

#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {
namespace plfsio {

class CuckooTest {
 public:
  CuckooTest() {
    // Ignore target occupation rate and always allocate the exact number of
    // cuckoo buckets
    options_.cuckoo_frac = -1;
    cf_ = new CF(options_, 0);  // Do not reserve memory
  }

  ~CuckooTest() {
    if (cf_ != NULL) {
      delete cf_;
    }
  }

  static uint32_t KeyFringerprint(uint64_t ha) {
    return CuckooFingerprint(ha, keybits_);
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

  bool AddKey(uint32_t k) {
    char tmp[4];
    EncodeFixed32(tmp, k);
    return cf_->TEST_AddKey(Slice(tmp, sizeof(tmp)));
  }

  void Finish() { data_ = cf_->TEST_Finish(); }
  void Reset(uint32_t num_keys) { cf_->Reset(num_keys); }
  enum { keybits_ = 16 };
  typedef CuckooBlock<keybits_, 0> CF;
  DirOptions options_;
  std::string data_;  // Final filter data
  CF* cf_;
};

TEST(CuckooTest, BytesPerBucket) {
  fprintf(stderr, "%d\n", int(cf_->TEST_BytesPerCuckooBucket()));
}

TEST(CuckooTest, AltIndex) {
  for (uint32_t ki = 1; ki <= 1024; ki *= 2) {
    uint32_t num_keys = ki << 10;
    Reset(num_keys);
    size_t num_buckets = cf_->TEST_NumBuckets();
    uint32_t k = 0;
    for (; k < num_keys; k++) {
      uint64_t hash = KeyHash(k);
      uint32_t fp = KeyFringerprint(hash);
      size_t i1 = hash % num_buckets, i2 = CuckooAlt(i1, fp) % num_buckets;
      size_t i3 = CuckooAlt(i2, fp) % num_buckets;
      ASSERT_TRUE(i1 == i3);
    }
  }
}

TEST(CuckooTest, Empty) {
  for (uint32_t ki = 1; ki <= 1024; ki *= 2) {
    uint32_t num_keys = ki << 10;
    Reset(num_keys);
    Finish();
    uint32_t k = 0;
    for (; k < num_keys; k++) {
      ASSERT_FALSE(KeyMayMatch(k));
    }
  }
}

TEST(CuckooTest, AddAndMatch) {
  for (uint32_t ki = 1; ki <= 1024; ki *= 2) {
    uint32_t num_keys = ki << 10;
    fprintf(stderr, "%4u Ki keys: ", ki);
    Reset(num_keys);
    uint32_t k = 0;
    for (; k < num_keys; k++) {
      if (!AddKey(k)) {
        break;
      }
    }
    Finish();
    fprintf(stderr, "%.2f%% filled\n", 100.0 * k / num_keys);
    uint32_t j = 0;
    for (; j < k; j++) {
      ASSERT_TRUE(KeyMayMatch(j));
    }
  }
}

class CuckooAuxTest : public CuckooTest {
 public:
  void AddKey(uint32_t k) {
    char tmp[4];
    EncodeFixed32(tmp, k);
    cf_->AddKey(Slice(tmp, sizeof(tmp)));
  }
};

TEST(CuckooAuxTest, AuxiliaryTables) {
  for (uint32_t ki = 1; ki <= 1024; ki *= 2) {
    uint32_t num_keys = ki << 10;
    fprintf(stderr, "%4u Ki keys: ", ki);
    Reset(num_keys);
    uint32_t k = 0;
    for (; k < num_keys; k++) {
      AddKey(k);
    }
    Finish();
    fprintf(stderr, "%.2fx buckets, %+d aux tables\n",
            1.0 * cf_->TEST_NumBuckets() / ((num_keys + 3) / 4),
            int(cf_->TEST_NumCuckooTables()) - 1);
    uint32_t j = 0;
    for (; j < k; j++) {
      ASSERT_TRUE(KeyMayMatch(j));
    }
  }
}

// Evaluate false positive rate under different filter configurations.
class PlfsFalsePositiveBench {
 protected:
  static int FromEnv(const char* key, int def) {
    const char* env = getenv(key);
    if (env && env[0]) {
      return atoi(env);
    } else {
      return def;
    }
  }

  static inline int GetOption(const char* key, int def) {
    int opt = FromEnv(key, def);
    fprintf(stderr, "%s=%d\n", key, opt);
    return opt;
  }

  void Report(uint32_t hits, uint32_t n) {
    const double ki = 1024.0;
    fprintf(stderr, "------------------------------------------------\n");
    fprintf(stderr, "          Bits per k: %d\n", int(keybits_));
    fprintf(stderr, "       Keys inserted: %.3g Mi\n", n / ki / ki);
    const uint32_t num_queries = 1u << qlg_;
    fprintf(stderr, "             Queries: %.3g Mi (ALL neg)\n",
            num_queries / ki / ki);
    fprintf(stderr, "                Hits: %u\n", hits);
    fprintf(stderr, "                  FP: %.4g%%\n",
            100.0 * hits / num_queries);
  }

  DirOptions options_;
  std::string filterdata_;
  size_t keybits_;
  // Number of keys to query is 1u << qlg_
  size_t qlg_;
  size_t nlg_;
};

class PlfsBloomBench : protected PlfsFalsePositiveBench {
 public:
  PlfsBloomBench() {
    keybits_ = GetOption("BLOOM_KEY_BITS", 12);
    nlg_ = GetOption("LG_KEYS", 20);
    assert(nlg_ < 30);
    qlg_ = GetOption("LG_QUERIES", nlg_);
    assert(qlg_ < 30);
  }

  // Store filter data in *dst. Return number of keys inserted.
  uint32_t BuildFilter(std::string* const dst) {
    char tmp[4];
    Slice key(tmp, sizeof(tmp));
    options_.bf_bits_per_key = keybits_;
    BloomBlock ft(options_, 0);  // Do not reserve memory for it
    const uint32_t num_keys = 1u << nlg_;
    ft.Reset(num_keys);
    uint32_t i = 0;
    for (; i < num_keys; i++) {
      EncodeFixed32(tmp, i);
      ft.AddKey(key);
    }
    *dst = ft.TEST_Finish();
    return i;
  }

  void LogAndApply() {
    uint32_t n = BuildFilter(&filterdata_);
    uint32_t hits = 0;
    char tmp[4];
    Slice key(tmp, sizeof(tmp));
    const uint32_t num_queries = 1u << qlg_;
    uint32_t i = n;
    for (; i < n + num_queries; i++) {
      EncodeFixed32(tmp, i);
      if (BloomKeyMayMatch(key, filterdata_)) {
        hits++;
      }
    }

    Report(hits, n);
  }
};

class PlfsCuckoBench : protected PlfsFalsePositiveBench {
 public:
  PlfsCuckoBench() {
    use_auxtables_ = GetOption("CUCKOO_ENABLE_AUX", 1);
    keybits_ = GetOption("CUCKOO_KEY_BITS", 12);
    nlg_ = GetOption("LG_KEYS", 20);
    assert(nlg_ < 30);
    qlg_ = GetOption("LG_QUERIES", nlg_);
    assert(qlg_ < 30);
  }

  template <size_t k>
  uint32_t CuckooBuildFilter(uint32_t* num_buckets, std::string* const dst) {
    char tmp[4];
    Slice key(tmp, sizeof(tmp));
    options_.cuckoo_frac = -1;
    CuckooBlock<k, 0> ft(options_, 0);  // Do not reserve memory for it
    const uint32_t num_keys = 1u << nlg_;
    ft.Reset(num_keys);
    uint32_t i = 0;
    for (; i < num_keys; i++) {
      EncodeFixed32(tmp, i);
      if (use_auxtables_) {
        ft.AddKey(key);
      } else if (!ft.TEST_AddKey(key)) {
        break;
      }
    }
    *dst = ft.TEST_Finish();
    *num_buckets = ft.TEST_NumBuckets();
    return i;
  }

  void LogAndApply() {
    uint32_t num_buckets;
    uint32_t n;
    switch (keybits_) {
#define CASE(k)                                           \
  case k:                                                 \
    n = CuckooBuildFilter<k>(&num_buckets, &filterdata_); \
    break
      CASE(10);
      CASE(12);
      CASE(14);
      CASE(16);
      CASE(18);
      CASE(20);
      CASE(24);
      CASE(32);
      default:
        n = 0;
    }
    uint32_t hits = 0;
    char tmp[4];
    Slice key(tmp, sizeof(tmp));
    const uint32_t num_queries = 1u << qlg_;
    uint32_t i = n;
    for (; i < n + num_queries; i++) {
      EncodeFixed32(tmp, i);
      if (CuckooKeyMayMatch(key, filterdata_)) {
        hits++;
      }
    }

    Report(num_buckets, hits, n);
  }

  void Report(uint32_t num_buckets, uint32_t hits, uint32_t n) {
    PlfsFalsePositiveBench::Report(hits, n);
    const double ki = 1024.0;
    fprintf(stderr, "   Cuckoo bits per k: %.2f\n",
            1.0 * keybits_ * 4 * num_buckets / n);
    fprintf(stderr, "             Buckets: %.3g Ki = %.3g Mi keys\n",
            1.0 * num_buckets / ki, 4.0 * num_buckets / ki / ki);
    fprintf(stderr, "                Util: %.2f%%\n",
            100.0 * n / num_buckets / 4);
  }

  // If aux tables should be used
  int use_auxtables_;
};

}  // namespace plfsio
}  // namespace pdlfs

#if defined(PDLFS_GFLAGS)
#include <gflags/gflags.h>
#endif
#if defined(PDLFS_GLOG)
#include <glog/logging.h>
#endif

static void BM_Usage() {
  fprintf(stderr, "Use --bench=[bf,cf] to run benchmark.\n");
  fprintf(stderr, "\n");
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
  } else {
    BM_Usage();
  }
  if (bench_name.starts_with("--bench=bf")) {
    typedef pdlfs::plfsio::PlfsBloomBench BM_Bench;
    BM_Bench bench;
    bench.LogAndApply();
  } else if (bench_name.starts_with("--bench=cf")) {
    typedef pdlfs::plfsio::PlfsCuckoBench BM_Bench;
    BM_Bench bench;
    bench.LogAndApply();
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
