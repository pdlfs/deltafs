/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "filter.h"
#include "types.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/histogram.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

#include <stdlib.h>
#ifdef PDLFS_PLATFORM_POSIX
#include <sys/resource.h>
#include <sys/time.h>
#endif

#include <algorithm>
#include <set>
#include <vector>

namespace pdlfs {
namespace plfsio {

// A generic filter test that can work with different
// filter implementations.
template <typename T, FilterTester tester>
class FilterTest {
 public:
  FilterTest(size_t key_bits = 24) : key_bits_(key_bits), ft_(NULL) {
    options_.bf_bits_per_key = 10;  // Override the defaults
    options_.bm_fmt = static_cast<BitmapFormat>(BitmapFormatFromType<T>());
    options_.bm_key_bits = key_bits_;
  }

  ~FilterTest() {
    delete ft_;  // Done
  }

  // Reset the underlying filter.
  void Reset(uint32_t num_keys) {
    if (ft_ == NULL) ft_ = new T(options_, 0);  // Does not reserve memory
    ft_->Reset(num_keys);
  }

  // REQUIRES: Reset() must have been called and Finish()
  // has not been called after the Reset().
  void AddKey(uint32_t seq) {
    char tmp[4];
    EncodeFixed32(tmp, seq);
    Slice key(tmp, sizeof(tmp));
    ft_->AddKey(key);
  }

  // Finalize and obtain filter contents.
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
    char tmp[4];
    EncodeFixed32(tmp, seq);
    Slice key(tmp, sizeof(tmp));
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
  fprintf(stderr, "%f%% FULL: %.2f per key, %u keys",
          100.0 * double(num_keys) / (1u << key_bits),
          8.0 * double(contents.size()) / num_keys, num_keys);
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

  fprintf(stderr, " OK! \n");
}

typedef FilterTest<BloomBlock, BloomKeyMayMatch> BloomFilterTest;

TEST(BloomFilterTest, BloomFormat) {
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

typedef FilterTest<BitmapBlock<UncompressedFormat>, BitmapKeyMustMatch>
    UncompressedBitmapFilterTest;
TEST(UncompressedBitmapFilterTest, UncompressedFormat) {
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

typedef FilterTest<BitmapBlock<VbFormat>, BitmapKeyMustMatch>
    VbBitmapFilterTest;
TEST(VbBitmapFilterTest, VbFormat) {
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

typedef FilterTest<BitmapBlock<VbPlusFormat>, BitmapKeyMustMatch>
    VbPlusBitmapFilterTest;
TEST(VbPlusBitmapFilterTest, VbPlusFormat) {
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

typedef FilterTest<BitmapBlock<FastVbPlusFormat>, BitmapKeyMustMatch>
    FastVbPlusBitmapFilterTest;
TEST(FastVbPlusBitmapFilterTest, FastVbPlusFormat) {
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

typedef FilterTest<BitmapBlock<PfDeltaFormat>, BitmapKeyMustMatch>
    PfDeltaBitmapFilterTest;
TEST(PfDeltaBitmapFilterTest, PfDeltaFormat) {
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

typedef FilterTest<BitmapBlock<FastPfDeltaFormat>, BitmapKeyMustMatch>
    FastPfDeltaBitmapFilterTest;
TEST(FastPfDeltaBitmapFilterTest, FastPfDeltaFormat) {
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

typedef FilterTest<BitmapBlock<RoaringFormat>, BitmapKeyMustMatch>
    RoaringBitmapFilterTest;
TEST(RoaringBitmapFilterTest, RoaringFormat) {
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

template <typename T>
class PlfsFilterBench {
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

 public:
  explicit PlfsFilterBench(size_t key_bits = 24)
      : num_tables_(GetOption("TABLE_NUM", 64)), key_bits_(key_bits) {
    options_.bf_bits_per_key = GetOption("BF_BITS", 10);
    options_.bm_fmt = static_cast<BitmapFormat>(BitmapFormatFromType<T>());
    options_.bm_key_bits = key_bits_;

    fprintf(stderr, "Generating unordered keys ... (may take a while)\n");
    keys_.reserve(1u << key_bits_);
    for (uint32_t x = 0; x < (1u << key_bits_); x++) keys_.push_back(x);
    std::random_shuffle(keys_.begin(), keys_.end());
    fprintf(stderr, "Done!\n");

    ft_ = new T(options_, 0);
  }

  ~PlfsFilterBench() {
    if (ft_ != NULL) {
      delete ft_;
    }
  }

#if defined(PDLFS_PLATFORM_POSIX)
  static inline double ToSecs(const struct timeval* tv) {
    return tv->tv_sec + tv->tv_usec / 1000.0 / 1000.0;
  }
#endif

  Slice BuildFilter(size_t num_keys, std::vector<uint32_t>::iterator& it) {
    char tmp[4];
    Slice key(tmp, sizeof(tmp));
    ft_->Reset(num_keys);
    for (size_t i = 0; i < num_keys; i++) {
      EncodeFixed32(tmp, *it);
      ft_->AddKey(key);
      ++it;
    }
    return ft_->Finish();
  }

  void LogAndApply() {
    const double ki = 1024.0;
#if defined(PDLFS_PLATFORM_POSIX)
#if defined(PDLFS_OS_LINUX)
    const double k = 1000.0;
    const uint64_t start = CurrentMicros();
#endif
    struct rusage tmp_usage;
    int r0 = getrusage(RUSAGE_SELF, &tmp_usage);
    ASSERT_EQ(r0, 0);
#endif
    fprintf(stderr, "Inserting keys ... (%d tables)\n", int(num_tables_));
    size_t size = 0;  // Accumulated filter size
    const size_t num_keys = (1u << key_bits_) / num_tables_;  // Keys per table
    std::vector<uint32_t>::iterator it = keys_.begin();
    for (size_t j = 0; j < num_tables_; j++) {
      fprintf(stderr, "\r%d/%d", int(j), int(num_tables_));
      Slice contents = BuildFilter(num_keys, it);
      size += contents.size();
    }
    fprintf(stderr, "\r%d/%d\n", int(num_tables_), int(num_tables_));
    fprintf(stderr, "Done!\n");
#if defined(PDLFS_PLATFORM_POSIX)
#if defined(PDLFS_OS_LINUX)
    const uint64_t end = CurrentMicros();
    const uint64_t dura = end - start;
#endif
#endif
    fprintf(stderr, "----------------------------------------\n");
    fprintf(stderr, " #Keys Per Filter: %d\n", int(num_keys));
    fprintf(stderr, "         #Filters: %d\n", int(num_tables_));
    fprintf(stderr, "      Filter Size: %.2f MiB\n", 1.0 * size / ki / ki);
    fprintf(stderr, "          Density: %.2f%%\n", 100.0 / num_tables_);
    fprintf(stderr, "             Cost: %.2f (bits per key)\n",
            8.0 * size / (num_keys * num_tables_));
    fprintf(stderr, "       Memory Use: %.2f MiB\n",
            1.0 * ft_->memory_usage() / ki / ki);

#if defined(PDLFS_PLATFORM_POSIX)
    struct rusage usage;
    int r1 = getrusage(RUSAGE_SELF, &usage);
    ASSERT_EQ(r1, 0);
    double utime = ToSecs(&usage.ru_utime) - ToSecs(&tmp_usage.ru_utime);
    double stime = ToSecs(&usage.ru_stime) - ToSecs(&tmp_usage.ru_stime);
    fprintf(stderr, "    User CPU Time: %.3f s\n", utime);
    fprintf(stderr, "  System CPU Time: %.3f s\n", stime);
#if defined(PDLFS_OS_LINUX)
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    int r2 = sched_getaffinity(getpid(), sizeof(cpu_set), &cpu_set);
    ASSERT_EQ(r2, 0);
    const int cc = CPU_COUNT(&cpu_set);
    fprintf(stderr, "    Num CPU Cores: %d\n", cc);
    fprintf(stderr, "        CPU Usage: %.1f%%\n",
            k * k * (utime + stime) / dura / cc * 100);
#endif
#endif
  }

 protected:
  const size_t num_tables_;  // Num tables per epoch
  const size_t key_bits_;
  std::vector<uint32_t> keys_;
  DirOptions options_;
  T* ft_;
};

template <typename T, FilterTester tester>
class PlfsFilterQueryBench : public PlfsFilterBench<T> {
 public:
  explicit PlfsFilterQueryBench(size_t key_bits = 24)
      : PlfsFilterBench<T>(key_bits) {}

  void RunQueries(size_t num_keys, std::vector<uint32_t>::iterator& it,
                  const Slice& filter) {
    const size_t ckpt = num_keys / 100;
    char tmp[4];
    Slice key(tmp, sizeof(tmp));
    for (size_t i = 0; i < num_keys; i++) {
      if (i % ckpt == 0) {
        fprintf(stderr, "\r%d/%d", int(i), int(num_keys));
      }
      EncodeFixed32(tmp, *it);
      tester(key, filter);
      ++it;
    }
    fprintf(stderr, "\r%d/%d\n", int(num_keys), int(num_keys));
  }

  void LogAndApply() {
    const double k = 1000.0;
    const size_t num_keys = (1u << this->key_bits_) / this->num_tables_;
    fprintf(stderr, "Inserting keys ... (%d keys)\n", int(num_keys));
    std::vector<uint32_t>::iterator it = this->keys_.begin();
    const std::string contents = this->BuildFilter(num_keys, it).ToString();
    fprintf(stderr, "Re-shuffling keys ...\n");
    std::random_shuffle(this->keys_.begin(), it);
    fprintf(stderr, "Done!\n");
    fprintf(stderr, "Testing ...\n");
    const uint64_t start = CurrentMicros();
    it = this->keys_.begin();
    RunQueries(num_keys, it, contents);
    fprintf(stderr, "Done!\n");
    uint64_t dura = CurrentMicros() - start;
    fprintf(stderr, "----------------------------------------\n");
    fprintf(stderr, "             Total Time: %.3f s\n", dura / k / k);
    fprintf(stderr, " Avg. Latency Per Query: %.3f us\n",
            double(dura) / num_keys);
  }
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
  fprintf(stderr,
          "Use --bench=ft,<fmt> or --bench=fq,<fmt> to run benchmark.\n\n");
  fprintf(stderr, "== valid fmt are:\n\n");
  fprintf(stderr, " bf     (bloom filter)\n");
  fprintf(stderr, " bmp    (bitmap, uncompressed)\n");
  fprintf(stderr, " vb     (bitmap, varint)\n");
  fprintf(stderr, " vbp    (bitmap, modified varint)\n");
  fprintf(stderr, "fvbp    (bitmap, fast modified varint)\n");
  fprintf(stderr, " pfd    (bitmap, modified p-for-delta)\n");
  fprintf(stderr, "fpfd    (bitmap, fast modified p-for-delta)\n");
  fprintf(stderr, " r      (bitmap, modified roaring)\n");
  fprintf(stderr, "\n");
}

typedef pdlfs::plfsio::FilterTester BM_Tester;
template <typename T, BM_Tester M>
static void BM_LogAndApply(const char* bench) {
  if (strcmp(bench, "fq") == 0) {
    typedef pdlfs::plfsio::PlfsFilterQueryBench<T, M> BM_bench;
    BM_bench bench;
    bench.LogAndApply();
  } else if (strcmp(bench, "ft") == 0) {
    typedef pdlfs::plfsio::PlfsFilterBench<T> BM_bench;
    BM_bench bench;
    bench.LogAndApply();
  } else {
    BM_Usage();
  }
}

template <typename T>
static void BM_Bmp(const char* bench) {
  BM_LogAndApply<pdlfs::plfsio::BitmapBlock<T>,
                 pdlfs::plfsio::BitmapKeyMustMatch>(bench);
}

static void BM_Parse(const char* bench, const char* fmt) {
  if (fmt[0] != ',') {
    BM_Usage();
  } else if (strcmp(fmt + 1, "bf") == 0) {
    BM_LogAndApply<pdlfs::plfsio::BloomBlock, pdlfs::plfsio::BloomKeyMayMatch>(
        bench);
  } else if (strcmp(fmt + 1, "bmp") == 0) {
    BM_Bmp<pdlfs::plfsio::UncompressedFormat>(bench);
  } else if (strcmp(fmt + 1, "r") == 0) {
    BM_Bmp<pdlfs::plfsio::RoaringFormat>(bench);
  } else if (strcmp(fmt + 1, "fvbp") == 0) {
    BM_Bmp<pdlfs::plfsio::FastVbPlusFormat>(bench);
  } else if (strcmp(fmt + 1, "vbp") == 0) {
    BM_Bmp<pdlfs::plfsio::VbPlusFormat>(bench);
  } else if (strcmp(fmt + 1, "vb") == 0) {
    BM_Bmp<pdlfs::plfsio::VbFormat>(bench);
  } else if (strcmp(fmt + 1, "fpfd") == 0) {
    BM_Bmp<pdlfs::plfsio::FastPfDeltaFormat>(bench);
  } else if (strcmp(fmt + 1, "pfd") == 0) {
    BM_Bmp<pdlfs::plfsio::PfDeltaFormat>(bench);
  } else {
    BM_Usage();
  }
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
  if (bench_name.starts_with("--bench=ft")) {
    BM_Parse("ft", bench_name.c_str() + 10);
  } else if (bench_name.starts_with("--bench=fq")) {
    BM_Parse("fq", bench_name.c_str() + 10);
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
