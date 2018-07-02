/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs/deltafs_api.h"
#include "deltafs_plfsio.h"
#include "deltafs_plfsio_cuckoo.h"
#include "deltafs_plfsio_filter.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"
#include "pdlfs-common/xxhash.h"

#include <fcntl.h>
#include <stdio.h>

#include <string>
#include <vector>

namespace pdlfs {
class PlfsDirTest {
 public:
  PlfsDirTest() {
    dirname_ = test::TmpDir() + "/plfsdir_test";
    wdir_ = rdir_ = NULL;
    epoch_ = 0;
  }

  ~PlfsDirTest() {
    if (wdir_ != NULL) {
      deltafs_plfsdir_free_handle(wdir_);
    }
    if (rdir_ != NULL) {
      deltafs_plfsdir_free_handle(rdir_);
    }
  }

  void OpenWriter() {
    const char* c = dirconf_.c_str();
    wdir_ = deltafs_plfsdir_create_handle(c, O_WRONLY, DELTAFS_PLFSDIR_DEFAULT);
    ASSERT_TRUE(wdir_ != NULL);
    deltafs_plfsdir_set_unordered(wdir_, 0);
    deltafs_plfsdir_force_leveldb_fmt(wdir_, 0);
    deltafs_plfsdir_set_fixed_kv(wdir_, 0);
    deltafs_plfsdir_set_side_io_buf_size(wdir_, 4096);
    deltafs_plfsdir_enable_side_io(wdir_, 1);
    deltafs_plfsdir_destroy(wdir_, dirname_.c_str());
    int r = deltafs_plfsdir_open(wdir_, dirname_.c_str());
    ASSERT_TRUE(r == 0);
  }

  void Finish() {
    int r = deltafs_plfsdir_finish(wdir_);
    ASSERT_TRUE(r == 0);
    deltafs_plfsdir_free_handle(wdir_);
    wdir_ = NULL;
  }

  void OpenReader() {
    const char* c = dirconf_.c_str();
    rdir_ = deltafs_plfsdir_create_handle(c, O_RDONLY, DELTAFS_PLFSDIR_DEFAULT);
    ASSERT_TRUE(rdir_ != NULL);
    deltafs_plfsdir_enable_side_io(rdir_, 1);
    int r = deltafs_plfsdir_open(rdir_, dirname_.c_str());
    ASSERT_TRUE(r == 0);
  }

  void Put(const Slice& k, const Slice& v) {
    if (wdir_ == NULL) OpenWriter();
    ssize_t r = deltafs_plfsdir_put(wdir_, k.data(), k.size(), epoch_, v.data(),
                                    v.size());
    ASSERT_TRUE(r == v.size());
  }

  void IoWrite(const Slice& d) {
    if (wdir_ == NULL) OpenWriter();
    ssize_t r = deltafs_plfsdir_io_append(wdir_, d.data(), d.size());
    ASSERT_TRUE(r == d.size());
  }

  void FinishEpoch() {
    if (wdir_ == NULL) OpenWriter();
    int r = deltafs_plfsdir_epoch_flush(wdir_, epoch_);
    ASSERT_TRUE(r == 0);
    r = deltafs_plfsdir_io_flush(wdir_);
    ASSERT_TRUE(r == 0);
    epoch_++;
  }

  std::string Get(const Slice& k) {
    if (wdir_ != NULL) Finish();
    if (rdir_ == NULL) OpenReader();
    size_t sz = 0;
    char* result =
        deltafs_plfsdir_get(rdir_, k.data(), k.size(), -1, &sz, NULL, NULL);
    ASSERT_TRUE(result != NULL);
    std::string tmp = Slice(result, sz).ToString();
    free(result);
    return tmp;
  }

  std::string IoRead(uint64_t off, size_t sz) {
    if (wdir_ != NULL) Finish();
    if (rdir_ == NULL) OpenReader();
    char* result = static_cast<char*>(malloc(sz));
    ssize_t r = deltafs_plfsdir_io_pread(rdir_, result, sz, off);
    ASSERT_TRUE(r >= 0);
    std::string tmp = Slice(result, r).ToString();
    free(result);
    return tmp;
  }

  std::string dirname_;
  std::string dirconf_;
  std::vector<std::string> options_;
  deltafs_plfsdir_t* wdir_;
  deltafs_plfsdir_t* rdir_;
  int epoch_;
};

TEST(PlfsDirTest, Empty) {
  FinishEpoch();
  std::string val = Get("non-exists");
  ASSERT_TRUE(val.empty());
}

TEST(PlfsDirTest, SingleEpoch) {
  Put("k1", "v1");
  IoWrite("a");
  Put("k2", "v2");
  IoWrite("b");
  Put("k3", "v3");
  IoWrite("c");
  Put("k4", "v4");
  IoWrite("x");
  Put("k5", "v5");
  IoWrite("y");
  Put("k6", "v6");
  IoWrite("z");
  FinishEpoch();
  ASSERT_EQ(IoRead(0, 6), "abcxyz");
  ASSERT_EQ(Get("k1"), "v1");
  ASSERT_EQ(Get("k2"), "v2");
  ASSERT_EQ(Get("k3"), "v3");
  ASSERT_EQ(Get("k4"), "v4");
  ASSERT_EQ(Get("k5"), "v5");
  ASSERT_EQ(Get("k6"), "v6");
}

class PlfsWiscBench {
  static int GetOptions(const char* key, int defval) {
    const char* env = getenv(key);
    if (!env || !env[0]) {
      return defval;
    } else {
      return atoi(env);
    }
  }

  void GetCompressionOptions() {
    if (GetOptions("COMPRESSION", 1)) dirconfs_.push_back("compression=snappy");
    if (GetOptions("FORCE_COMPRESSION", 1))
      dirconfs_.push_back("force_compression=true");
    if (GetOptions("INDEX_COMPRESSION", 0))
      dirconfs_.push_back("index_compression=snappy");
  }

  void GetFilterOptions() {
    dirconfs_.push_back("bf_bits_per_key=12");
    dirconfs_.push_back("filter=bloom");
  }

  void GetMemTableOptions() {
    dirconfs_.push_back("total_memtable_budget=24MiB");
    dirconfs_.push_back("compaction_buffer=2MiB");
    dirconfs_.push_back("lg_parts=2");
  }

  void GetBlkOptions() {
    dirconfs_.push_back("block_padding=false");
    dirconfs_.push_back("block_size=32KiB");
    dirconfs_.push_back("leveldb_compatible=false");
    dirconfs_.push_back("fixed_kv=true");
    dirconfs_.push_back("value_size=12");
    dirconfs_.push_back("key_size=8");
  }

  void GetIoOptions() {
    dirconfs_.push_back("min_index_buffer=2MiB");
    dirconfs_.push_back("index_buffer=2MiB");
    dirconfs_.push_back("min_data_buffer=3MiB");
    dirconfs_.push_back("data_buffer=4MiB");
  }

  std::string AssembleDirConf() {
    std::string conf("rank=0");
    std::vector<std::string>::iterator it;
    for (it = dirconfs_.begin(); it != dirconfs_.end(); ++it) {
      conf.append("&" + *it);
    }
    return conf;
  }

 public:
  PlfsWiscBench() : dirname_(test::TmpDir() + "/plfsdir_test_benchmark") {
    GetCompressionOptions();
    GetFilterOptions();
    GetMemTableOptions();
    GetBlkOptions();
    GetIoOptions();

    value_size_ = 40;
    unordered_ = 0;
    mfiles_ = 1;
    kranks_ = 1;
  }

  ~PlfsWiscBench() {
    if (dir_ != NULL) {
      deltafs_plfsdir_free_handle(dir_);
    }
  }

  void LogAndApply() {
    std::string conf = AssembleDirConf();
    const char* c = conf.c_str();
    dir_ = deltafs_plfsdir_create_handle(c, O_WRONLY, DELTAFS_PLFSDIR_DEFAULT);
    ASSERT_TRUE(dir_ != NULL);
    deltafs_plfsdir_enable_side_io(dir_, 1);
    deltafs_plfsdir_set_unordered(dir_, unordered_);
    deltafs_plfsdir_destroy(dir_, dirname_.c_str());
    int r = deltafs_plfsdir_open(dir_, dirname_.c_str());
    ASSERT_TRUE(r == 0);
    char tmp1[8];
    char tmp2[12];
    uint32_t num_files = mfiles_ << 20;
    uint32_t comm_sz = kranks_ << 10;
    uint32_t k = 0;
    fprintf(stderr, "Inserting data...\n");
    for (uint32_t i = 0; i < num_files / comm_sz; i++) {
      for (uint32_t j = 0; j < comm_sz; j++) {
        if ((k & 0x7FFFFu) == 0) {
          fprintf(stderr, "\r%.2f%%", 100.0 * k / num_files);
        }
        uint64_t h = xxhash64(&k, sizeof(k), 0);
        uint32_t f = xxhash32(&h, sizeof(h), 301);
        uint32_t a = f % comm_sz;
        EncodeFixed32(tmp1, a);
        EncodeFixed32(tmp1 + 4, f);
        uint32_t g = xxhash32(&h, sizeof(h), 103);
        uint32_t b = g % comm_sz;
        EncodeFixed32(tmp2, b);
        uint64_t c = i * comm_sz + h % comm_sz;
        // c *= value_size_;
        EncodeFixed64(tmp2 + 4, c);
        ssize_t rr = deltafs_plfsdir_put(dir_, tmp1, sizeof(tmp1), 0, tmp2,
                                         sizeof(tmp2));
        ASSERT_TRUE(rr == sizeof(tmp2));
        k++;
      }
    }
    fprintf(stderr, "\r100.00%%");
    fprintf(stderr, "\n");

    r = deltafs_plfsdir_epoch_flush(dir_, 0);
    ASSERT_TRUE(r == 0);
    r = deltafs_plfsdir_finish(dir_);
    ASSERT_TRUE(r == 0);

    fprintf(stderr, "Done!\n");
    PrintStats();
  }

  void PrintStats() {
    typedef long long integer;
#define GETPROP(h, k) deltafs_plfsdir_get_integer_property(h, k)
    const double ki = 1024.0;
    const int num_files = mfiles_ << 20;
    const int side_storage = value_size_ * num_files;
    const int entry_size = 8 + value_size_;
    fprintf(stderr, "----------------------------------------\n");
    fprintf(stderr, "   Total User Data: %.2f MiB (%.2f MiB keys)\n",
            entry_size * num_files / ki / ki, 8 * num_files / ki / ki);
    integer tbw = GETPROP(dir_, "io.total_bytes_written");
    fprintf(stderr,
            " Total Dir Storage: %.2f MiB (%.2f MiB main + %.2f MiB side "
            "storage)\n",
            (tbw + side_storage) / ki / ki, tbw / ki / ki,
            side_storage / ki / ki);
    integer sdb = GETPROP(dir_, "sstable_data_bytes");
    integer sfb = GETPROP(dir_, "sstable_filter_bytes");
    integer sib = GETPROP(dir_, "sstable_index_bytes");
    fprintf(stderr, "         Breakdown: D=%.2f MiB, F=%.2f MiB, I=%.2f MiB\n",
            sdb / ki / ki, sfb / ki / ki, sib / ki / ki);
    fprintf(stderr,
            "           Per Key: D+=%.2f Bytes, F=%.2f Bytes, I=%.2f Bytes\n",
            1.0 * sdb / num_files - 8, 1.0 * sfb / num_files,
            1.0 * sib / num_files);
    fprintf(stderr, "              Cost: D+=%.2f%%, F=%.2f%%, I=%.2f%%\n",
            100.0 * (1.0 * sdb / num_files - 8) / entry_size,
            100.0 * sfb / num_files / entry_size,
            100.0 * sib / num_files / entry_size);

    fprintf(stderr, "             Value: %d Bytes\n", value_size_);
    fprintf(stderr, "               Key: 8 Bytes\n");
#undef GETPROP
  }

 private:
  int unordered_;
  int value_size_;
  deltafs_plfsdir_t* dir_;
  std::vector<std::string> dirconfs_;
  std::string dirname_;
  int mfiles_;
  int kranks_;
};

namespace {
template <uint32_t N = 10>
class Histo {
 public:
  Histo() { Clear(); }

  void Add(uint32_t a) {
    sum_ += a;
    max_ = std::max(max_, a);
    if (a > N - 1) {
      a = N - 1;
    }
    rep_[a]++;
    num_++;
  }

  double CDF(uint32_t a) {
    if (num_ == 0) return 0;
    double subtotal = 0;
    for (uint32_t i = 0; i <= a && i < N; i++) {
      subtotal += rep_[i];
    }
    return subtotal / num_;
  }

  double Average() const {
    if (num_ == 0) return 0;
    return sum_ / num_;
  }

  void Clear() {
    memset(rep_, 0, sizeof(rep_));
    max_ = 0;
    num_ = 0;
    sum_ = 0;
  }

  uint32_t max_;
  uint32_t num_;  // Total number of samples
  // Num of times we get i results (0 <= i <= N-1)
  uint32_t rep_[N];
  double sum_;
};

}  // namespace

template <typename FilterType, plfsio::FilterTester filter_tester>
class PlfsFtBench {
  static int GetOptions(const char* key, int def) {
    const char* env = getenv(key);
    if (env && env[0]) {
      return atoi(env);
    } else {
      return def;
    }
  }

 public:
  PlfsFtBench() {
    options_.cuckoo_frac = 0.95;
    options_.bf_bits_per_key = GetOptions("BF_BITS_PER_KEY", 20);
    kranks_ = GetOptions("NUM_RANKS", 1);
    qstep_ = GetOptions("QUERY_STEP", 1);
    mkeys_ = 1;
  }

  ~PlfsFtBench() {
    if (bf_ != NULL) {
      delete bf_;
    }
  }

  void LogAndApply() {
    bf_ = new FilterType(options_, 0);  // Do not reserve memory
    ASSERT_TRUE(bf_ != NULL);
    uint32_t num_keys = mkeys_ << 20;
    uint32_t comm_sz = kranks_ << 10;
    bf_->Reset(num_keys);
    char tmp[12];
    fprintf(stderr, "Populating the bloom filter...\n");
    for (uint32_t k = 0; k < num_keys; k++) {
      if ((k & 0x7FFFFu) == 0) {
        fprintf(stderr, "\r%.2f%%", 100.0 * k / num_keys);
      }
      uint64_t h = xxhash64(&k, sizeof(k), 0);
      uint32_t f = xxhash32(&h, sizeof(h), 301);
      uint32_t a = f % comm_sz;
      EncodeFixed32(tmp, a);
      EncodeFixed32(tmp + 4, f);
      uint32_t g = xxhash32(&h, sizeof(h), 103);
      uint32_t b = g % comm_sz;
      EncodeFixed32(tmp + 8, b);
      bf_->AddKey(Slice(tmp, sizeof(tmp)));
    }
    fprintf(stderr, "\r100.00%%");
    fprintf(stderr, "\n");

    ft_ = bf_->Finish();
    ASSERT_TRUE(!ft_.empty());

    fprintf(stderr, "Done!\n");
    Query();
  }

  void Query() {
    uint32_t num_keys = mkeys_ << 20;
    uint32_t comm_sz = kranks_ << 10;
    char tmp[12];
    fprintf(stderr, "Querying...\n");
    for (uint32_t k = 0; k < num_keys; k += qstep_) {
      if ((k & 0x7FFFu) == 0) {
        fprintf(stderr, "\r%.2f%%", 100.0 * k / num_keys);
      }
      uint64_t h = xxhash64(&k, sizeof(k), 0);
      uint32_t f = xxhash32(&h, sizeof(h), 301);
      uint32_t a = f % comm_sz;
      EncodeFixed32(tmp, a);
      EncodeFixed32(tmp + 4, f);
      uint32_t n = 0;
      for (uint32_t r = 0; r < comm_sz; r++) {
        EncodeFixed32(tmp + 8, r);
        if (filter_tester(Slice(tmp, sizeof(tmp)), ft_)) {
          n++;
        }
      }
      histo_.Add(n);
    }
    fprintf(stderr, "\r100.00%%");
    fprintf(stderr, "\n");
    fprintf(stderr, "Done!\n");

    Report();
  }

  void Report() {
    fprintf(stderr, "----------------------------------------\n");
    fprintf(stderr, "           Num Keys: %d M\n", mkeys_);
    fprintf(stderr, "              Ranks: %d K\n", kranks_);
    fprintf(stderr, "        Num Queries: %d\n", int(histo_.num_));
    fprintf(stderr, "   Avg Hits Per Key: %.3f, MAX=%d (BF bits=%d)\n",
            histo_.Average(), int(histo_.max_), int(options_.bf_bits_per_key));
    fprintf(stderr, "         CDF 1 Hits: %.2f%%\n", histo_.CDF(1) * 100);
    for (uint32_t i = 2; i <= 128; i++) {
      double d = histo_.CDF(i);
      if (d > 0.01 && d < 0.99)
        fprintf(stderr, "          %4u Hits: %.2f%%\n", i, d * 100);
    }
  }

 private:
  Slice ft_;  // Point to filter data once finished
  Histo<128> histo_;
  plfsio::DirOptions options_;
  FilterType* bf_;
  int kranks_;
  int mkeys_;
  int qstep_;
};

}  // namespace pdlfs

#if defined(PDLFS_GFLAGS)
#include <gflags/gflags.h>
#endif
#if defined(PDLFS_GLOG)
#include <glog/logging.h>
#endif

static void BM_Usage() {
  fprintf(stderr, "Use --bench=[wisc, bf, or cf] to launch tests.\n");
  fprintf(stderr, "\n");
}

static void BM_LogAndApply(const char* bm) {
#define BENCH(x, y) \
  pdlfs::PlfsFtBench<pdlfs::plfsio::x##y, pdlfs::plfsio::x##KeyMayMatch>
  if (strcmp(bm, "wisc") == 0) {
    pdlfs::PlfsWiscBench bench;
    bench.LogAndApply();
  } else if (strcmp(bm, "bf") == 0) {
    BENCH(Bloom, Block) bench;
    bench.LogAndApply();
  } else if (strcmp(bm, "cf") == 0) {
    BENCH(Cuckoo, Block<16>) bench;
    bench.LogAndApply();
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
  pdlfs::Slice bm_arg;
  if (*argc > 1) {
    bm_arg = pdlfs::Slice((*argv)[*argc - 1]);
  } else {
    BM_Usage();
  }
  if (bm_arg.starts_with("--bench=")) {
    BM_LogAndApply(bm_arg.c_str() + 8);
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
