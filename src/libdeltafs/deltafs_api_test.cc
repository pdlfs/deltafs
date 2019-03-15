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

#include "deltafs/deltafs_api.h"

#include "plfsio/v1/deltafs_plfsio_cuckoo.h"
#include "plfsio/v1/deltafs_plfsio_filter.h"
#include "plfsio/v1/deltafs_plfsio_types.h"

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

  void OpenWriter(int io_engine) {
    const char* c = dirconf_.c_str();
    wdir_ = deltafs_plfsdir_create_handle(c, O_WRONLY, io_engine);
    ASSERT_TRUE(wdir_ != NULL);
    deltafs_plfsdir_set_unordered(wdir_, 0);
    deltafs_plfsdir_force_leveldb_fmt(wdir_, 0);
    deltafs_plfsdir_set_fixed_kv(wdir_, 1);
    deltafs_plfsdir_set_key_size(wdir_, 2);
    deltafs_plfsdir_set_val_size(wdir_, 2);
    deltafs_plfsdir_set_side_io_buf_size(wdir_, 4096);
    deltafs_plfsdir_destroy(wdir_, dirname_.c_str());
    ASSERT_TRUE(deltafs_plfsdir_open(wdir_, dirname_.c_str()) == 0);
    ASSERT_TRUE(deltafs_plfsdir_io_open(wdir_, dirname_.c_str()) == 0);
  }

  void Finish() {
    ASSERT_TRUE(deltafs_plfsdir_io_finish(wdir_) == 0);
    ASSERT_TRUE(deltafs_plfsdir_finish(wdir_) == 0);
    deltafs_plfsdir_free_handle(wdir_);
    wdir_ = NULL;
  }

  void OpenReader(int io_engine) {
    const char* c = dirconf_.c_str();
    rdir_ = deltafs_plfsdir_create_handle(c, O_RDONLY, io_engine);
    ASSERT_TRUE(rdir_ != NULL);
    ASSERT_TRUE(deltafs_plfsdir_open(rdir_, dirname_.c_str()) == 0);
    ASSERT_TRUE(deltafs_plfsdir_io_open(rdir_, dirname_.c_str()) == 0);
  }

  void Put(const Slice& k, const Slice& v) {
    if (wdir_ == NULL) OpenWriter(kDefEngine);
    ssize_t r = deltafs_plfsdir_put(wdir_, k.data(), k.size(), epoch_, v.data(),
                                    v.size());
    ASSERT_TRUE(r == v.size());
  }

  void IoWrite(const Slice& d) {
    if (wdir_ == NULL) OpenWriter(kDefEngine);
    ssize_t r = deltafs_plfsdir_io_append(wdir_, d.data(), d.size());
    ASSERT_TRUE(r == d.size());
  }

  void Flush() {
    if (wdir_ == NULL) OpenWriter(kDefEngine);
    int r = deltafs_plfsdir_flush(wdir_, epoch_);
    ASSERT_TRUE(r == 0);
    r = deltafs_plfsdir_io_flush(wdir_);
    ASSERT_TRUE(r == 0);
  }

  void FinishEpoch() {
    if (wdir_ == NULL) OpenWriter(kDefEngine);
    int r = deltafs_plfsdir_epoch_flush(wdir_, epoch_);
    ASSERT_TRUE(r == 0);
    r = deltafs_plfsdir_io_flush(wdir_);
    ASSERT_TRUE(r == 0);
    epoch_++;
  }

  std::string Get(const Slice& k) {
    if (wdir_ != NULL) Finish();
    if (rdir_ == NULL) OpenReader(kDefEngine);
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
    if (rdir_ == NULL) OpenReader(kDefEngine);
    char* result = static_cast<char*>(malloc(sz));
    ssize_t r = deltafs_plfsdir_io_pread(rdir_, result, sz, off);
    ASSERT_TRUE(r >= 0);
    std::string tmp = Slice(result, r).ToString();
    free(result);
    return tmp;
  }

  std::string dirname_;
  std::string dirconf_;
  enum { kDefEngine = DELTAFS_PLFSDIR_DEFAULT };
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
  Flush();
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

TEST(PlfsDirTest, PdbEmpty) {
  OpenWriter(DELTAFS_PLFSDIR_PLAINDB);
  FinishEpoch();
  Finish();
  OpenReader(DELTAFS_PLFSDIR_PLAINDB);
  std::string val = Get("non");
  ASSERT_TRUE(val.empty());
}

TEST(PlfsDirTest, PdbRw) {
  OpenWriter(DELTAFS_PLFSDIR_PLAINDB);
  Put("k1", "v1");
  IoWrite("a");
  Put("k2", "v2");
  IoWrite("b");
  Put("k3", "v3");
  IoWrite("c");
  Flush();
  Put("k4", "v4");
  IoWrite("x");
  Put("k5", "v5");
  IoWrite("y");
  Put("k6", "v6");
  IoWrite("z");
  FinishEpoch();
  Finish();
  OpenReader(DELTAFS_PLFSDIR_PLAINDB);
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
    deltafs_plfsdir_set_unordered(dir_, unordered_);
    deltafs_plfsdir_destroy(dir_, dirname_.c_str());
    ASSERT_TRUE(deltafs_plfsdir_open(dir_, dirname_.c_str()) == 0);
    ASSERT_TRUE(deltafs_plfsdir_io_open(dir_, dirname_.c_str()) == 0);
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

    int r = deltafs_plfsdir_epoch_flush(dir_, 0);
    ASSERT_TRUE(r == 0);
    r = deltafs_plfsdir_io_finish(dir_);
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

  uint32_t Get(uint32_t a) const {
    if (a < N) return rep_[a];
    return 0;
  }

  double Subtotal(uint32_t a) const {
    double subtotal = 0;
    for (uint32_t i = 0; i < N; i++) {
      if (i <= a) {
        subtotal += rep_[i];
      } else {
        break;
      }
    }
    return subtotal;
  }

  double CDF(uint32_t a) const {
    if (num_ == 0) return 0;
    return Subtotal(a) / num_;
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
  // The number of times we get i results (0 <= i <= N-1)
  uint32_t rep_[N];
  double sum_;
};

}  // namespace

template <typename FilterType, plfsio::FilterTester filter_tester,
          int N = 10240>
class PlfsFtBench {
 protected:
  static int FromEnv(const char* key, int def) {
    const char* env = getenv(key);
    if (env && env[0]) {
      return atoi(env);
    } else {
      return def;
    }
  }

  static inline int GetOptions(const char* key, int def) {
    int opt = FromEnv(key, def);
    fprintf(stderr, "%s=%d\n", key, opt);
    return opt;
  }

 public:
  PlfsFtBench() {
    // Force exact space match for the main cuckoo table
    options_.cuckoo_frac = -1;  // Auxiliary tables will be created on-demand
    options_.bf_bits_per_key = GetOptions("BF_BITS_PER_KEY", 20);
    kranks_ = GetOptions("KI_RANKS", 1);
    mkeys_ = GetOptions("MI_KEYS", 1);
    qstep_ = GetOptions("QUERY_STEP", (mkeys_ << 13));
    compression_ = GetOptions("SNAPPY", 0);
    dump_ = GetOptions("DUMP", 0);
  }

  ~PlfsFtBench() {
    if (ft_ != NULL) {
      delete ft_;
    }
  }

  void LogAndApply() {
    ft_ = new FilterType(options_, 0);  // Do not reserve memory for the filter
    ASSERT_TRUE(ft_ != NULL);
    const uint32_t num_keys = mkeys_ << 20;
    uint32_t num_ranks = kranks_ << 10;
    ft_->Reset(num_keys);
    char tmp[12];
    fprintf(stderr, "Populating filter data...\n");
    for (uint32_t k = 0; k < num_keys; k++) {
      if ((k & 0x7FFFFu) == 0) {
        fprintf(stderr, "\r%.2f%%", 100.0 * k / num_keys);
      }
      uint64_t h = xxhash64(&k, sizeof(k), 0);
      EncodeFixed64(tmp, h);
      uint32_t x = xxhash32(&h, sizeof(h), 301);
      uint32_t r = x % num_ranks;
      EncodeFixed32(tmp + 8, r);
      ft_->AddKey(Slice(tmp, sizeof(tmp)));
    }
    fprintf(stderr, "\r100.00%%");
    fprintf(stderr, "\n");

    ftdata_ = ft_->Finish();
    ASSERT_TRUE(!ftdata_.empty());
    if (compression_) {
      if (!port::Snappy_Compress(ftdata_.data(), ftdata_.size(),
                                 &compressed_)) {
        compressed_.clear();
      }
    }

    fprintf(stderr, "Done!\n");
    Query();
  }

  static inline bool KeyMatMatch(Slice key, Slice filter_data) {
    return filter_tester(key, filter_data);
  }

  void Query() {
    const uint32_t num_keys = mkeys_ << 20;
    uint32_t num_ranks = kranks_ << 10;
    char tmp[12];
    fprintf(stderr, "Querying...\n");
    const uint64_t start = Env::Default()->NowMicros();
    for (uint32_t k = 0; k < num_keys; k += qstep_) {
      if ((k & 0x7FFu) == 0) {
        fprintf(stderr, "\r%.2f%%", 100.0 * k / num_keys);
      }
      uint64_t h = xxhash64(&k, sizeof(k), 0);
      EncodeFixed64(tmp, h);
      uint32_t n = 0;
      for (uint32_t r = 0; r < num_ranks; r++) {
        EncodeFixed32(tmp + 8, r);
        if (KeyMatMatch(Slice(tmp, sizeof(tmp)), ftdata_)) {
          n++;
        }
      }
      histo_.Add(n);
    }
    const uint64_t end = Env::Default()->NowMicros();
    const uint64_t dura = end - start;
    fprintf(stderr, "\r100.00%%");
    fprintf(stderr, "\n");
    fprintf(stderr, "Done!\n");

    Report(dura);
  }

  int Report(uint64_t dura) {
    if (dump_) return Dump();
    const double k = 1000.0, ki = 1024.0;
    fprintf(stderr, "------------------------------------------------------\n");
    fprintf(stderr, "            Num Keys: %d Mi (%.3g%% victims)\n", mkeys_,
            100.0 * ft_->num_victims() / mkeys_ / ki / ki);
    fprintf(stderr, "               Ranks: %d Ki\n", kranks_);
    fprintf(stderr, "Filter Bytes Per Key: %.3f (%.3f after compression)\n",
            1.0 * ftdata_.size() / mkeys_ / ki / ki,
            1.0 * (compressed_.empty() ? ftdata_.size() : compressed_.size()) /
                mkeys_ / ki / ki);
    fprintf(stderr, "             Queries: %d\n", int(histo_.num_));
    fprintf(stderr, "        Total Q-time: %.3f ms\n", dura / k);
    fprintf(stderr, "               T-put: %.3g K queries/s\n",
            k * histo_.num_ / dura);
    assert(histo_.Average() >= 1.00);
    fprintf(stderr, "                  FP: %.4g%%\n",
            100.0 * (histo_.Average() - 1) / kranks_ / ki);
    fprintf(stderr, "    Avg Hits Per Key: %.3f\n", histo_.Average());
    fprintf(stderr, "            Max Hits: %d\n", int(histo_.max_));
    fprintf(stderr, "------------------------------------------------------\n");
    fprintf(stderr, "          CDF 1 Hits: %5.2f%% (%u)\n", histo_.CDF(1) * 100,
            histo_.Get(1));
    for (uint32_t i = 2; i <= N; i++) {
      double d = histo_.CDF(i);
      if (d > 0.0001 && d < 0.9999)
        fprintf(stderr, "           %4u Hits: %5.2f%% (%u)\n", i, d * 100,
                histo_.Get(i));
    }
    return 0;
  }

  int Dump() {  // TODO
    return 0;
  }

 protected:
  // Point to filter data once finished
  Slice ftdata_;
  Histo<N> histo_;
  plfsio::DirOptions options_;
  std::string compressed_;
  FilterType* ft_;
  int dump_;
  int compression_;
  int qstep_;   // So only a subset of keys are queried: [0, max_key, step]
  int kranks_;  // Total number of ranks in Thousands to emulate
  // Total number of keys (per rank) in Millions
  int mkeys_;
};

template <size_t K, int N = 10240>
class PlfsFtBenchKv
    : public PlfsFtBench<plfsio::CuckooBlock<K, 32>,
                         static_cast<plfsio::FilterTester>(NULL), N> {
 public:
  void LogAndApply() {
    plfsio::CuckooBlock<K, 32>* const ft =
        new plfsio::CuckooBlock<K, 32>(this->options_, 0);
    ASSERT_TRUE(ft != NULL);
    const uint32_t num_keys = this->mkeys_ << 20;
    uint32_t num_ranks = this->kranks_ << 10;
    ft->Reset(num_keys);
    char tmp[8];
    Slice key(tmp, sizeof(tmp));
    fprintf(stderr, "Populating filter data...\n");
    for (uint32_t k = 0; k < num_keys; k++) {
      if ((k & 0x7FFFFu) == 0) {
        fprintf(stderr, "\r%.2f%%", 100.0 * k / num_keys);
      }
      uint64_t h = xxhash64(&k, sizeof(k), 0);
      EncodeFixed64(tmp, h);
      uint32_t x = xxhash32(&h, sizeof(h), 301);
      uint32_t r = x % num_ranks;
      ft->AddKey(key, r);
    }
    fprintf(stderr, "\r100.00%%");
    fprintf(stderr, "\n");

    this->ftdata_ = ft->Finish();
    this->ft_ = ft;
    ASSERT_TRUE(!this->ftdata_.empty());
    if (this->compression_) {
      if (!port::Snappy_Compress(this->ftdata_.data(), this->ftdata_.size(),
                                 &this->compressed_)) {
        this->compressed_.clear();
      }
    }

    fprintf(stderr, "Done!\n");
    Query();
  }

  void Query() {
    Slice ftdata(this->ftdata_);
    const uint32_t num_keys = this->mkeys_ << 20;
    std::vector<uint32_t> ranks;
    char tmp[8];
    Slice key(tmp, sizeof(tmp));
    fprintf(stderr, "Querying...\n");
    const uint64_t start = Env::Default()->NowMicros();
    const uint32_t step = this->qstep_;
    for (uint32_t k = 0; k < num_keys; k += step) {
      if ((k & 0x7FFu) == 0) {
        fprintf(stderr, "\r%.2f%%", 100.0 * k / num_keys);
      }
      uint64_t h = xxhash64(&k, sizeof(k), 0);
      EncodeFixed64(tmp, h);
      plfsio::CuckooValues(key, ftdata, &ranks);
      this->histo_.Add(ranks.size());
      ranks.resize(0);
    }
    const uint64_t end = Env::Default()->NowMicros();
    const uint64_t dura = end - start;
    fprintf(stderr, "\r100.00%%");
    fprintf(stderr, "\n");
    fprintf(stderr, "Done!\n");

    this->Report(dura);
  }
};

}  // namespace pdlfs

#if defined(PDLFS_GFLAGS)
#include <gflags/gflags.h>
#endif
#if defined(PDLFS_GLOG)
#include <glog/logging.h>
#endif

static void BM_Usage() {
  fprintf(stderr, "Use --bench=[wisc, bf, cf[n], or kv[m]] to launch tests.\n");
  fprintf(stderr, "n = 8,16,24,32.\n");
  fprintf(stderr, "m = 1,2,4,8.\n");
  fprintf(stderr, "\n");
}

static void BM_LogAndApply(const char* bm) {
#define BF_BENCH \
  pdlfs::PlfsFtBench<pdlfs::plfsio::BloomBlock, pdlfs::plfsio::BloomKeyMayMatch>
#define CF_BENCH(n)                                    \
  pdlfs::PlfsFtBench<pdlfs::plfsio::CuckooBlock<n, 0>, \
                     pdlfs::plfsio::CuckooKeyMayMatch>
#define KV_BENCH(n) pdlfs::PlfsFtBenchKv<n>
  if (strcmp(bm, "wisc") == 0) {
    pdlfs::PlfsWiscBench bench;
    bench.LogAndApply();
  } else if (strcmp(bm, "bf") == 0) {
    BF_BENCH bench;
    bench.LogAndApply();
  } else if (strcmp(bm, "kv1") == 0) {
    KV_BENCH(1) bench;
    bench.LogAndApply();
  } else if (strcmp(bm, "kv2") == 0) {
    KV_BENCH(2) bench;
    bench.LogAndApply();
  } else if (strcmp(bm, "kv4") == 0) {
    KV_BENCH(4) bench;
    bench.LogAndApply();
  } else if (strcmp(bm, "kv8") == 0) {
    KV_BENCH(8) bench;
    bench.LogAndApply();
  } else if (strcmp(bm, "cf8") == 0) {
    CF_BENCH(8) bench;
    bench.LogAndApply();
  } else if (strcmp(bm, "cf16") == 0) {
    CF_BENCH(16) bench;
    bench.LogAndApply();
  } else if (strcmp(bm, "cf24") == 0) {
    CF_BENCH(24) bench;
    bench.LogAndApply();
  } else if (strcmp(bm, "cf32") == 0) {
    CF_BENCH(32) bench;
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
