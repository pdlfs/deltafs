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

#include "range_writer.h"
#include "types.h"

#include "pdlfs-common/env.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"
#include "pdlfs-common/xxhash.h"

#if __cplusplus >= 201103
#define OVERRIDE override
#else
#define OVERRIDE
#endif

namespace pdlfs {
namespace plfsio {

// Test Env ...
namespace {
// A file implementation that controls write speed and discards all data...
class EmulatedWritableFile : public WritableFileWrapper {
 public:
  explicit EmulatedWritableFile(uint64_t bps) : bytes_per_sec_(bps) {}

  virtual ~EmulatedWritableFile() {}

  virtual Status Append(const Slice& buf) OVERRIDE {
    if (!buf.empty()) {
      const int micros_to_delay =
          static_cast<int>(1000 * 1000 * buf.size() / bytes_per_sec_);
      SleepForMicroseconds(micros_to_delay);
    }
    return Status::OK();
  }

 private:
  const uint64_t bytes_per_sec_;
};

// All writable files are individually rate limited.
class EmulatedEnv : public EnvWrapper {
 public:
  explicit EmulatedEnv(uint64_t bps)
      : EnvWrapper(Env::Default()), bytes_per_sec_(bps) {}

  virtual ~EmulatedEnv() {}

  virtual Status NewWritableFile(const char* f, WritableFile** r) OVERRIDE {
    *r = new EmulatedWritableFile(bytes_per_sec_);
    return Status::OK();
  }

 private:
  const uint64_t bytes_per_sec_;
};

}  // namespace

// Measure implementation's bandwidth utilization under
// different configurations.
class RangeWriterBench {
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

  static inline float randf(float scale) { return rand() * scale / RAND_MAX; }

  static inline bool multipleof(size_t x, size_t factor) {
    return ((x && factor) && !(x % factor));
  }

 public:
  RangeWriterBench() {
    mkeys_ = GetOption("MI_KEYS", 4);
    bytes_per_sec_ = GetOption("BYTES_PER_SEC", 6000000);
    buf_size_ = GetOption("BUF_SIZE", 4 << 20);
    n_ = GetOption("NUM_BUFS", 4);
    int single_threaded = GetOption("SINGLE_THREADED", 0);
    epoch_flshcnt_ = GetOption("EPOCH_FLUSH_COUNT", 0);
    mid_epoch_flshcnt_ = GetOption("MID_EPOCH_FLUSH_COUNT", 0);
    thread_pool_ = single_threaded
                       ? nullptr
                       : ThreadPool::NewFixed(n_, true /* eager init */);
    skip_sort_ = GetOption("SKIP_SORT", 0);

    options_.bf_bits_per_key = 0;
    options_.compaction_pool = thread_pool_;
    options_.cuckoo_frac = -1;
    options_.skip_sort = skip_sort_ ? true : false;
  }

  ~RangeWriterBench() {  //
    if (thread_pool_) delete thread_pool_;
  }

  void LogAndApply() {
    RangeWriter* rdb = NULL;
    WritableFile* dst = NULL;

    Env* const env = new EmulatedEnv(bytes_per_sec_);
    ASSERT_OK(env->NewWritableFile("test.tbl", &dst));
    options_.allow_env_threads = false;
    options_.key_size = 4;
    options_.value_size = 56;
    options_.env = env;
    rdb = new RangeWriter(options_, dst, 1, 2, buf_size_);
    const uint64_t start = CurrentMicros();
    char tmp[4];
    Slice key(tmp, sizeof(tmp));
    std::string val(options_.value_size, '\0');
    const size_t num_keys = static_cast<size_t>(mkeys_) << 20;

    size_t epoch_size = epoch_flshcnt_ ? num_keys / epoch_flshcnt_ : 0;
    size_t flush_size = mid_epoch_flshcnt_
                            ? num_keys / (epoch_flshcnt_ * mid_epoch_flshcnt_)
                            : 0;

    size_t i = 0;
    for (; i < num_keys; i++) {
      if ((i & 0x7FFFu) == 0) fprintf(stderr, "\r%.2f%%", 100.0 * i / num_keys);

      float f = randf(1000);
      memcpy(tmp, reinterpret_cast<char*>(&f), sizeof(f));

      if (multipleof(i, flush_size)) {
        float rand_start = randf(100);
        float rand_end = randf(1000) + rand_start;
        printf("\nRange update: %f %f\n", rand_start, rand_end);
        rdb->UpdateBounds(rand_start, rand_end);
      }

      if (multipleof(i, epoch_size)) {
        printf("==========NEW EPOCH=========\n");
        rdb->EpochFlush();
      }

      ASSERT_OK(rdb->Add(key, val));
    }

    fprintf(stderr, "\r100.00%%");
    fprintf(stderr, "\n");
    ASSERT_OK(rdb->Finish());
    uint64_t dura = CurrentMicros() - start;
    Report(dura);

    dst->Close();

    delete rdb;
    delete dst;
    delete env;
  }

  void Report(uint64_t dura) {
    const double k = 1000.0, ki = 1024.0;
    const double d = options_.key_size + options_.value_size +
                     double(options_.bf_bits_per_key) / 8;
    fprintf(stderr, "-----------------------------------------\n");
    fprintf(stderr, "     Total dura: %.0f sec\n", 1.0 * dura / k / k);
    fprintf(stderr, "          Speed: %.0f bytes per sec\n",
            d * mkeys_ * ki * ki * k * k / dura);
    fprintf(stderr, "           Util: %.2f%%\n",
            100 * d * mkeys_ * ki * ki * k * k / dura / bytes_per_sec_);
  }

 private:
  ThreadPool* thread_pool_;
  DirOptions options_;
  uint64_t bytes_per_sec_;
  size_t buf_size_;
  size_t n_;
  int mkeys_;
  size_t epoch_flshcnt_;
  size_t mid_epoch_flshcnt_;
  int skip_sort_;
};

}  // namespace plfsio
}  // namespace pdlfs

#if defined(PDLFS_GFLAGS)
#include <gflags/gflags.h>
#endif
#if defined(PDLFS_GLOG)
#include <glog/logging.h>
#endif

namespace {
void BM_Usage() {
  fprintf(stderr, "Use --bench=rdb to run benchmark.\n");
  fprintf(stderr, "\n");
}

void BM_Main(int* argc, char*** argv) {
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
  if (bench_name == "--bench=rdb") {
    pdlfs::plfsio::RangeWriterBench bench;
    bench.LogAndApply();
  } else {
    BM_Usage();
  }
}
}  // namespace

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
