/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "bbos_env.h"

#include "pdlfs-common/strutil.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

#include <stdarg.h>
#include <stdlib.h>
static const char* argv0;  // Program name

namespace pdlfs {
namespace bbos {

static void va_log(int err, const char* fmt, va_list va) {
  if (err) fprintf(stderr, "ERROR: ");
  fprintf(stderr, "%s: ", argv0);
  vfprintf(stderr, fmt, va);
  fprintf(stderr, "\n");
  if (err) exit(1);
}

// Print an error message and kill the run.
static void FATAL(const char* fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  va_log(1, fmt, ap);
  va_end(ap);
}

// Print a notice message.
static void NOTICE(const char* fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  va_log(0, fmt, ap);
  va_end(ap);
}

// Retrieve a string-based env variable or return a specified default.
static const char* GetBbosOption(const char* key, const char* def) {
  const char* env = getenv(key);
  if (env == NULL) {
    return def;
  } else if (env[0] == 0) {
    return def;
  } else {
    return env;
  }
}

// Retrieve a int-based env variable or return a specified default.
static uint64_t GetBbosIntOption(const char* key, uint64_t def) {
  uint64_t result;
  char tmp[100];
  snprintf(tmp, sizeof(tmp), "%llu", static_cast<unsigned long long>(def));
  const char* opt = GetBbosOption(key, tmp);
  if (ParsePrettyNumber(opt, &result)) {
    return result;
  } else {
    return def;
  }
}

// A simple benchmark for bbos
class BbosBench {
 public:
  BbosBench();
  ~BbosBench() { delete env_; }
  void LogAndApply();

  const char* hg_lo_;
  const char* hg_remote_;
  Env* base_env_;
  Env* env_;
};

BbosBench::BbosBench() : base_env_(NULL), env_(NULL) {
  base_env_ = Env::Default();
  hg_lo_ = GetBbosOption("BBOS_HG_LOCAL", "bmi+tcp");
  hg_remote_ = GetBbosOption("BBOS_HG_REMOTE", "bmi+tcp://127.0.0.1:12345");
  NOTICE("creating bbos env (hg_local=%s, hg_remote=%s) ...", hg_lo_,
         hg_remote_);
  Status s = BbosInit(&env_, hg_lo_, hg_remote_, NULL, NULL);
  ASSERT_OK(s) << "\n  > cannot init bbos";
  ASSERT_TRUE(env_ != NULL) << "\n  > bbos handle is null";
  NOTICE("creating ok!");
}

void BbosBench::LogAndApply() {
  Random rnd(301);
  uint64_t chuck_size;
  uint64_t num_chunks;
  const std::string prefix = "bbos_bench_obj";
  WritableFile* data_sink;
  Status s;

  chuck_size = GetBbosIntOption("BBOS_CHUCK_SIZE", 2 << 20);
  num_chunks = GetBbosIntOption("BBOS_NUM_CHUNKS", 8);
  NOTICE("creating bbos object: %s.dat ...", prefix.c_str());
  s = env_->NewWritableFile(std::string(prefix + ".dat").c_str(), &data_sink);
  ASSERT_OK(s) << "\n  > cannot create writable non-index object";
  NOTICE("creating ok!");

  std::string buffer;
  test::RandomString(&rnd, int(chuck_size), &buffer);
  NOTICE("writing data into bbos object: %s.dat ...", prefix.c_str());
  for (int i = 0; i < int(num_chunks); i++) {
    NOTICE("chuck %d ...", i);
    s = data_sink->Append(buffer);
    ASSERT_OK(s) << "\n  > cannot write into object";
    s = data_sink->Flush();
    ASSERT_OK(s) << "\n  > cannot flush object";
    NOTICE("ok!");
  }
  data_sink->Close();
  delete data_sink;
}

}  // namespace bbos
}  // namespace pdlfs

#if defined(PDLFS_GFLAGS)
#include <gflags/gflags.h>
#endif
#if defined(PDLFS_GLOG)
#include <glog/logging.h>
#endif

static void BM_Main(int* argc, char*** argv) {
  argv0 = (*argv)[0];
#if defined(PDLFS_GFLAGS)
  google::ParseCommandLineFlags(argc, argv, true);
#endif
#if defined(PDLFS_GLOG)
  google::InitGoogleLogging((*argv)[0]);
  google::InstallFailureSignalHandler();
#endif
  pdlfs::bbos::BbosBench bench;
  bench.LogAndApply();
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
