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

#include "range_reader.h"
#include "range_writer.h"
#include "types.h"

#include "pdlfs-common/env.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"
#include "pdlfs-common/xxhash.h"

#include <sys/stat.h>

#if __cplusplus >= 201103
#define OVERRIDE override
#else
#define OVERRIDE
#endif

namespace pdlfs {
namespace plfsio{
  void gen() {

  }
  void test() {
    DirOptions options;
    RandomAccessFile* src;
    Env* env = port::PosixGetDefaultEnv();
    const char* dpath = "/Users/schwifty/Repos/workloads/rdb/tables";
    options.env = env;
    RangeReader rr(options);
    rr.Read(dpath);
    rr.Query(0.041, 0.042);
  }
}
}

int main(int argc, char* argv[]) {
  printf("Hello World!\n");
  pdlfs::plfsio::test();
//  pdlfs::Slice token;
//  if (argc > 1) {
//    token = pdlfs::Slice(argv[argc - 1]);
//  }
//  if (!token.starts_with("--bench")) {
//    return pdlfs::test::RunAllTests(&argc, &argv);
//  } else {
//    BM_Main(&argc, &argv);
//    return 0;
//  }
}
