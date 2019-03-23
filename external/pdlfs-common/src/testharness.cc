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

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/pdlfs_config.h"
#if defined(PDLFS_GFLAGS)
#include <gflags/gflags.h>
#endif
#if defined(PDLFS_GLOG)
#include <glog/logging.h>
#endif

#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string>

namespace pdlfs {
namespace test {

struct Test {
  const char* base;
  const char* name;
  void (*func)();
};

static std::vector<Test>* tests;

bool RegisterTest(const char* base, const char* name, void (*func)()) {
  if (tests == NULL) {
    tests = new std::vector<Test>;
  }
  Test t;
  t.base = base;
  t.name = name;
  t.func = func;
  tests->push_back(t);
  return true;
}

int RunAllTests(int* argc, char*** argv) {
#if defined(PDLFS_GFLAGS)
  ::google::SetUsageMessage(
      "command line arguments are initially absorbed by gflags; "
      "to pass additional arguments to the actual test program, add '--' "
      "and put those additional arguments after it.");
  ::google::SetVersionString(PDLFS_COMMON_VERSION);
  ::google::ParseCommandLineFlags(argc, argv, true);
#endif
  const char* matcher = getenv("PDLFS_TESTS");
  if (matcher == NULL) {
    matcher = getenv("TESTS");
  }
#if defined(PDLFS_GLOG)
  ::google::InitGoogleLogging((*argv)[0]);
  ::google::InstallFailureSignalHandler();
#endif
  int num = 0;
  if (tests != NULL) {
    for (size_t i = 0; i < tests->size(); i++) {
      const Test& t = (*tests)[i];
      if (matcher != NULL) {
        std::string name = t.base;
        name.push_back('.');
        name.append(t.name);
        if (matcher[0] != '~') {  // only run tests that matches
          if (strstr(name.c_str(), matcher) == NULL) {
            continue;
          }
        } else {  // skip tests that matches
          if (strstr(name.c_str(), matcher + 1) != NULL) {
            continue;
          }
        }
      }
      fprintf(stderr, "==== Test %s.%s\n", t.base, t.name);
      (*t.func)();
      ++num;
    }
  }
  fprintf(stderr, "==== PASSED %d tests\n", num);
#if defined(PDLFS_GLOG)
  ::google::ShutdownGoogleLogging();
#endif
  return 0;
}

std::string TmpDir() {
  std::string dir;
  Status s = Env::Default()->GetTestDirectory(&dir);
  ASSERT_TRUE(s.ok());
  return dir;
}

int RandomSeed() {
  const char* seed = getenv("PDLFS_TEST_RANDOM_SEED");
  if (seed == NULL) {
    seed = getenv("TEST_RANDOM_SEED");
  }
  int result = (seed != NULL ? atoi(seed) : 301);
  if (result <= 0) {
    result = 301;
  }
  return result;
}

std::string PrepareTmpDir(const char* subdir, Env* env) {
  if (env == NULL) env = Env::Default();
  const std::string dirname = TmpDir() + "/" + subdir;
  env->CreateDir(dirname.c_str());
  std::vector<std::string> names;
  Status s = env->GetChildren(dirname.c_str(), &names);
  if (s.ok()) {
    std::string fname = dirname;
    fname.push_back('/');
    const size_t prefix = fname.length();
    for (size_t i = 0; i < names.size(); i++) {
      if (!Slice(names[i]).starts_with(".")) {
        fname.resize(prefix);
        fname.append(names[i]);
        s = env->DeleteFile(fname.c_str());
        if (!s.ok()) {
          break;
        }
      }
    }
  }
  ASSERT_OK(s);
  return dirname;
}

}  // namespace test
}  // namespace pdlfs
