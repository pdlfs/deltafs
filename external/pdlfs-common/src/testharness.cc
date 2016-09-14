#include "pdlfs-common/testharness.h"

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/pdlfs_config.h"
#if defined(GFLAGS)
#include <gflags/gflags.h>
#endif
#if defined(GLOG)
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
#if defined(GFLAGS)
  ::google::ParseCommandLineFlags(argc, argv, true);
#endif
  const char* matcher = getenv("TESTS");
#if defined(GLOG)
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
        if (strstr(name.c_str(), matcher) == NULL) {
          continue;
        }
      }
      fprintf(stderr, "==== Test %s.%s\n", t.base, t.name);
      (*t.func)();
      ++num;
    }
  }
  fprintf(stderr, "==== PASSED %d tests\n", num);
  return 0;
}

std::string TmpDir() {
  std::string dir;
  Status s = Env::Default()->GetTestDirectory(&dir);
  ASSERT_TRUE(s.ok());
  return dir;
}

int RandomSeed() {
  const char* env = getenv("TEST_RANDOM_SEED");
  int result = (env != NULL ? atoi(env) : 301);
  if (result <= 0) {
    result = 301;
  }
  return result;
}

std::string NewTmpDirectory(const Slice& subdir, Env* env) {
  if (env == NULL) env = Env::Default();
  std::string dirname = TmpDir() + "/" + subdir.data();
  env->CreateDir(dirname);

  std::vector<std::string> names;
  Status s = env->GetChildren(dirname, &names);
  if (s.ok()) {
    std::string fname = dirname;
    fname.push_back('/');
    size_t prefix = fname.length();
    for (size_t i = 0; i < names.size(); i++) {
      if (!Slice(names[i]).starts_with(".")) {
        fname.resize(prefix);
        fname.append(names[i]);
        s = env->DeleteFile(fname);
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
