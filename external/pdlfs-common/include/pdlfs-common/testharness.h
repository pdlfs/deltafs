#pragma once

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdio.h>
#include <stdlib.h>
#include <sstream>

#include "pdlfs-common/env.h"
#include "pdlfs-common/random.h"

namespace pdlfs {
namespace test {

// Run some of the tests registered by the TEST() macro.  If the
// environment variable "TESTS" is not set, runs all tests.
// Otherwise, runs only the tests whose name contains the value of
// "TESTS" as a substring.  E.g., suppose the tests are:
//    TEST(Foo, Hello) { ... }
//    TEST(Foo, World) { ... }
// TESTS=Hello will run the first test
// TESTS=o     will run both tests
// TESTS=Junk  will run no tests
//
// Returns 0 if all tests pass.
// Dies or returns a non-zero value if some test fails.
extern int RunAllTests(int* argc, char*** argv);

// Return the directory to use for temporary storage.
extern std::string TmpDir();

// Create a sub-directory under test::TmpDir().
// If the sub-directory already exists, remove all its existing children.
// Return the full path of this sub-directory.
extern std::string NewTmpDirectory(const Slice& subdir, Env* env = NULL);

// Return a randomization seed for this run.  Typically returns the
// same number on repeated invocations of this binary, but automated
// runs may be able to vary the seed.
extern int RandomSeed();

// An instance of Tester is allocated to hold temporary state during
// the execution of an assertion.
class Tester {
 private:
  bool ok_;
  const char* fname_;
  int line_;
  std::stringstream ss_;

 public:
  Tester(const char* f, int l) : ok_(true), fname_(f), line_(l) {}

  ~Tester() {
    if (!ok_) {
      fprintf(stderr, "%s:%d:%s\n", fname_, line_, ss_.str().c_str());
      exit(1);
    }
  }

  Tester& Is(bool b, const char* msg) {
    if (!b) {
      ss_ << " Assertion failure: " << msg;
      ok_ = false;
    }
    return *this;
  }

  Tester& IsNot(bool b, const char* msg) {
    if (b) {
      ss_ << " Error condition is true: " << msg;
      ok_ = false;
    }
    return *this;
  }

  Tester& IsOk(const Status& s) {
    if (!s.ok()) {
      ss_ << " " << s.ToString();
      ok_ = false;
    }
    return *this;
  }

  Tester& IsErr(const Status& s) {
    if (s.ok()) {
      ss_ << " " << s.ToString();
      ok_ = false;
    }
    return *this;
  }

  Tester& IsNotFound(const Status& s) {
    if (!s.IsNotFound()) {
      ss_ << " " << s.ToString();
      ok_ = false;
    }
    return *this;
  }

  Tester& IsAlreadyExists(const Status& s) {
    if (!s.IsAlreadyExists()) {
      ss_ << " " << s.ToString();
      ok_ = false;
    }
    return *this;
  }

#define BINARY_OP(name, op)                          \
  template <class X, class Y>                        \
  Tester& name(const X& x, const Y& y) {             \
    if (!(x op y)) {                                 \
      ss_ << " failed: " << x << (" " #op " ") << y; \
      ok_ = false;                                   \
    }                                                \
    return *this;                                    \
  }

  BINARY_OP(IsEq, ==)
  BINARY_OP(IsNe, !=)
  BINARY_OP(IsGe, >=)
  BINARY_OP(IsGt, >)
  BINARY_OP(IsLe, <=)
  BINARY_OP(IsLt, <)
#undef BINARY_OP

  // Attach the specified value to the error message if an error has occurred
  template <class V>
  Tester& operator<<(const V& value) {
    if (!ok_) {
      ss_ << " " << value;
    }
    return *this;
  }
};

#define ASSERT_TRUE(c) ::pdlfs::test::Tester(__FILE__, __LINE__).Is((c), #c)
#define ASSERT_FALSE(c) ::pdlfs::test::Tester(__FILE__, __LINE__).IsNot((c), #c)
#define ASSERT_OK(s) ::pdlfs::test::Tester(__FILE__, __LINE__).IsOk((s))
#define ASSERT_ERR(s) ::pdlfs::test::Tester(__FILE__, __LINE__).IsErr((s))
#define ASSERT_NOTFOUND(s) \
  ::pdlfs::test::Tester(__FILE__, __LINE__).IsNotFound((s))
#define ASSERT_CONFLICT(s) \
  ::pdlfs::test::Tester(__FILE__, __LINE__).IsAlreadyExists((s))

#define ASSERT_EQ(a, b) ::pdlfs::test::Tester(__FILE__, __LINE__).IsEq((a), (b))
#define ASSERT_NE(a, b) ::pdlfs::test::Tester(__FILE__, __LINE__).IsNe((a), (b))
#define ASSERT_GE(a, b) ::pdlfs::test::Tester(__FILE__, __LINE__).IsGe((a), (b))
#define ASSERT_GT(a, b) ::pdlfs::test::Tester(__FILE__, __LINE__).IsGt((a), (b))
#define ASSERT_LE(a, b) ::pdlfs::test::Tester(__FILE__, __LINE__).IsLe((a), (b))
#define ASSERT_LT(a, b) ::pdlfs::test::Tester(__FILE__, __LINE__).IsLt((a), (b))

inline bool Between(uint64_t val, uint64_t low, uint64_t high) {
  bool r = (val >= low) && (val <= high);
  if (!r) {
    fprintf(stderr, "Value %llu is not in range [%llu, %llu]\n",
            static_cast<unsigned long long>(val),
            static_cast<unsigned long long>(low),
            static_cast<unsigned long long>(high));
  }
  return r;
}

#define BETWEEN(a, b, c) ::pdlfs::test::Between(a, b, c)

#define TCONCAT(a, b) TCONCAT1(a, b)
#define TCONCAT1(a, b) a##b
#define TEST(base, name)                                            \
  class TCONCAT(_Test_, name) : public base {                       \
   public:                                                          \
    void _Run();                                                    \
    static void _RunIt() {                                          \
      TCONCAT(_Test_, name) t;                                      \
      t._Run();                                                     \
    }                                                               \
  };                                                                \
  bool TCONCAT(_Test_ignored_, name) = ::pdlfs::test::RegisterTest( \
      #base, #name, &TCONCAT(_Test_, name)::_RunIt);                \
  void TCONCAT(_Test_, name)::_Run()

// Register the specified test.  Typically not used directly, but
// invoked via the macro expansion of TEST.
extern bool RegisterTest(const char* base, const char* name, void (*func)());

}  // namespace test
}  // namespace pdlfs
