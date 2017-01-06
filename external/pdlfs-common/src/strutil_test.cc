/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/strutil.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {

class StrUtilTest {
 public:
  static std::string ToBool(const Slice& v) {
    bool b;
    if (ParsePrettyBool(v, &b)) {
      return b ? "True" : "False";
    } else {
      return "Unknown";
    }
  }
  static std::string ToInt(const Slice& v) {
    uint64_t i;
    if (ParsePrettyNumber(v, &i)) {
      char tmp[30];
      snprintf(tmp, sizeof(tmp), "%llu", (long long unsigned)i);
      return tmp;
    } else {
      return "Unknown";
    }
  }
};

TEST(StrUtilTest, ParseBool) {
  ASSERT_EQ(ToBool("y"), "True");
  ASSERT_EQ(ToBool("yes"), "True");
  ASSERT_EQ(ToBool("enabled"), "True");
  ASSERT_EQ(ToBool("true"), "True");
  ASSERT_EQ(ToBool("n"), "False");
  ASSERT_EQ(ToBool("no"), "False");
  ASSERT_EQ(ToBool("disabled"), "False");
  ASSERT_EQ(ToBool("false"), "False");
  ASSERT_EQ(ToBool(""), "Unknown");
  ASSERT_EQ(ToBool("ok"), "Unknown");
  ASSERT_EQ(ToBool("default"), "Unknown");
  ASSERT_EQ(ToBool("1"), "Unknown");
}

TEST(StrUtilTest, ParseNumber) {
  ASSERT_EQ(ToInt("1m"), "1048576");
  ASSERT_EQ(ToInt("4k"), "4096");
  ASSERT_EQ(ToInt("256"), "256");
  ASSERT_EQ(ToInt("0"), "0");
  ASSERT_EQ(ToInt("-20"), "Unknown");
  ASSERT_EQ(ToInt("0.3"), "Unknown");
  ASSERT_EQ(ToInt("23p"), "Unknown");
}

TEST(StrUtilTest, Split) {
  std::vector<std::string> v;
  ASSERT_EQ(SplitString(&v, "a; b ;c", ';'), 3);
  ASSERT_EQ(v[0], "a");
  ASSERT_EQ(v[1], "b");
  ASSERT_EQ(v[2], "c");
  v.clear();
  ASSERT_EQ(SplitString(&v, " a , b,", ','), 2);
  ASSERT_EQ(v[0], "a");
  ASSERT_EQ(v[1], "b");
  v.clear();
  ASSERT_EQ(SplitString(&v, "&&& a &", '&'), 1);
  ASSERT_EQ(v[0], "a");
  v.clear();
  ASSERT_EQ(SplitString(&v, "  # ", '#'), 0);
  ASSERT_EQ(SplitString(&v, " ## ", '#'), 0);
  ASSERT_EQ(SplitString(&v, "#  #", '#'), 0);
}

TEST(StrUtilTest, SplitN) {
  std::vector<std::string> v;
  ASSERT_EQ(SplitString(&v, "a; b ;c", ';', 2), 3);
  ASSERT_EQ(v[0], "a");
  ASSERT_EQ(v[1], "b");
  ASSERT_EQ(v[2], "c");
  v.clear();
  ASSERT_EQ(SplitString(&v, " a , b,", ',', 1), 2);
  ASSERT_EQ(v[0], "a");
  ASSERT_EQ(v[1], "b,");
  v.clear();
  ASSERT_EQ(SplitString(&v, "&&& a &", '&', 0), 1);
  ASSERT_EQ(v[0], "&&& a &");
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
