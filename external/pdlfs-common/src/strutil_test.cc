/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
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

class StrUtilTest {};

TEST(StrUtilTest, ParseBool) {
  ASSERT_TRUE(ParsePrettyBool("y"));
  ASSERT_TRUE(ParsePrettyBool("yes"));
  ASSERT_TRUE(ParsePrettyBool("t"));
  ASSERT_TRUE(ParsePrettyBool("true"));
}

TEST(StrUtilTest, ParseNumber) {
  ASSERT_EQ(ParsePrettyNumber("4096"), 4096);
  ASSERT_EQ(ParsePrettyNumber("4k"), 4096);
}

TEST(StrUtilTest, Split) {
  std::vector<std::string> v;
  ASSERT_EQ(SplitString("a; b ;c", ';', &v), 3);
  ASSERT_EQ(v[0], "a");
  ASSERT_EQ(v[1], "b");
  ASSERT_EQ(v[2], "c");
  v.clear();
  ASSERT_EQ(SplitString("a , b,", ',', &v), 2);
  ASSERT_EQ(v[0], "a");
  ASSERT_EQ(v[1], "b");
  v.clear();
  ASSERT_EQ(SplitString("&a&&", '&', &v), 1);
  ASSERT_EQ(v[0], "a");
  v.clear();
  ASSERT_EQ(SplitString(" # ", '#', &v), 0);
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
