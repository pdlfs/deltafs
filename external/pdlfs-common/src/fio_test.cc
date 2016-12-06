/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/fio.h"
#include "pdlfs-common/testharness.h"

namespace pdlfs {

class FioTest {};

TEST(FioTest, EncodeDecode) {
  Fentry entry1;
  Fentry entry2;
  entry1.pid = DirId(1, 2, 3);
  entry1.nhash = "xxx";
  entry1.zserver = 4;
  Stat* stat1 = &entry1.stat;
  stat1->SetInodeNo(4);
  stat1->SetSnapId(5);
  stat1->SetRegId(6);
  stat1->SetFileMode(7);
  stat1->SetUserId(8);
  stat1->SetGroupId(9);
  stat1->SetZerothServer(10);
  char tmp1[200];
  Slice encoding1 = entry1.EncodeTo(tmp1);
  Slice input = encoding1;
  bool ok = entry2.DecodeFrom(&input);
  ASSERT_TRUE(ok);
  char tmp2[200];
  Slice encoding2 = entry2.EncodeTo(tmp2);
  ASSERT_EQ(encoding1, encoding2);
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
