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
  entry1.nhash = "xyz";
  entry1.zserver = 4;
  Stat* stat1 = &entry1.stat;
  stat1->SetInodeNo(5);
  stat1->SetSnapId(6);
  stat1->SetRegId(7);
  stat1->SetFileSize(8);
  stat1->SetFileMode(9);
  stat1->SetUserId(10);
  stat1->SetGroupId(11);
  stat1->SetZerothServer(12);
  stat1->SetChangeTime(13);
  stat1->SetModifyTime(14);
  char tmp1[DELTAFS_FENTRY_BUFSIZE];
  Slice encoding1 = entry1.EncodeTo(tmp1);
  Slice input = encoding1;
  bool ok = entry2.DecodeFrom(&input);
  ASSERT_TRUE(ok);
  char tmp2[DELTAFS_FENTRY_BUFSIZE];
  Slice encoding2 = entry2.EncodeTo(tmp2);
  ASSERT_EQ(encoding1, encoding2);
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
