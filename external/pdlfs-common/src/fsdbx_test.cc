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

#include "pdlfs-common/fsdbx.h"
#include "pdlfs-common/testharness.h"

namespace pdlfs {

class DirIdTest {
  // Empty
};

TEST(DirIdTest, DirIdEnc1) {
  DirId id1(2);
  ASSERT_EQ(id1.ino, 2);
#if defined(DELTAFS_PROTO)
  DirId id2(3, 4);
  ASSERT_EQ(id2.dno, 3);
  ASSERT_EQ(id2.ino, 4);
#endif
}

TEST(DirIdTest, DirIdEnc2) {
  Stat stat1;
#if defined(DELTAFS_PROTO)
  stat1.SetDnodeNo(3);
#endif
  stat1.SetInodeNo(4);
  DirId id1(stat1);
#if defined(DELTAFS_PROTO)
  ASSERT_EQ(id1.dno, 3);
#endif
  ASSERT_EQ(id1.ino, 4);

#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
  LookupStat stat2;
#if defined(DELTAFS_PROTO)
  stat2.SetDnodeNo(3);
#endif
  stat2.SetInodeNo(4);
  DirId id2(stat2);
#if defined(DELTAFS_PROTO)
  ASSERT_EQ(id2.dno, 3);
#endif
  ASSERT_EQ(id2.ino, 4);
#endif
}

TEST(DirIdTest, DirIdEnc3) {
  DirId id1(2);
  DirId id2(2);
  DirId id3(3);
  DirId id4(4);
  ASSERT_TRUE(id1 == id2);
  ASSERT_TRUE(id1 != id3);
  ASSERT_TRUE(id4.compare(id3) > 0);
  ASSERT_TRUE(id3.compare(id4) < 0);
#if defined(DELTAFS_PROTO)
  DirId id5(3, 4);
  DirId id6(3, 4);
  DirId id7(3, 5);
  DirId id8(4, 5);
  ASSERT_TRUE(id5 == id6);
  ASSERT_TRUE(id5 != id7);
  ASSERT_TRUE(id7.compare(id6) > 0);
  ASSERT_TRUE(id6.compare(id7) < 0);
  ASSERT_TRUE(id8.compare(id7) > 0);
  ASSERT_TRUE(id7.compare(id8) < 0);
#endif
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
