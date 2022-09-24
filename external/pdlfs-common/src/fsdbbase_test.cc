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

#include "pdlfs-common/fsdbbase.h"
#include "pdlfs-common/testharness.h"

namespace pdlfs {

class KeyTest {
  // Empty.
};

TEST(KeyTest, KeyEnc1) {
  Key k1(7, static_cast<KeyType>(1));
  ASSERT_EQ(k1.inode(), 7);
  ASSERT_EQ(int(k1.type()), 1);
#if defined(DELTAFS_PROTO)
  Key k2(9, 7, static_cast<KeyType>(2));
  ASSERT_EQ(k2.dnode(), 9);
  ASSERT_EQ(k2.inode(), 7);
  ASSERT_EQ(int(k2.type()), 2);
#endif
#if defined(DELTAFS)
  Key k2(29, 12345, static_cast<KeyType>(127));
  ASSERT_EQ(k2.snap_id(), 29);
  ASSERT_EQ(k2.inode(), 12345);
  ASSERT_EQ(int(k2.type()), 127);
  Key k3(13, 37, 67890, static_cast<KeyType>(255));
  ASSERT_EQ(k3.reg_id(), 13);
  ASSERT_EQ(k3.snap_id(), 37);
  ASSERT_EQ(k3.inode(), 67890);
  ASSERT_EQ(int(k3.type()), 255);
#endif
}

TEST(KeyTest, KeyEnc2) {
  Stat stat1;
#if defined(DELTAFS_PROTO)
  stat1.SetDnodeNo(2);
#endif
  stat1.SetInodeNo(3);
  Key k1(stat1, static_cast<KeyType>(5));
#if defined(DELTAFS_PROTO)
  ASSERT_EQ(k1.dnode(), 2);
#endif
  ASSERT_EQ(k1.inode(), 3);

#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
  LookupStat stat2;
#if defined(DELTAFS_PROTO)
  stat2.SetDnodeNo(2);
#endif
  stat2.SetInodeNo(3);
  Key k2(stat2, static_cast<KeyType>(5));
#if defined(DELTAFS_PROTO)
  ASSERT_EQ(k2.dnode(), 2);
#endif
  ASSERT_EQ(k2.inode(), 3);
#endif
}

TEST(KeyTest, KeyEnc3) {
  char zero[50];
  memset(zero, 0, sizeof(zero));
  Key k1(0, static_cast<KeyType>(0));
  k1.SetName(Slice());
  ASSERT_EQ(k1.Encode(), Slice(zero, k1.size()));
  Key k2(1, static_cast<KeyType>(1));
  k2.SetName(Slice("X"));
#if !defined(DELTAFS) && !defined(INDEXFS)
  ASSERT_EQ(k2.suffix(), Slice("X"));
#endif
  Key k3(1, static_cast<KeyType>(1));
  k3.SetName(Slice("Y"));
#if !defined(DELTAFS) && !defined(INDEXFS)
  ASSERT_EQ(k3.suffix(), Slice("Y"));
#endif
  ASSERT_EQ(k2.prefix(), k3.prefix());
}

TEST(KeyTest, KeyEnc4) {
  Key k1(99, static_cast<KeyType>(99));
  k1.SetName(Slice("X"));
  Key k2(k1);
  k2.SetName(Slice("YY"));
  Key k3(k2);
  k3.SetName(Slice("X"));
  ASSERT_EQ(k3.Encode(), k1.Encode());
  Key k4 = k2;
  k4.SetSuffix(k1.suffix());
  ASSERT_EQ(k4.Encode(), k1.Encode());
#if defined(DELTAFS) || defined(INDEXFS)
  Key k5 = k2;
  k5.SetHash(k1.hash());
  ASSERT_EQ(k5.Encode(), k1.Encode());
#endif
}

TEST(KeyTest, KeyEnc5) {
  Key k1(31, kDataBlockType);
  Key k2(31, kDataBlockType);
  Key k3(31, kDataBlockType);
  k1.SetOffset(0);
  k2.SetOffset(32);
  k3.SetOffset(128);
  ASSERT_LT(k1.Encode(), k2.Encode());
  ASSERT_LT(k2.Encode(), k3.Encode());
  ASSERT_EQ(k1.offset(), 0);
  ASSERT_EQ(k2.offset(), 32);
  ASSERT_EQ(k3.offset(), 128);
}

TEST(KeyTest, KeyEnc6) {
  Key k1(31, kDataDesType);
  Key k2(k1.prefix());
  ASSERT_EQ(k1.prefix(), k2.prefix());
  Key k3(31, kDataBlockType);
  k3.SetType(kDataDesType);
  ASSERT_EQ(k1.prefix(), k3.prefix());
}

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
