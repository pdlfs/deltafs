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

#include "pdlfs-common/fstypes.h"
#include "pdlfs-common/testharness.h"

namespace pdlfs {

class KeyTest {
  // Empty.
};

TEST(KeyTest, KeyEnc1) {
  Key k1(7, static_cast<KeyType>(1));
  ASSERT_EQ(k1.inode(), 7);
  ASSERT_EQ(int(k1.type()), 1);
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
  char zero[50];
  memset(zero, 0, sizeof(zero));
  Key k1(0, static_cast<KeyType>(0));
  k1.SetName(Slice());
  ASSERT_EQ(k1.Encode(), Slice(zero, k1.size()));
  Key k2(1, static_cast<KeyType>(1));
  k2.SetName(Slice("X"));
  Key k3(1, static_cast<KeyType>(1));
  k3.SetName(Slice("Y"));
  ASSERT_EQ(k2.prefix(), k3.prefix());
  ASSERT_NE(k2.Encode(), k3.Encode());
}

TEST(KeyTest, KeyEnc3) {
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

TEST(KeyTest, KeyEnc4) {
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

TEST(KeyTest, KeyEnc5) {
  Key k1(31, kDataDesType);
  Key k2(k1.prefix());
  ASSERT_EQ(k1.prefix(), k2.prefix());
  Key k3(31, kDataBlockType);
  k3.SetType(kDataDesType);
  ASSERT_EQ(k1.prefix(), k3.prefix());
}

class StatTest {
  // Empty.
};

TEST(StatTest, StatEncoding) {
  Stat stat;
#if defined(DELTAFS)
  stat.SetRegId(13);
  stat.SetSnapId(37);
#endif
  stat.SetInodeNo(12345);
  stat.SetFileSize(90);
  stat.SetFileMode(678);
#if defined(DELTAFS) || defined(INDEXFS)
  stat.SetZerothServer(777);
#endif
  stat.SetUserId(11);
  stat.SetGroupId(22);
  stat.SetModifyTime(44332211);
  stat.SetChangeTime(11223344);
  stat.AssertAllSet();
  char tmp[100];
  ASSERT_TRUE(sizeof(tmp) >= Stat::kMaxEncodedLength);
  Slice encoding = stat.EncodeTo(tmp);
  Stat stat2;
  bool r = stat2.DecodeFrom(encoding);
  ASSERT_TRUE(r);
  char tmp2[sizeof(tmp)];
  Slice encoding2 = stat2.EncodeTo(tmp2);
  ASSERT_EQ(encoding, encoding2);
}

#if defined(DELTAFS) || defined(INDEXFS)
class LookupEntryTest {
  // Empty
};

TEST(LookupEntryTest, EntryEncoding) {
  LookupStat stat;
#if defined(DELTAFS)
  stat.SetRegId(13);
  stat.SetSnapId(37);
#endif
  stat.SetInodeNo(12345);
  stat.SetDirMode(678);
  stat.SetZerothServer(777);
  stat.SetUserId(11);
  stat.SetGroupId(22);
  stat.SetLeaseDue(55667788);
  stat.AssertAllSet();
  char tmp[100];
  ASSERT_TRUE(sizeof(tmp) >= LookupStat::kMaxEncodedLength);
  Slice encoding = stat.EncodeTo(tmp);
  LookupStat stat2;
  bool r = stat2.DecodeFrom(encoding);
  ASSERT_TRUE(r);
  char tmp2[sizeof(tmp)];
  Slice encoding2 = stat2.EncodeTo(tmp2);
  ASSERT_EQ(encoding, encoding2);
}

#endif

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
