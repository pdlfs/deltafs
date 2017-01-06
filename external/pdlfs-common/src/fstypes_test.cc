/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/fstypes.h"
#include "pdlfs-common/testharness.h"

namespace pdlfs {

class KeyTest {};

TEST(KeyTest, KeyEncoding) {
  {
    Key k1(0, static_cast<KeyType>(1));
    Key k2(29, 12345, static_cast<KeyType>(127));
    Key k3(13, 37, 67890, static_cast<KeyType>(255));
    ASSERT_EQ(k1.inode(), 0);
    ASSERT_EQ(int(k1.type()), 1);
#if defined(DELTAFS)
    ASSERT_EQ(k2.snap_id(), 29);
#endif
    ASSERT_EQ(k2.inode(), 12345);
    ASSERT_EQ(int(k2.type()), 127);
#if defined(DELTAFS)
    ASSERT_EQ(k3.reg_id(), 13);
    ASSERT_EQ(k3.snap_id(), 37);
#endif
    ASSERT_EQ(k3.inode(), 67890);
    ASSERT_EQ(int(k3.type()), 255);
  }
  {
    char zero[50];
    memset(zero, 0, sizeof(zero));
    Key k1(0, static_cast<KeyType>(0));
    k1.SetName(Slice());
    Key k2(0, static_cast<KeyType>(1));
    k2.SetName(Slice("X"));
    Key k3(0, static_cast<KeyType>(1));
    k3.SetName(Slice("Y"));
    ASSERT_EQ(k1.Encode(), Slice(zero, k1.size()));
    ASSERT_EQ(k2.prefix(), k3.prefix());
    ASSERT_NE(k2.Encode(), k3.Encode());
    Key k4(k3);
    Key k5 = k3;
    k4.SetName(Slice("X"));
    ASSERT_EQ(k4.Encode(), k2.Encode());
    k5.SetHash(k2.hash());
    ASSERT_EQ(k5.Encode(), k2.Encode());
  }
  {
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
  {
    Key k1(31, kDataDesType);
    Key k2(k1.prefix());
    ASSERT_EQ(k1.prefix(), k2.prefix());
    Key k3(31, kDataBlockType);
    k3.SetType(kDataDesType);
    ASSERT_EQ(k1.prefix(), k3.prefix());
  }
}

class StatTest {};

TEST(StatTest, StatEncoding) {
  Stat stat;
  stat.SetRegId(13);
  stat.SetSnapId(37);
  stat.SetInodeNo(12345);
  stat.SetFileMode(678);
  stat.SetFileSize(90);
  stat.SetUserId(11);
  stat.SetGroupId(22);
  stat.SetChangeTime(11223344);
  stat.SetModifyTime(44332211);
  stat.SetZerothServer(777);
  stat.AssertAllSet();
  char tmp[sizeof(Stat)];
  Slice encoding = stat.EncodeTo(tmp);
  Stat stat2;
  bool r = stat2.DecodeFrom(encoding);
  ASSERT_TRUE(r);
  char tmp2[sizeof(Stat)];
  Slice encoding2 = stat2.EncodeTo(tmp2);
  ASSERT_EQ(encoding, encoding2);
}

class LookupEntryTest {};

TEST(LookupEntryTest, EntryEncoding) {
  LookupStat ent;
  ent.SetRegId(13);
  ent.SetSnapId(37);
  ent.SetInodeNo(12345);
  ent.SetDirMode(678);
  ent.SetUserId(11);
  ent.SetGroupId(22);
  ent.SetZerothServer(777);
  ent.SetLeaseDue(55667788);
  ent.AssertAllSet();
  char tmp[sizeof(LookupStat)];
  Slice encoding = ent.EncodeTo(tmp);
  LookupStat ent2;
  bool r = ent2.DecodeFrom(encoding);
  ASSERT_TRUE(r);
  char tmp2[sizeof(LookupStat)];
  Slice encoding2 = ent2.EncodeTo(tmp2);
  ASSERT_EQ(encoding, encoding2);
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
