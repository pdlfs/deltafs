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

class StatTest {
  // Empty.
};

TEST(StatTest, StatEncoding) {
  Stat stat;
#if defined(DELTAFS_PROTO)
  stat.SetDnodeNo(21);
#endif
#if defined(DELTAFS)
  stat.SetRegId(13);
  stat.SetSnapId(37);
#endif
  stat.SetInodeNo(12345);
  stat.SetFileSize(90);
  stat.SetFileMode(678);
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
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

#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
class LookupEntryTest {
  // Empty
};

TEST(LookupEntryTest, EntryEncoding) {
  LookupStat stat;
#if defined(DELTAFS_PROTO)
  stat.SetDnodeNo(21);
#endif
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
