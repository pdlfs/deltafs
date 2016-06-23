/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
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

TEST(KeyTest, EncodeDecode) {
  {
    Key k1(1, static_cast<KeyType>(1), Slice());
    Key k2(2, static_cast<KeyType>(1), Slice());
    Key k3(2, static_cast<KeyType>(2), Slice());
    Key k4(2, static_cast<KeyType>(2), Slice("X"));
    Key k5(3, static_cast<KeyType>(1), Slice());
    ASSERT_LT(k1.Encode(), k2.Encode());
    ASSERT_LT(k2.Encode(), k3.Encode());
    ASSERT_LT(k3.Encode(), k4.Encode());
    ASSERT_LT(k4.Encode(), k5.Encode());
  }
  {
    Key k1(0, static_cast<KeyType>(1), Slice());
    Key k2(12345, static_cast<KeyType>(127), Slice());
    Key k3(67890, static_cast<KeyType>(255), Slice());
    ASSERT_EQ(k1.dir_id(), 0);
    ASSERT_EQ(int(k1.type()), 1);
    ASSERT_EQ(k2.dir_id(), 12345);
    ASSERT_EQ(int(k2.type()), 127);
    ASSERT_EQ(k3.dir_id(), 67890);
    ASSERT_EQ(int(k3.type()), 255);
  }
  {
    char zero[50];
    memset(zero, 0, sizeof(zero));
    Key k1(0, static_cast<KeyType>(0), Slice());
    Key k2(0, static_cast<KeyType>(1), Slice("X"));
    Key k3(0, static_cast<KeyType>(1), Slice("Y"));
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
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
