/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/random.h"
#include "pdlfs-common/testharness.h"

namespace pdlfs {

class RndTest {};

static void VerifyKeyUniqueness(uint32_t seed) {
  RandomSequence rsu(seed);
  unsigned char* bitfield = new unsigned char[0x20000000];
  memset(bitfield, 0, 0x20000000);
  unsigned int i = 0;
  do {  // Iterates through all 2^32 integers
    if ((i & 0xffffff) == 0) {
      fprintf(stderr, "Progress: %d/256\n", i >> 24);
    }
    unsigned int x = rsu.Next();
    unsigned char* byte = bitfield + (x >> 3);
    unsigned char bit = static_cast<unsigned char>(1u << (x & 7));
    ASSERT_TRUE((*byte & bit) == 0);
    *byte |= bit;
  } while (++i != 0);
  fprintf(stderr, "Done!\n");
  delete[] bitfield;
}

TEST(RndTest, RndSeqOfUni) {
  RandomSequence rsu1(301, 0, 0);
  rsu1.Next();
  rsu1.Skip(98);
  rsu1.Next();
  RandomSequence rsu2(301, 0, 100);
  ASSERT_EQ(rsu1.Next(), rsu2.Next());
  ASSERT_EQ(rsu1.Next(), rsu2.Next());
}

}  // namespace pdlfs

int main(int argc, char* argv[]) {
  if (strcmp(argv[argc - 1], "--bench") != 0) {
    return pdlfs::test::RunAllTests(&argc, &argv);
  } else {
    pdlfs::VerifyKeyUniqueness(301);
    return 0;
  }
}
