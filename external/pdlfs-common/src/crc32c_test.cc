/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/crc32c.h"
#include "pdlfs-common/testharness.h"

#include "crc32c_internal.h"

namespace pdlfs {
namespace crc32c {

class CRC {
 public:
  CRC() : hw_(CanAccelerateCrc32c()) {}

  uint32_t CRCExtend(uint32_t crc, const char* buf, size_t n) {
    uint32_t result = Extend(crc, buf, n);
    if (hw_) ASSERT_EQ(result, ExtendHW(crc, buf, n));
    ASSERT_EQ(result, ExtendSW(crc, buf, n));
    return result;
  }

  uint32_t CRCValue(const char* buf, size_t n) {
    uint32_t result = Value(buf, n);
    if (hw_) ASSERT_EQ(result, ValueHW(buf, n));
    ASSERT_EQ(result, ValueSW(buf, n));
    return result;
  }

  int hw_;
};

TEST(CRC, HW) {
  if (hw_) {
    fprintf(stderr, "crc32c hardware acceleration is available");
  } else {
    fprintf(stderr, "crc32c hardware acceleration is off");
  }

  fprintf(stderr, "\n");
}

TEST(CRC, StandardResults) {
  // From rfc3720 section B.4.
  char buf[32];

  memset(buf, 0, sizeof(buf));
  ASSERT_EQ(0x8a9136aa, CRCValue(buf, sizeof(buf)));

  memset(buf, 0xff, sizeof(buf));
  ASSERT_EQ(0x62a8ab43, CRCValue(buf, sizeof(buf)));

  for (int i = 0; i < 32; i++) {
    buf[i] = i;
  }
  ASSERT_EQ(0x46dd794e, CRCValue(buf, sizeof(buf)));

  for (int i = 0; i < 32; i++) {
    buf[i] = 31 - i;
  }
  ASSERT_EQ(0x113fdb5c, CRCValue(buf, sizeof(buf)));

  unsigned char data[48] = {
      /* clang-format off */
      0x01, 0xc0, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00,
      0x14, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x04, 0x00,
      0x00, 0x00, 0x00, 0x14,
      0x00, 0x00, 0x00, 0x18,
      0x28, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00,
      0x02, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00,
  }; /* clang-format on */
  ASSERT_EQ(0xd9963a56, CRCValue(reinterpret_cast<char*>(data), sizeof(data)));
}

TEST(CRC, Values) { ASSERT_NE(CRCValue("a", 1), CRCValue("foo", 3)); }

TEST(CRC, Extend) {
  ASSERT_EQ(CRCValue("hello world", 11),
            CRCExtend(CRCValue("hello ", 6), "world", 5));
}

TEST(CRC, Mask) {
  uint32_t crc = CRCValue("foo", 3);
  ASSERT_NE(crc, Mask(crc));
  ASSERT_NE(crc, Mask(Mask(crc)));
  ASSERT_EQ(crc, Unmask(Mask(crc)));
  ASSERT_EQ(crc, Unmask(Unmask(Mask(Mask(crc)))));
}
}  // namespace crc32c
}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
