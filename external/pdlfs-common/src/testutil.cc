#include "pdlfs-common/testutil.h"

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdio.h>
#include <string.h>

#include "pdlfs-common/random.h"

namespace pdlfs {
namespace test {

bool StringEndWith(const std::string& str, const std::string& suffix) {
  return Slice(str).ends_with(suffix);
}

std::string FileName(int i) {
  char buf[1024];
  snprintf(buf, 1024, "file%d", i);
  return buf;
}

Slice RandomString(Random* rnd, int len, std::string* dst) {
  dst->resize(len);
  for (int i = 0; i < len; i++) {
    (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));  // ' ' .. '~'
  }
  return Slice(*dst);
}

/* clang-format off */
std::string RandomKey(Random* rnd, int len) {
  // Make sure to generate a wide variety of characters so we
  // test the boundary conditions for short-key optimizations.
  static const char kTestChars[] = {
    '\0', '\1', 'a', 'b', 'c', 'd', 'e', '\xfd', '\xfe', '\xff'
  };
  std::string result;
  for (int i = 0; i < len; i++) {
    result += kTestChars[rnd->Uniform(sizeof(kTestChars))];
  }
  return result;
}
/* clang-format on */

extern Slice CompressibleString(Random* rnd, double compressed_fraction,
                                size_t len, std::string* dst) {
  int raw = static_cast<int>(len * compressed_fraction);
  if (raw < 1) raw = 1;
  std::string raw_data;
  RandomString(rnd, raw, &raw_data);

  // Duplicate the random data until we have filled "len" bytes
  dst->clear();
  while (dst->size() < len) {
    dst->append(raw_data);
  }
  dst->resize(len);
  return Slice(*dst);
}

}  // namespace test
}  // namespace pdlfs
