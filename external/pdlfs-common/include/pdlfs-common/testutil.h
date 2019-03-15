#pragma once

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

#include "pdlfs-common/env.h"
#include "pdlfs-common/random.h"

namespace pdlfs {
namespace test {
// Return a file name containing a given unique identifier.
extern std::string FileName(int i);

// Return true iff a file ends with a given suffix
extern bool StringEndWith(const std::string& str, const std::string& suffix);

// Store in *dst a random string of length "len" and return a Slice that
// references the generated data.
extern Slice RandomString(Random* rnd, int len, std::string* dst);

// Return a random key with the specified length that may contain interesting
// characters (e.g. \x00, \xff, etc.).
extern std::string RandomKey(Random* rnd, int len);

// Store in *dst a string of length "len" that will compress to
// "N*compressed_fraction" bytes and return a Slice that references
// the generated data.
extern Slice CompressibleString(Random* rnd, double compressed_fraction,
                                size_t len, std::string* dst);

}  // namespace test
}  // namespace pdlfs
