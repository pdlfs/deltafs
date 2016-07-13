/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdlib.h>
#include <string>
#if defined(GFLAGS)
#include <gflags/gflags.h>
#define DEFINE_FLAG_PORT(n, v)                       \
  static std::string FLAGS_load_##n() { return ""; } \
  DEFINE_string(n, v, "deltafs");
#else
static std::string LoadFromEnv(const char* key) {
  const char* v = getenv(key);
  if (v == NULL) {
    return "";
  } else {
    return v;
  }
}
#define DEFINE_FLAG_PORT(n, v)                                    \
  static std::string FLAGS_load_##n() { return LoadFromEnv(#n); } \
  static std::string FLAGS_##n = v;
#endif
#define DEFINE_FLAG(n, v)                  \
  DEFINE_FLAG_PORT(n, v)                   \
  std::string n() {                        \
    std::string result = FLAGS_load_##n(); \
    if (result.empty()) {                  \
      result = FLAGS_##n;                  \
    }                                      \
    return result;                         \
  }

#include "deltafs_conf.h"
namespace pdlfs {
namespace config {

DEFINE_FLAG(NumOfMetadataSrvs, "1")
DEFINE_FLAG(NumOfVirMetadataSrvs, "1")
DEFINE_FLAG(MetadataSrvAddrs, "bmi+tcp://localhost:10101")
DEFINE_FLAG(SizeOfCliLookupCache, "64k")
DEFINE_FLAG(SizeOfCliIndexCache, "16k")
DEFINE_FLAG(AtomicPathResolution, "false")
DEFINE_FLAG(ParanoidChecks, "false")
DEFINE_FLAG(VerifyChecksums, "false")
DEFINE_FLAG(EnvName, "posix")
DEFINE_FLAG(EnvConf, "")

}  // namespace config
}  // namespace pdlfs
