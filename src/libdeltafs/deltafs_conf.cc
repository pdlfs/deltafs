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
#define DEFINE_FLAG_PORT(n, v)                              \
  static inline std::string FLAGS_load_##n() { return ""; } \
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
#include <ctype.h>
static void ToLowerCase(std::string* str) {
  std::string::iterator it;
  for (it = str->begin(); it != str->end(); ++it) {
    *it = tolower(*it);
  }
}
#define DEFINE_FLAG(n, v)                  \
  DEFINE_FLAG_PORT(n, v)                   \
  std::string n() {                        \
    std::string result = FLAGS_load_##n(); \
    if (result.empty()) {                  \
      result = FLAGS_##n;                  \
    }                                      \
    ToLowerCase(&result);                  \
    return result;                         \
  }

#include "deltafs_conf.h"
namespace pdlfs {
namespace config {

DEFINE_FLAG(NumOfMetadataSrvs, "1")
DEFINE_FLAG(NumOfVirMetadataSrvs, "1")
DEFINE_FLAG(InstanceId, "0")
DEFINE_FLAG(RPCProto, "bmi+tcp")
DEFINE_FLAG(RPCTracing, "false")
DEFINE_FLAG(MetadataSrvAddrs, "localhost:10101")
DEFINE_FLAG(SizeOfSrvLeaseTable, "4k")
DEFINE_FLAG(SizeOfSrvDirTable, "1k")
DEFINE_FLAG(SizeOfCliLookupCache, "4k")
DEFINE_FLAG(SizeOfCliIndexCache, "1k")
DEFINE_FLAG(AtomicPathRes, "false")
DEFINE_FLAG(ParanoidChecks, "false")
DEFINE_FLAG(VerifyChecksums, "false")
DEFINE_FLAG(Inputs, "/tmp/deltafs/inputs")
DEFINE_FLAG(Outputs, "/tmp/deltafs/outputs")
DEFINE_FLAG(EnvName, "posix")
DEFINE_FLAG(EnvConf, "")

}  // namespace config
}  // namespace pdlfs
