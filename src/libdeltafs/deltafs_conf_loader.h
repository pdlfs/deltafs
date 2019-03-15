#pragma once

/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_conf_raw.h"

#include "pdlfs-common/strutil.h"

#include <ctype.h>

namespace pdlfs {
namespace config {

inline void ToLowerCase(std::string* str) {
  std::string::iterator it;
  for (it = str->begin(); it != str->end(); ++it) {
    *it = tolower(*it);
  }
}

#define CONF_LOADER_UI64(conf)                           \
  inline Status Load##conf(uint64_t* dst) {              \
    std::string str_##conf = config::conf();             \
    ToLowerCase(&str_##conf);                            \
    if (!ParsePrettyNumber(str_##conf, dst)) {           \
      return Status::InvalidArgument(#conf, str_##conf); \
    } else {                                             \
      return Status::OK();                               \
    }                                                    \
  }

#define CONF_LOADER_BOOL(conf)                           \
  inline Status Load##conf(bool* dst) {                  \
    std::string str_##conf = config::conf();             \
    ToLowerCase(&str_##conf);                            \
    if (!ParsePrettyBool(str_##conf, dst)) {             \
      return Status::InvalidArgument(#conf, str_##conf); \
    } else {                                             \
      return Status::OK();                               \
    }                                                    \
  }

CONF_LOADER_UI64(NumOfMetadataSrvs)
CONF_LOADER_UI64(NumOfVirMetadataSrvs)
CONF_LOADER_UI64(InstanceId)
CONF_LOADER_BOOL(MDSTracing)
CONF_LOADER_UI64(MaxNumOfOpenFiles)
CONF_LOADER_UI64(SizeOfSrvLeaseTable)
CONF_LOADER_UI64(SizeOfSrvDirTable)
CONF_LOADER_UI64(SizeOfCliLookupCache)
CONF_LOADER_UI64(SizeOfCliIndexCache)
CONF_LOADER_UI64(SizeOfMetadataWriteBuffer)
CONF_LOADER_UI64(SizeOfMetadataTables)
CONF_LOADER_BOOL(DisableMetadataCompaction)
CONF_LOADER_BOOL(AtomicPathRes)
CONF_LOADER_BOOL(ParanoidChecks)
CONF_LOADER_BOOL(VerifyChecksums)

#undef CONF_LOADER_UI64
#undef CONF_LOADER_BOOL

}  // namespace config
}  // namespace pdlfs
