#pragma once

/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_conf.h"

#include "pdlfs-common/strutil.h"

namespace pdlfs {
namespace config {

#define CONF_LOADER_UI64(conf)                           \
  inline Status Load##conf(uint64_t* dst) {              \
    std::string str_##conf = config::conf();             \
    if (!ParsePrettyNumber(str_##conf, dst)) {           \
      return Status::InvalidArgument(#conf, str_##conf); \
    } else {                                             \
      return Status::OK();                               \
    }                                                    \
  }

#define CONF_LOADER_BOOL(conf)                           \
  inline Status Load##conf(bool* dst) {                  \
    std::string str_##conf = config::conf();             \
    if (!ParsePrettyBool(str_##conf, dst)) {             \
      return Status::InvalidArgument(#conf, str_##conf); \
    } else {                                             \
      return Status::OK();                               \
    }                                                    \
  }

CONF_LOADER_UI64(NumOfMetadataSrvs)
CONF_LOADER_UI64(NumOfVirMetadataSrvs)
CONF_LOADER_UI64(InstanceId)
CONF_LOADER_BOOL(RPCTracing)
CONF_LOADER_UI64(SizeOfCliLookupCache)
CONF_LOADER_UI64(SizeOfCliIndexCache)
CONF_LOADER_BOOL(AtomicPathResolution)
CONF_LOADER_BOOL(ParanoidChecks)
CONF_LOADER_BOOL(VerifyChecksums)

#undef CONF_LOADER_UI64
#undef CONF_LOADER_BOOL

}  // namespace config
}  // namespace pdlfs
