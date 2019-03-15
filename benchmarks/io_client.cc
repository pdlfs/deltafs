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

#include "io_client.h"

namespace pdlfs {
namespace ioclient {

Dir::~Dir() {}

IOClient::~IOClient() {}

IOClient* IOClient::Factory(const IOClientOptions& raw_options) {
  IOClientOptions options = raw_options;
  if (options.argc >= 3) {
    options.conf_str = options.argv[2];
  }
  std::string fs = "default";  // Default io client type
  if (options.argc >= 2) {
    fs = options.argv[1];
  }
  if (fs == "deltafs") {
    return IOClient::Deltafs(options);
  } else {
    return IOClient::Default(options);
  }
}

}  // namespace ioclient
}  // namespace pdlfs
