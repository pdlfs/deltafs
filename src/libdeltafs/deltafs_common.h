/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/env.h"

#include <stdlib.h>

namespace pdlfs {
// Obtain OS and compiler settings
extern void PrintSysInfo();  // XXX: allow specifying a logger?

// Reference to an Env instance
struct EnvRef {
  // True if the env belongs to the system
  bool is_system;
  Env* env;
};

// Open an Env instance according to a given configuration string
extern EnvRef OpenEnvOrDie(const char* name, const char* conf);

// Open an Env instance using the specified direct arguments
extern EnvRef OpenEnvWithArgsOrDie(int argc, void* argv[]);

}  // namespace pdlfs
