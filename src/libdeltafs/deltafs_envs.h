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

#pragma once

#include "pdlfs-common/env.h"

#include <stdlib.h>
#include <string.h>

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

// Attempt to open an Env instance using the given Env configuration string.
// Return a pointer to the Env instance on success, or commit abort() on errors.
// If success, also set *is_system to True iff the env belongs to the system
// and should not ever be deleted. Otherwise, the returned env
// instance should be deleted when no longer needed.
inline Env* TryOpenEnv(const char* name, const char* conf, bool* is_system) {
  if (name == NULL) name = "posix.unbufferedio";
  if (strlen(name) == 0) name = "posix.unbufferedio";
  if (conf == NULL) conf = "";
  EnvRef ref = OpenEnvOrDie(name, conf);
  *is_system = ref.is_system;
  return ref.env;
}

// Open an Env instance using the specified direct arguments
extern EnvRef OpenEnvOrDie(int argc, void* argv[]);

// Attempt to open an Env instance using the given set of direct arguments.
// Return a pointer to the Env instance on success, or commit abort() on errors.
// If success, also set *is_system to True iff the env belongs to the
// system and should not ever be deleted. Otherwise, the returned env
// should be deleted when no longer needed.
inline Env* TryOpenEnv(int argc, void* argv[], bool* is_system) {
  EnvRef ref;
  if (argc != 0) {
    ref = OpenEnvOrDie(argc, argv);
  } else {
    char argv0[] = "posix.unbufferedio";
    argv = reinterpret_cast<void**>(&argv0);
    ref = OpenEnvOrDie(1, argv);
  }
  *is_system = ref.is_system;
  return ref.env;
}

}  // namespace pdlfs
