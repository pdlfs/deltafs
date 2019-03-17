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

#include "deltafs_envs.h"

#include "pdlfs-common/logging.h"
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/pdlfs_platform.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

namespace pdlfs {
namespace {
#if defined(PDLFS_OS_LINUX)
Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit - 1])) {
    limit--;
  }

  Slice r = s;
  r.remove_suffix(s.size() - limit);
  r.remove_prefix(start);
  return r;
}
#endif
}  // namespace

void PrintSysInfo() {
  Info(__LOG_ARGS__, " DeltaFS: Version %d.%d.%d (dev)",
       PDLFS_COMMON_VERSION_MAJOR, PDLFS_COMMON_VERSION_MINOR,
       PDLFS_COMMON_VERSION_PATCH);

#if defined(PDLFS_OS_LINUX)
  time_t now = time(NULL);
  Info(__LOG_ARGS__, "    Date: %s", ctime(&now));
  FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
  if (cpuinfo != NULL) {
    char line[1000];
    int num_cpus = 0;
    std::string cpu_type = "???";
    std::string cache_size = "???";
    std::string cl_size = "???";
    while (fgets(line, sizeof(line), cpuinfo) != NULL) {
      const char* sep = strchr(line, ':');
      if (sep == NULL) {
        continue;
      }
      Slice key = TrimSpace(Slice(line, sep - 1 - line));
      Slice val = TrimSpace(Slice(sep + 1));
      if (key == "model name") {
        cpu_type = val.ToString();
      } else if (key == "cache size") {
        cache_size = val.ToString();
      } else if (key == "clflush size") {
        cl_size = val.ToString();
      } else if (key == "processor") {
        num_cpus++;
      }
    }
    Info(__LOG_ARGS__, "     CPU: %d x %s", num_cpus, cpu_type.c_str());
    Info(__LOG_ARGS__, "CPUCache: %s (Line: %s Bytes)", cache_size.c_str(),
         cl_size.c_str());
    fclose(cpuinfo);
  }
  FILE* meminfo = fopen("/proc/meminfo", "r");
  if (meminfo != NULL) {
    char line[1000];
    std::string total_mem = "???";
    while (fgets(line, sizeof(line), meminfo) != NULL) {
      const char* sep = strchr(line, ':');
      if (sep == NULL) {
        continue;
      }
      Slice key = TrimSpace(Slice(line, sep - line));
      Slice val = TrimSpace(Slice(sep + 1));
      if (key == "MemTotal") {
        total_mem = val.ToString();
        break;
      }
    }
    Info(__LOG_ARGS__, "  Memory: %s", total_mem.c_str());
    fclose(meminfo);
  }
#endif

  Info(__LOG_ARGS__, "      OS: %s %s (target)", PDLFS_TARGET_OS,
       PDLFS_TARGET_OS_VERSION);
  Info(__LOG_ARGS__, "      OS: %s %s", PDLFS_HOST_OS, PDLFS_HOST_OS_VERSION);

#if defined(__INTEL_COMPILER)
  Info(__LOG_ARGS__, "     CXX: Intel (icc/icpc) %d.%d.%d %d",
       __INTEL_COMPILER / 100, __INTEL_COMPILER % 100, __INTEL_COMPILER_UPDATE,
       __INTEL_COMPILER_BUILD_DATE);
#elif defined(_CRAYC)
  Info(__LOG_ARGS__, "     CXX: CRAY (crayc/crayc++) %d.%d", _RELEASE,
       _RELEASE_MINOR);
#elif defined(__GNUC__)
  Info(__LOG_ARGS__, "     CXX: GNU (gcc/g++) %d.%d.%d", __GNUC__,
       __GNUC_MINOR__, __GNUC_PATCHLEVEL__);
#elif defined(__clang__)
  Info(__LOG_ARGS__, "     CXX: Clang (clang/clang++) %d.%d.%d",
       __clang_major__, __clang_minor__, __clang_patchlevel__);
#endif
}

EnvRef OpenEnvOrDie(int argc, void* argv[]) {
  if (argc >= 1) {
    const char* name = static_cast<const char*>(argv[0]);
    return OpenEnvOrDie(name, "");
  } else {
    EnvRef ref;
    ref.env = Env::Default();
    ref.is_system = true;
    return ref;
  }
}

EnvRef OpenEnvOrDie(const char* name, const char* conf) {
  bool is_system;
  Env* env = Env::Open(name, conf, &is_system);
  if (env != NULL) {
    EnvRef ref;
    ref.is_system = is_system;
    ref.env = env;
    return ref;
  } else {
    Error(__LOG_ARGS__, "Cannot open env");
    Error(__LOG_ARGS__, "Abort...");
    abort();
  }
}

}  // namespace pdlfs
