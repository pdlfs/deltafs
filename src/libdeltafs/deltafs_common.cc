/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/pdlfs_platform.h"

#include "pdlfs-common/logging.h"

#include "deltafs_common.h"
#if defined(DELTAFS_BBOS)
#include "bbos_env.h"
#endif

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

namespace pdlfs {

#if defined(PDLFS_OS_LINUX)
static Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit - 1])) {
    limit--;
  }
  Slice r = Slice(s.data() + start, limit - start);
  return r;
}
#endif

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

EnvRef OpenEnvWithArgsOrDie(int argc, void* argv[]) {
  if (argc >= 1) {
    const char* name = static_cast<const char*>(argv[0]);
#if defined(DELTAFS_BBOS)
    if (strcmp(name, "bbos") == 0) {
      Env* env = NULL;
      if (argc >= 5) {
        Status s = bbos::BbosInit(
            &env, static_cast<const char*>(argv[1]),  // HG local URI
            static_cast<const char*>(argv[2]),        // HG remote URI
            argv[3], argv[4]);
        if (s.ok()) {
          EnvRef ref;
          ref.is_system = false;
          ref.env = env;
          return ref;
        }
      }
      Error(__LOG_ARGS__, "Cannot create bbos env");
      Error(__LOG_ARGS__, "Abort...");
      abort();
    }
#endif
    return OpenEnvOrDie(name, "");  // Compatibility mode
  } else {
    EnvRef ref;
    ref.env = Env::Default();
    ref.is_system = true;
    return ref;
  }
}

// XXX: BBOS is not supported in this path
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
