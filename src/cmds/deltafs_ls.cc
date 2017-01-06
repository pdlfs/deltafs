/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs/deltafs_api.h"
#include "deltafs/deltafs_config.h"
#include "pdlfs-common/pdlfs_config.h"

#if defined(PDLFS_GFLAGS)
#include <gflags/gflags.h>
#endif

#if defined(PDLFS_GLOG)
#include <glog/logging.h>
#endif

#include <errno.h>
#include <stdio.h>
#include <string.h>
int main(int argc, char* argv[]) {
#if defined(PDLFS_GLOG)
  FLAGS_logtostderr = true;
#endif
#if defined(PDLFS_GFLAGS)
  std::string usage("Sample usage: ");
  usage += argv[0];
  google::SetUsageMessage(usage);
  google::SetVersionString(PDLFS_COMMON_VERSION);
  google::ParseCommandLineFlags(&argc, &argv, true);
#endif
#if defined(PDLFS_GLOG)
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
#endif
  struct NamePrinter {
    static int Print(const char* name, void* count) {
      (*reinterpret_cast<int*>(count))++;
      fprintf(stdout, "%s\n", name);
      return 0;
    }
  };
  for (int i = 1; i < argc; i++) {
    int count = 0;
    if (argc > 2) fprintf(stdout, "%s:\n", argv[i]);
    int r = deltafs_listdir(argv[i], NamePrinter::Print, &count);
    if (r != 0) {
      fprintf(stderr, "ls: cannot list directory '%s': %s\n", argv[i],
              strerror(errno));
      return -1;
    } else if (argc > 2) {
      fprintf(stdout, "\n");
    }
  }

  return 0;
}
