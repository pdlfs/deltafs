/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
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
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
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

  struct StatPrinter {
    static std::string PrintMode(const struct stat* statbuf) {
      if (S_ISREG(statbuf->st_mode)) {
        return "Regular file";
      } else if (S_ISDIR(statbuf->st_mode)) {
        return "Directory";
      } else {
        return "Unknown file type";
      }
    }
    static std::string PrintUser(const struct stat* statbuf) {
      char tmp[30];
      struct passwd* pw = getpwuid(statbuf->st_uid);
      if (pw != NULL) {
        snprintf(tmp, sizeof(tmp), "%d / %s", statbuf->st_uid, pw->pw_name);
      } else {
        snprintf(tmp, sizeof(tmp), "%d", statbuf->st_uid);
      }
      return tmp;
    }
    static std::string PrintGroup(const struct stat* statbuf) {
      char tmp[30];
      struct group* gr = getgrgid(statbuf->st_gid);
      if (gr != NULL) {
        snprintf(tmp, sizeof(tmp), "%d / %s", statbuf->st_gid, gr->gr_name);
      } else {
        snprintf(tmp, sizeof(tmp), "%d", statbuf->st_gid);
      }
      return tmp;
    }
  };
  struct stat statbuf;
  for (int i = 1; i < argc; i++) {
    int r = deltafs_stat(argv[i], &statbuf);
    if (r == 0) {
      fprintf(stdout,
              "  File: '%s'\n"
              "  Size: %llu\n"
              "  Mode: %s\n"
              "Access: %o\tUser: %s\tGroup: %s\n",
              argv[i], static_cast<unsigned long long>(statbuf.st_size),
              StatPrinter::PrintMode(&statbuf).c_str(), statbuf.st_mode,
              StatPrinter::PrintUser(&statbuf).c_str(),
              StatPrinter::PrintGroup(&statbuf).c_str());
    } else {
      fprintf(stderr, "stat: cannot stat file '%s': %s\n", argv[i],
              strerror(errno));
      return -1;
    }
  }

  return 0;
}
