/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs/deltafs_api.h"
#include "pdlfs-common/slice.h"
#include "pdlfs-common/strutil.h"

#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string>
#include <vector>
#if defined(GFLAGS)
#include <gflags/gflags.h>
#endif
#if defined(GLOG)
#include <glog/logging.h>
#endif

static bool shutting_down;

static void HandleSignal(int signal) {
  if (signal == SIGINT) {
    fprintf(stderr, "\n[Type exit to quit]\n");
    fprintf(stdout, "deltafs $");
    fflush(stdout);
  }
}

static void PrintHelloMsg() {
  fprintf(stdout,
          "== Deltafs shell (wip)\n\n"
          "Type help to see all available commands.\n"
          "Type exit to quit.\n\n");
}

static void PrintHelpMsg() {
  fprintf(stdout,
          "== Deltafs shell (wip)\n\n"
          "mkdir <path>\n\tMake a new directory\n\n"
          "mkfile <path>\n\tMake a new file\n\n"
          "cpfrom <local_path> <deltafs_path>\n\tCopy a file from a local file "
          "system to deltafs\n\n"
          "cat <path>\n\tPrint the content of a file\n\n"
          "\n");
}

static void Mkdir(const std::string& path) {
  if (deltafs_mkdir(path.c_str(), ACCESSPERMS) != 0) {
    perror("Fail to make directory");
  }
}

static void Mkfile(const std::string& path) {
  if (deltafs_mkfile(path.c_str(), ACCESSPERMS) != 0) {
    perror("Fail to create regular file");
  }
}

static void Cat(const std::string& path) {
  char buf[5000];
  size_t off = 0;
  struct stat ignored_stat;
  int fd = deltafs_open(path.c_str(), O_RDONLY, 0, &ignored_stat);
  if (fd == -1) {
    perror("Cannot open file");
  } else {
    ssize_t n = 0;
    while ((n = deltafs_pread(fd, buf, 4096, off)) > 0) {
      buf[n] = 0;
      fprintf(stdout, "%s", buf);
      off += n;
    }
    if (n == -1) {
      perror("Cannot read from file");
    }
    deltafs_close(fd);
  }
}

static void CopyFromLocal(const std::string& src, const std::string& dst) {
  char buf[5000];
  size_t off = 0;
  int fd = open(src.c_str(), O_RDONLY);
  if (fd == -1) {
    perror("Cannot open source file");
  } else {
    struct stat ignored_stat;
    int dfd = deltafs_open(dst.c_str(), O_WRONLY | O_CREAT, ACCESSPERMS,
                           &ignored_stat);
    if (dfd == -1) {
      perror("Cannot open destination file");
    } else {
      ssize_t nr = 0;
      ssize_t nw = 0;
      while ((nr = read(fd, buf, 4096)) > 0) {
        nw = deltafs_pwrite(dfd, buf, nr, off);
        if (nw == nr) {
          off += nw;
        } else {
          perror("Cannot write into destination file");
        }
      }
      if (nr == -1) {
        perror("Cannot read from source file");
      }
      deltafs_close(dfd);
    }
    close(fd);
  }
}

static void Exec(const std::vector<std::string>& inputs) {
  pdlfs::Slice cmd = inputs[0];
  if (cmd == "help") {
    PrintHelpMsg();
  } else if (cmd == "exit") {
    fprintf(stderr, "exit\n");
    shutting_down = true;
  } else if (cmd == "mkdir") {
    for (size_t i = 1; i < inputs.size(); i++) {
      Mkdir(inputs[i]);
    }
  } else if (cmd == "mkfile") {
    for (size_t i = 1; i < inputs.size(); i++) {
      Mkfile(inputs[i]);
    }
  } else if (cmd == "cpfrom") {
    for (size_t i = 2; i < inputs.size(); i++) {
      CopyFromLocal(inputs[1], inputs[i]);
    }
  } else if (cmd == "cat") {
    for (size_t i = 1; i < inputs.size(); i++) {
      Cat(inputs[i]);
    }
  } else {
    fprintf(stderr, "Command not found\n");
  }
}

int main(int argc, char* argv[]) {
#if defined(GFLAGS)
  std::string usage("Sample usage: ");
  usage += argv[0];
  google::SetUsageMessage(usage);
  google::SetVersionString("1.0");
  google::ParseCommandLineFlags(&argc, &argv, true);
#endif
#if defined(GLOG)
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
#endif
  PrintHelloMsg();
  signal(SIGINT, HandleSignal);
  char tmp[4096];
  shutting_down = false;
  while (!shutting_down) {
    fprintf(stdout, "deltafs$ ");
    fflush(stdout);
    char* p = fgets(tmp, sizeof(tmp), stdin);
    if (p == NULL) {
      break;  // End-of-file
    } else {
      std::vector<std::string> inputs;
      size_t n = pdlfs::SplitString(pdlfs::Slice(p), ' ', &inputs);
      if (n != 0) {
        Exec(inputs);
      }
    }
  }
  return 0;
}
