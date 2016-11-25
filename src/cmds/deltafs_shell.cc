/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/slice.h"
#include "pdlfs-common/strutil.h"
#define DELTAFS_PS1 "Deltafs (v" PDLFS_COMMON_VERSION ") > "
#include "deltafs/deltafs_api.h"

#include <fcntl.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <string>
#include <vector>

#if defined(PDLFS_GFLAGS)
#include <gflags/gflags.h>
#endif

#if defined(PDLFS_GLOG)
#include <glog/logging.h>
#endif

// True if user has requested shutdown by sending the ctrl+c signal
static bool shutting_down_signaled = false;
// Timestamp of the last shutdown signal received by us
static time_t shutting_down_timestamp = 0;
// True if user has requested shutdown by typing "exit" at command line
static bool shutting_down_requested = false;

static void Print(const char* msg, ...) {
  va_list va;
  va_start(va, msg);
  vfprintf(stdout, msg, va);
  fflush(stdout);
  va_end(va);
}

static void HandleIntSignal() {
  const time_t current = time(NULL);
  if (!shutting_down_signaled || current - shutting_down_timestamp > 1) {
    Print("\n[Type ctrl+c again to force exit]\n");
    Print(DELTAFS_PS1);
    shutting_down_timestamp = current;
    shutting_down_signaled = true;
  } else {
    exit(EXIT_FAILURE);
  }
}

static void HandleSignal(int signal) {
  if (signal == SIGINT) {
    HandleIntSignal();
  } else {
    // Ignore it
  }
}

static void PrintWelcomeMessage() {
  Print(
      "== Deltafs shell (still work-in-progress)\n\n"
      "  XXXXXXXXX\n"
      "  XX      XX                 XX                  XXXXXXXXXXX\n"
      "  XX       XX                XX                  XX\n"
      "  XX        XX               XX                  XX\n"
      "  XX         XX              XX   XX             XX\n"
      "  XX          XX             XX   XX             XXXXXXXXX\n"
      "  XX           XX  XXXXXXX   XX XXXXXXXXXXXXXXX  XX          XX\n"
      "  XX          XX  XX     XX  XX   XX       XX XX XX       XX\n"
      "  XX         XX  XX       XX XX   XX      XX  XX XX     XX\n"
      "  XX        XX   XXXXXXXXXX  XX   XX     XX   XX XX     XXXXXXXX\n"
      "  XX       XX    XX          XX   XX    XX    XX XX           XX\n"
      "  XX      XX      XX      XX XX   XX X    XX  XX XX         XX\n"
      "  XXXXXXXXX        XXXXXXX   XX    XX        XX  XX      XX\n"
      "\n\n"
      "Type help to see all available commands.\n"
      "Type exit to quit.\n\n");
}

static void PrintUsage() {
  Print(
      "== Deltafs shell (wip)\n\n"
      "ls <path>\n\tList the children of a given directory\n\n"
      "mkdir <path>\n\tMake a new directory\n\n"
      "mkfile <path>\n\tMake a new file\n\n"
      "cpfrom <local_path> <deltafs_path>\n\t"
      "Copy a file from the local file system to deltafs\n\n"
      "cat <path>\n\tPrint the content of a file\n\n"
      "\n");
}

static void Listdir(const std::string& path) {
  struct DirLister {
    static int Print(const char* name, void* arg) {
      ++(*reinterpret_cast<int*>(arg));
      ::Print("%s\n", name);
      return 0;
    }
  };
  int n = 0;
  if (deltafs_listdir(path.c_str(), DirLister::Print, &n) != 0) {
    perror("Fail to list directory");
  } else {
    Print("== %d entries\n", n);
  }
}

static void Mkdir(const std::string& path) {
  if (deltafs_mkdir(path.c_str(), ACCESSPERMS) != 0) {
    perror("Fail to make directory");
  } else {
    Print("OK\n");
  }
}

static void Mkfile(const std::string& path) {
  if (deltafs_mkfile(path.c_str(), ACCESSPERMS) != 0) {
    perror("Fail to create regular file");
  } else {
    Print("OK\n");
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
    printf("OK\n");
  }
}

static void Exec(const std::vector<std::string>& inputs) {
  pdlfs::Slice cmd = inputs[0];
  if (cmd == "help" || cmd == "?") {
    PrintUsage();
  } else if (cmd == "exit" || cmd == "quit") {
    Print("exit\n");
    shutting_down_requested = true;
  } else if (cmd == "ls") {
    for (size_t i = 1; i < inputs.size(); i++) {
      Listdir(inputs[i]);
    }
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
#if defined(PDLFS_GLOG)
  FLAGS_logtostderr = true;
#endif
#if defined(PDLFS_GFLAGS)
  std::string usage("Sample usage: ");
  usage += argv[0];
  google::SetUsageMessage(usage);
  google::SetVersionString("1.0");
  google::ParseCommandLineFlags(&argc, &argv, true);
#endif
#if defined(PDLFS_GLOG)
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
#endif
  PrintWelcomeMessage();
  signal(SIGINT, HandleSignal);
  char tmp[4096];
  while (!shutting_down_requested) {
    Print(DELTAFS_PS1);
    char* p = fgets(tmp, sizeof(tmp), stdin);
    if (p != NULL) {
      std::vector<std::string> inputs;
      size_t n = pdlfs::SplitString(&inputs, pdlfs::Slice(p), ' ');
      if (n != 0) {
        Exec(inputs);
      }
    } else {
      break;  // End-of-file
    }
  }
  return 0;
}
