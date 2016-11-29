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
#define DELTAFS_PS1_ "Deltafs (v" PDLFS_COMMON_VERSION "):%s$ "
static const char* __DELTAFS_PS1__();
#define DELTAFS_PS1 (__DELTAFS_PS1__())
#include "deltafs/deltafs_api.h"

#include <errno.h>
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

static const char* __DELTAFS_PS1__() {
  static char cwd[4096] = {0};
  static char tmp[5000] = {0};
  const char* p = deltafs_getcwd(cwd, sizeof(cwd));
  if (p == NULL) p = "?";
  snprintf(tmp, sizeof(tmp), DELTAFS_PS1_, p);
  return tmp;
}

// True if user has requested shutdown by sending the ctrl+c signal
static bool shutting_down_signaled = false;
// Timestamp of the last shutdown signal received by us
static time_t shutting_down_timestamp = 0;
// True if user has requested shutdown by typing "exit" at command line
static bool shutting_down_requested = false;

static void Error(const char* msg, ...) {
  va_list va;
  va_start(va, msg);
  char tmp[500];
  vsnprintf(tmp, sizeof(tmp), msg, va);
  perror(tmp);
  va_end(va);
}

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
    Print("\n[Type ctrl+c again to force termination]\n");
    Print(DELTAFS_PS1);
    shutting_down_timestamp = current;
    shutting_down_signaled = true;
  } else {
    Print("\n");
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
      "  XX           XX  XXXXXXX   XX XXXXXXXXXXXXXXX  XX         XX\n"
      "  XX          XX  XX     XX  XX   XX       XX XX XX      XX\n"
      "  XX         XX  XX       XX XX   XX      XX  XX XX    XX\n"
      "  XX        XX   XXXXXXXXXX  XX   XX     XX   XX XX    XXXXXXXX\n"
      "  XX       XX    XX          XX   XX    XX    XX XX           XX\n"
      "  XX      XX      XX      XX XX   XX X    XX  XX XX         XX\n"
      "  XXXXXXXXX        XXXXXXX   XX    XX        XX  XX      XX\n"
      "\n\n"
      "Type help to see all available commands.\n"
      "Type exit to quit.\n\n"
      "\n");
}

static void PrintUsage() {
  Print(
      "== Deltafs shell (still work-in-progress)\n\n"
      "pwd\n\tPrint the current working directory\n\n"
      "cd <path>\n\tChange the current working directory\n\n"
      "ls <path>\n\tList the children of a given directory\n\n"
      "mkdir <path>\n\tMake a new directory\n\n"
      "mkfile <path>\n\tMake a new file\n\n"
      "cpfrom <local_path> <deltafs_path>\n\t"
      "Copy a file from the local file system to deltafs\n\n"
      "cat <path>\n\tPrint the content of a file\n\n"
      "\n");
}

static void Getcwd(int argc, char* argv[]) {
  char tmp[4096];
  char* p = deltafs_getcwd(tmp, sizeof(tmp));
  if (p == NULL) {
    Error("cannot getcwd");
  } else {
    Print("%s\n", p);
  }
}

static void Chdir(int argc, char* argv[]) {
  static char* argv2[2] = {NULL, (char*)"/"};
  if (argc <= 1) {
    argv = argv2;  // XXX: cd /
    argc = 2;
  }
  if (deltafs_chdir(argv[1]) != 0) {
    Error("cannot chdir '%s'", argv[1]);
  } else {
    Print("OK\n");
  }
}

static void Listdir(int argc, char* argv[]) {
  static char* argv2[2] = {NULL, (char*)"."};
  struct DirLister {
    static int Print(const char* name, void* arg) {
      ++(*reinterpret_cast<int*>(arg));
      ::Print("%s\n", name);
      return 0;
    }
  };
  if (argc <= 1) {
    argv = argv2;  // XXX: ls .
    argc = 2;
  }
  for (int i = 1; i < argc; i++) {
    int n = 0;
    if (deltafs_listdir(argv[i], DirLister::Print, &n) != 0) {
      Error("cannot list directory '%s'", argv[i]);
      break;
    } else {
      Print("== %d entries\n", n);
    }
  }
}

static void Mkdir(int argc, char* argv[]) {
  static const mode_t mode = 0755;  // XXX: make this an argument?
  for (int i = 1; i < argc; i++) {
    if (deltafs_mkdir(argv[i], mode) != 0) {
      Error("cannot make directory '%s'", argv[i]);
      break;
    } else {
      Print("OK\n");
    }
  }
}

static void Mkfile(int argc, char* argv[]) {
  static const mode_t mode = 0644;  // XXX: make this an argument?
  for (int i = 1; i < argc; i++) {
    if (deltafs_mkfile(argv[i], mode) != 0) {
      Error("cannot create file '%s'", argv[i]);
      break;
    } else {
      Print("OK\n");
    }
  }
}

static void Cat(int argc, char* argv[]) {
  char buf[5000];
  for (int i = 1; i < argc; i++) {
    ssize_t nr = 0;
    off_t off = 0;
    int fd = deltafs_open(argv[i], O_RDONLY, 0);
    if (fd != -1) {
      while ((nr = deltafs_pread(fd, buf, 4096, off)) > 0) {
        buf[nr] = 0;
        Print("%s", pdlfs::EscapeString(buf).c_str());
        off += nr;
      }
    }
    if (fd == -1 || nr < 0) {
      Error("cannot access file '%s'", argv[i]);
      break;
    }
  }
}

static void CopyFromLocal(int argc, char* argv[]) {
  if (argc < 2) return;
  int fd = open(argv[1], O_RDONLY);
  if (fd == -1) {
    Error("cannot open source file '%s'", argv[1]);
    return;
  }
  char buf[5000];
  static const mode_t mode = 0644;  // XXX: make this an argument?
  for (int i = 2; i < argc; i++) {
    ssize_t nr = 0, nw = 0;
    off_t off = 0;
    int dfd = deltafs_open(argv[i], O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (dfd != -1) {
      while ((nr = read(fd, buf, 4096)) > 0) {
        nw = deltafs_pwrite(dfd, buf, nr, off);
        if (nw == nr) {
          off += nw;
        } else {
          break;
        }
      }
      deltafs_close(dfd);
    }
    if (dfd == -1 || nr < 0 || nw < 0) {
      Error("cannot copy to file '%s'", argv[i]);
      break;
    } else {
      Print("OK\n");
    }
  }
  close(fd);
}

static void Exec(const std::vector<std::string>& inputs) {
  assert(inputs.size() != 0);
  const int argc = inputs.size();
  std::vector<char*> argv;
  for (size_t i = 0; i < inputs.size(); i++) {
    argv.push_back(const_cast<char*>(inputs[i].c_str()));
  }
  pdlfs::Slice cmd = inputs[0];
  if (cmd == "help" || cmd == "?") {
    PrintUsage();
  } else if (cmd == "exit" || cmd == "quit") {
    Print("exit\n");
    shutting_down_requested = true;
  } else if (cmd == "ls") {
    Listdir(argc, &argv[0]);
  } else if (cmd == "mkdir") {
    Mkdir(argc, &argv[0]);
  } else if (cmd == "mkfile") {
    Mkfile(argc, &argv[0]);
  } else if (cmd == "cpfrom") {
    CopyFromLocal(argc, &argv[0]);
  } else if (cmd == "cat") {
    Cat(argc, &argv[0]);
  } else if (cmd == "cd") {
    Chdir(argc, &argv[0]);
  } else if (cmd == "pwd") {
    Getcwd(argc, &argv[0]);
  } else {
    Print("No such command\n\n");
    PrintUsage();
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
  google::SetVersionString(PDLFS_COMMON_VERSION);
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
