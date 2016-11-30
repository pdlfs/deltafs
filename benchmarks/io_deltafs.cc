/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "io_client.h"

#include "deltafs/deltafs_api.h"
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/strutil.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <vector>

#if defined(PDLFS_GLOG)
#include <glog/logging.h>
#endif

namespace pdlfs {
namespace ioclient {

static const mode_t IO_FILEPERMS =
    (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);  // rw-r--r--
static const mode_t IO_DIRPERMS =
    (S_IRWXU | S_IRGRP | S_IROTH | S_IXGRP | S_IXOTH);  // rwxr-x-r-x

static Status IOError(const std::string& target) {
  if (errno != 0) {
    return Status::IOError(target, strerror(errno));
  } else {
    return Status::IOError(target);
  }
}

// Be very verbose
static const bool kVVerbose = false;
// Be verbose
static const bool kVerbose = true;

// IOClient implementation layered on top of deltafs API.
class DeltafsClient : public IOClient {
 public:
  explicit DeltafsClient() {}
  virtual ~DeltafsClient() {}

  // Common FS operations
  virtual Status NewFile(const std::string& path);
  virtual Status DelFile(const std::string& path);
  virtual Status MakeDirectory(const std::string& path);
  virtual Status GetAttr(const std::string& path);
  virtual Status Append(const std::string& path, const char*, size_t);

  virtual Status Dispose();
  virtual Status Init();
};

// Convenient method for printing status lines
static inline void print(const Status& s) {
  printf("> %s\n", s.ToString().c_str());
}

Status DeltafsClient::Init() {
  Status s;
#if VERBOSE >= 10
  if (kVVerbose) printf("deltafs_init...\n");
#endif
  int r = deltafs_nonop();
  if (r != 0) {
    s = IOError(".");
  }
#if VERBOSE >= 10
  if (kVVerbose) print(s);
#endif
  return s;
}

Status DeltafsClient::Dispose() {
#if defined(PDLFS_GLOG)
  google::FlushLogFiles(google::GLOG_INFO);
#endif
  return Status::OK();
}

Status DeltafsClient::NewFile(const std::string& path) {
  const char* p = path.c_str();
#if VERBOSE >= 10
  if (kVVerbose) printf("deltafs_mkfile %s...\n", p);
#endif
  Status s;
  int r = deltafs_mkfile(p, IO_FILEPERMS);
  if (r != 0) {
    s = IOError(path);
  }
#if VERBOSE >= 10
  if (kVVerbose) print(s);
#endif
  return s;
}

Status DeltafsClient::DelFile(const std::string& path) {
  const char* p = path.c_str();
#if VERBOSE >= 10
  if (kVVerbose) printf("deltafs_unlink %s...\n", p);
#endif
  Status s;
  int r = deltafs_unlink(p);
  if (r != 0) {
    s = IOError(path);
  }
#if VERBOSE >= 10
  if (kVVerbose) print(s);
#endif
  return s;
}

Status DeltafsClient::MakeDirectory(const std::string& path) {
  const char* p = path.c_str();
#if VERBOSE >= 10
  if (kVVerbose) printf("deltafs_mkdir %s...\n", p);
#endif
  Status s;
  int r = deltafs_mkdir(p, IO_DIRPERMS);
  if (r != 0) {
    s = IOError(path);
  }
#if VERBOSE >= 10
  if (kVVerbose) print(s);
#endif
  return s;
}

Status DeltafsClient::GetAttr(const std::string& path) {
  const char* p = path.c_str();
#if VERBOSE >= 10
  if (kVVerbose) printf("deltafs_stat %s...\n", p);
#endif
  Status s;
  struct stat statbuf;
  int r = deltafs_stat(p, &statbuf);
  if (r != 0) {
    s = IOError(path);
  }
#if VERBOSE >= 10
  if (kVVerbose) print(s);
#endif
  return s;
}

Status DeltafsClient::Append(const std::string& path, const char* data,
                             size_t size) {
  const char* p = path.c_str();
#if VERBOSE >= 10
  if (kVVerbose) printf("deltafs_open+w %s...\n", p);
#endif
  Status s;
  struct stat statbuf;
  int fd = deltafs_openstat(p, O_WRONLY | O_CREAT, IO_FILEPERMS, &statbuf);
  if (fd == -1) {
    s = IOError(path);
  } else {
    ssize_t n = deltafs_pwrite(fd, data, size, statbuf.st_size);
    if (n != size) {
      s = IOError(path);
    }
    deltafs_close(fd);
  }
#if VERBOSE >= 10
  if (kVVerbose) print(s);
#endif
  return s;
}

static void MaybeSetVerboseLevel() {
#if defined(PDLFS_GLOG)
  const char* env = getenv("DELTAFS_Verbose");
  if (env != NULL && strlen(env) != 0) {
    FLAGS_v = atoi(env);  // Update log verbose level
  }
#else
// Do nothing
#endif
}

static void MaybeLogToStderr() {
#if defined(PDLFS_GLOG)
  const char* env = getenv("DELTAFS_LogToStderr");
  if (env != NULL && strlen(env) != 0) {
    FLAGS_logtostderr = true;
  }
#else
// Do nothing
#endif
}

static void InstallDeltafsOpts(const IOClientOptions& options) {
  std::vector<std::string> confs;
  SplitString(&confs, options.conf_str);
  for (size_t i = 0; i < confs.size(); i++) {
    std::vector<std::string> kv;
    SplitString(&kv, confs[i], ':', 1);
    if (kv.size() == 2) {
      const char* k = kv[0].c_str();
      const char* v = kv[1].c_str();
      setenv(k, v, 1);  // Override the existing one

#if VERBOSE >= 2
      if (options.rank == 0) {
        if (kVerbose) {
          printf("%s -> %s\n", k, v);
        }
      }
#endif
    }
  }

  // XXX: Must get called before glog is initialized
  MaybeSetVerboseLevel();
  MaybeLogToStderr();
}

IOClient* IOClient::Deltafs(const IOClientOptions& options) {
  DeltafsClient* cli = new DeltafsClient;
  InstallDeltafsOpts(options);
#if defined(PDLFS_GLOG)
  const char* argv0 = "io_deltafs";
  if (options.argc > 0 && options.argv != NULL) {
    argv0 = options.argv[0];
  }

  // XXX: Deltafs relies on glog to print important messages so we
  // setup it here.
  google::InitGoogleLogging(argv0);
  google::InstallFailureSignalHandler();
#endif
  return cli;
}

}  // namespace ioclient
}  // namespace pdlfs
