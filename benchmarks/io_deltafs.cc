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

static const mode_t kFilePerms =
    (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);  // rw-r--r--
static const mode_t kDirPerms =
    (S_IRWXU | S_IRGRP | S_IROTH | S_IXGRP | S_IXOTH);  // rwxr-x-r-x

static Status IOError(const std::string& target) {
  if (errno != 0) {
    return Status::IOError(target, strerror(errno));
  } else {
    return Status::IOError(target);
  }
}

// IOClient implementation layered on top of deltafs API.
class DeltafsClient : public IOClient {
 public:
  explicit DeltafsClient() {}
  virtual ~DeltafsClient() {}

  // Common FS operations
  virtual Status NewFile(const std::string& path);
  virtual Status MakeDirectory(const std::string& path);
  virtual Status GetAttr(const std::string& path);
  virtual Status Append(const std::string& path, const char*, size_t);

  virtual Status Dispose();
  virtual Status Init();
};

Status DeltafsClient::Init() {
  Status s;
  struct stat ignored_stat;
  int r = deltafs_stat("/", &ignored_stat);
  if (r != 0) {
    return IOError("/");
  } else {
    return s;
  }
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
  printf("deltafs_mkfile %s ... ", p);
#endif
  Status s;
  if (deltafs_mkfile(p, kFilePerms) != 0) {
    s = IOError(path);
  }
#if VERBOSE >= 10
  printf("%s\n", s.ToString().c_str());
#endif
  return s;
}

Status DeltafsClient::MakeDirectory(const std::string& path) {
  const char* p = path.c_str();
#if VERBOSE >= 10
  printf("deltafs_mkdir %s ... ", p);
#endif
  Status s;
  if (deltafs_mkdir(p, kDirPerms) != 0) {
    s = IOError(path);
  }
#if VERBOSE >= 10
  printf("%s\n", s.ToString().c_str());
#endif
  return s;
}

Status DeltafsClient::GetAttr(const std::string& path) {
  const char* p = path.c_str();
#if VERBOSE >= 10
  printf("deltafs_stat %s ... ", p);
#endif
  Status s;
  struct stat statbuf;
  if (deltafs_stat(p, &statbuf) != 0) {
    s = IOError(path);
  }
#if VERBOSE >= 10
  printf("%s\n", s.ToString().c_str());
#endif
  return s;
}

Status DeltafsClient::Append(const std::string& path, const char* data,
                             size_t size) {
  const char* p = path.c_str();
#if VERBOSE >= 10
  printf("deltafs_open/write %s ... ", p);
#endif
  Status s;
  struct stat statbuf;
  int fd = deltafs_open(p, O_WRONLY | O_CREAT, kFilePerms, &statbuf);
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
  printf("%s\n", s.ToString().c_str());
#endif
  return s;
}

static void InstallDeltafsOpts(const Slice& conf_str) {
  std::vector<std::string> confs;
  SplitString(&confs, conf_str);
  for (size_t i = 0; i < confs.size(); i++) {
    std::vector<std::string> kv;
    SplitString(&kv, confs[i], ':', 1);
    if (kv.size() == 2) {
      const char* k = kv[0].c_str();
      const char* v = kv[1].c_str();
      setenv(k, v, 1);  // Override the existing one
#if VERBOSE >= 10
      printf("%s -> %s\n", k, v);
#endif
    }
  }
}

IOClient* IOClient::Deltafs(const IOClientOptions& options) {
  DeltafsClient* cli = new DeltafsClient;
  InstallDeltafsOpts(options.conf_str);
#if defined(PDLFS_GLOG)
  const char* argv0 = "io_deltafs";
  if (options.argc > 0 && options.argv != NULL) {
    argv0 = options.argv[0];
  }
  google::InitGoogleLogging(argv0);
  google::InstallFailureSignalHandler();
#endif
  return cli;
}

}  // namespace ioclient
}  // namespace pdlfs
