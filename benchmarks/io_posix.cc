/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "io_client.h"

#include "pdlfs-common/slice.h"
#include "pdlfs-common/strutil.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

namespace pdlfs {
namespace ioclient {

static Status IOError(const std::string& target) {
  if (errno != 0) {
    return Status::IOError(target, strerror(errno));
  } else {
    return Status::IOError(target);
  }
}

static Status Mkdir(const std::string& path) {
  if (access(path.c_str(), F_OK) != 0) {
    int r = mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
    if (r != 0 && errno != EEXIST) {
      return IOError(path);
    }
  }

  return Status::OK();
}

// IOClient implementation that uses local FS as its backend file system.
class PosixClient : public IOClient {
 public:
  explicit PosixClient() : mp_size_(0) {}
  virtual ~PosixClient() {}

  // Common FS operations
  virtual Status NewFile(const std::string& path);
  virtual Status DelFile(const std::string& path);
  virtual Status MakeDirectory(const std::string& path);
  virtual Status GetAttr(const std::string& path);
  virtual Status Append(const std::string& path, const char*, size_t);

  virtual Status Dispose();
  virtual Status Init();

 private:
  friend class IOClient;
  void SetMountPoint(const std::string& mp);
  std::string path_buf_;  // Buffer space for generating file paths
  size_t mp_size_;        // Length of the mount point
};

Status PosixClient::Init() {
  assert(mp_size_ != 0);                 // Mount point is non-empty
  assert(path_buf_.size() >= mp_size_);  // Mount point is valid
  path_buf_.resize(mp_size_);
  return Mkdir(path_buf_);
}

Status PosixClient::Dispose() {
  // Do nothing
  return Status::OK();
}

Status PosixClient::NewFile(const std::string& path) {
  path_buf_.resize(mp_size_);
  path_buf_.append(path);
  const char* filename = path_buf_.c_str();
#if VERBOSE >= 10
  printf("mknod %s...\n", filename);
#endif
  Status s;
  if (mknod(filename, S_IRWXU | S_IRWXG | S_IRWXO, S_IFREG) != 0) {
    s = IOError(path_buf_);
  } else {
    s = Status::OK();
  }
#if VERBOSE >= 10
  printf("%s\n", s.ToString().c_str());
#endif
  return s;
}

Status PosixClient::DelFile(const std::string& path) {
  path_buf_.resize(mp_size_);
  path_buf_.append(path);
  const char* filename = path_buf_.c_str();
#if VERBOSE >= 10
  printf("unlink %s...\n", filename);
#endif
  Status s;
  if (unlink(filename) != 0) {
    s = IOError(path_buf_);
  } else {
    s = Status::OK();
  }
#if VERBOSE >= 10
  printf("%s\n", s.ToString().c_str());
#endif
  return s;
}

Status PosixClient::MakeDirectory(const std::string& path) {
  path_buf_.resize(mp_size_);
  path_buf_.append(path);
  const char* dirname = path_buf_.c_str();
#if VERBOSE >= 10
  printf("mkdir %s...\n", dirname);
#endif
  Status s;
  if (mkdir(dirname, S_IRWXU | S_IRWXG | S_IRWXO) != 0) {
    s = IOError(path_buf_);
  } else {
    s = Status::OK();
  }
#if VERBOSE >= 10
  printf("%s\n", s.ToString().c_str());
#endif
  return s;
}

Status PosixClient::GetAttr(const std::string& path) {
  path_buf_.resize(mp_size_);
  path_buf_.append(path);
  const char* nodename = path_buf_.c_str();
#if VERBOSE >= 10
  printf("stat %s...\n", nodename);
#endif
  Status s;
  struct stat statbuf;
  if (stat(nodename, &statbuf) != 0) {
    s = IOError(path_buf_);
  } else {
    s = Status::OK();
  }
#if VERBOSE >= 10
  printf("%s\n", s.ToString().c_str());
#endif
  return s;
}

Status PosixClient::Append(const std::string& path, const char* data,
                           size_t size) {
  path_buf_.resize(mp_size_);
  path_buf_.append(path);
  const char* filename = path_buf_.c_str();
#if VERBOSE >= 10
  printf("open+w %s...\n", filename);
#endif
  Status s;
  const mode_t mode = S_IRWXU | S_IRWXG | S_IRWXO;
  int fd = open(filename, O_WRONLY | O_APPEND | O_CREAT, mode);
  if (fd == -1) {
    s = IOError(path_buf_);
  } else {
    ssize_t n = write(fd, data, size);
    if (n != size) {
      s = IOError(path_buf_);
    }
    close(fd);
  }
#if VERBOSE >= 10
  printf("%s\n", s.ToString().c_str());
#endif
  return s;
}

// REQUIRES: mp is a valid absolute file system path
void PosixClient::SetMountPoint(const std::string& mp) {
  assert(mp.size() != 0);
  assert(mp[0] == '/');
  mp_size_ = mp.size();
  path_buf_ = mp;
}

// Fetch the mount point from the given configuration string
static std::string MP(const IOClientOptions& options) {
  std::string mp =
      "/tmp/ioclient";  // Allow falling back to the default mount point
  std::vector<std::string> confs;
  SplitString(&confs, options.conf_str);
  for (size_t i = 0; i < confs.size(); i++) {
    Slice input = confs[i];
    if (input.size() != 0) {
      if (input.starts_with("mount_point=")) {
        input.remove_prefix(Slice("mount_point=").size());
        mp = input.ToString();
      }
    }
  }
#if VERBOSE >= 2
  if (options.rank == 0) {
    printf("mount_point -> %s\n", mp.c_str());
  }
#endif
  return mp;
}

IOClient* IOClient::Default(const IOClientOptions& options) {
  PosixClient* cli = new PosixClient;
  cli->SetMountPoint(MP(options));
  cli->path_buf_.reserve(256);
  return cli;
}

}  // namespace ioclient
}  // namespace pdlfs
