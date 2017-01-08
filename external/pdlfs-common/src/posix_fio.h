#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "posix_env.h"
#include "posix_fio.h"

namespace pdlfs {

class PosixFio : public Fio {
  static std::string ToFileName(const Fentry& fentry) {
    std::string key_prefix = fentry.UntypedKeyPrefix();
    char tmp[200];
    sprintf(tmp, "F_");
    char* p = tmp + 2;
    for (size_t i = 0; i < key_prefix.size(); i++) {
      sprintf(p, "%02X", static_cast<unsigned char>(key_prefix[i]));
      p += 2;
    }
    return tmp;
  }

#ifndef NDEBUG
  class PosixFd : public Handle {
   public:
    explicit PosixFd(int fd) : fd(fd) {}
    virtual ~PosixFd() {}

    int fd;
  };
#endif

  static Handle* NewHandle(int fd) {
#ifndef NDEBUG
    return new PosixFd(fd);
#else
    intptr_t tmp = static_cast<intptr_t>(fd);
    return (Handle*)tmp;
#endif
  }

  static void Free(Handle* fh) {
#ifndef NDEBUG
    delete dynamic_cast<PosixFd*>(fh);
#endif
  }

  static int ToFd(Handle* fh) {
#ifndef NDEBUG
    assert(fh != NULL);
    return dynamic_cast<PosixFd*>(fh)->fd;
#else
    return (intptr_t)fh;
#endif
  }

 public:
  explicit PosixFio(const Slice& root) {
    Env::Default()->CreateDir(root);
    root_ = root.ToString();
  }

  virtual ~PosixFio() {
    // Do nothing
  }

  virtual Status Creat(const Fentry& fentry, bool append_only, Handle** fh) {
    Status s;
    const int o1 = append_only ? O_APPEND : 0;
    std::string fname = root_ + "/";
    fname += ToFileName(fentry);
    const char* f = fname.c_str();
    int fd = open(f, O_RDWR | O_CREAT | O_TRUNC | o1, DEFFILEMODE);
    if (fd != -1) {
      *fh = NewHandle(fd);
    } else {
      s = IOError(fname, errno);
    }
    return s;
  }

  virtual Status Open(const Fentry& fentry, bool create_if_missing,
                      bool truncate_if_exists, bool append_only,
                      uint64_t* mtime, uint64_t* size, Handle** fh) {
    Status s;
    const int o1 = create_if_missing ? O_CREAT : 0;
    const int o2 = truncate_if_exists ? O_TRUNC : 0;
    const int o3 = append_only ? O_APPEND : 0;
    std::string fname = root_ + "/";
    fname += ToFileName(fentry);
    int fd = open(fname.c_str(), O_RDWR | o1 | o2 | o3, DEFFILEMODE);
    if (fd == -1) {
      s = IOError(fname, errno);
    }

    if (s.ok()) {
      struct stat statbuf;
      int r = fstat(fd, &statbuf);
      if (r != 0) {
        s = IOError(fname, errno);
      } else {
        *fh = NewHandle(fd);
        *size = statbuf.st_size;
        *mtime = statbuf.st_mtime;
        *mtime *= 1000;
        *mtime *= 1000;
      }
    }

    return s;
  }

  virtual Status Fstat(const Fentry& fentry, Handle* fh, uint64_t* mtime,
                       uint64_t* size, bool skip_cache = false) {
    Status s;
    int fd = ToFd(fh);
    struct stat statbuf;
    int r = fstat(fd, &statbuf);
    if (r == 0) {
      *mtime = 1000LLU * 1000LLU * statbuf.st_mtime;
      *size = statbuf.st_size;
    } else {
      std::string fname = root_ + "/";
      fname += ToFileName(fentry);
      s = IOError(fname, errno);
    }

    return s;
  }

  virtual Status Write(const Fentry& fentry, Handle* fh, const Slice& buf) {
    Status s;
    int fd = ToFd(fh);
    ssize_t n = write(fd, buf.data(), buf.size());
    if (n == -1) {
      std::string fname = root_ + "/";
      fname += ToFileName(fentry);
      s = IOError(fname, errno);
    }

    return s;
  }

  virtual Status Pwrite(const Fentry& fentry, Handle* fh, const Slice& buf,
                        uint64_t off) {
    Status s;
    int fd = ToFd(fh);
    ssize_t n = pwrite(fd, buf.data(), buf.size(), off);
    if (n == -1) {
      std::string fname = root_ + "/";
      fname += ToFileName(fentry);
      s = IOError(fname, errno);
    }

    return s;
  }

  virtual Status Read(const Fentry& fentry, Handle* fh, Slice* result,
                      uint64_t size, char* scratch) {
    Status s;
    int fd = ToFd(fh);
    ssize_t n = read(fd, scratch, size);
    if (n == -1) {
      std::string fname = root_ + "/";
      fname += ToFileName(fentry);
      s = IOError(fname, errno);
    }
    if (s.ok()) {
      *result = Slice(scratch, n);
    }

    return s;
  }

  virtual Status Pread(const Fentry& fentry, Handle* fh, Slice* result,
                       uint64_t off, uint64_t size, char* scratch) {
    Status s;
    int fd = ToFd(fh);
    ssize_t n = pread(fd, scratch, size, off);
    if (n == -1) {
      std::string fname = root_ + "/";
      fname += ToFileName(fentry);
      s = IOError(fname, errno);
    }
    if (s.ok()) {
      *result = Slice(scratch, n);
    }

    return s;
  }

  virtual Status Ftrunc(const Fentry& fentry, Handle* fh, uint64_t size) {
    Status s;
    int fd = ToFd(fh);
    int r = ftruncate(fd, size);
    if (r != 0) {
      std::string fname = root_ + "/";
      fname += ToFileName(fentry);
      s = IOError(fname, errno);
    }

    return s;
  }

  virtual Status Flush(const Fentry& fentry, Handle* fh,
                       bool force_sync = false) {
    Status s;
    if (force_sync) {
      int fd = ToFd(fh);
      int r = fdatasync(fd);
      if (r != 0) {
        std::string fname = root_ + "/";
        fname += ToFileName(fentry);
        s = IOError(fname, errno);
      }
    }

    return s;
  }

  virtual Status Close(const Fentry& fentry, Handle* fh) {
    int fd = ToFd(fh);
    close(fd);
    Free(fh);
    return Status::OK();
  }

  virtual Status Trunc(const Fentry& fentry, uint64_t size) {
    Status s;
    std::string fname = root_ + "/";
    fname += ToFileName(fentry);
    int r = truncate(fname.c_str(), size);
    if (r != 0) {
      return IOError(fname, errno);
    } else {
      return s;
    }
  }

  virtual Status Stat(const Fentry& fentry, uint64_t* mtime, uint64_t* size) {
    Status s;
    std::string fname = root_ + "/";
    fname += ToFileName(fentry);
    struct stat statbuf;
    int r = stat(fname.c_str(), &statbuf);
    if (r != 0) {
      return IOError(fname, errno);
    } else {
      *mtime = 1000LLU * 1000LLU * statbuf.st_mtime;
      *size = statbuf.st_size;
      return s;
    }
  }

  virtual Status Drop(const Fentry& fentry) {
    Status s;
    std::string fname = root_ + "/";
    fname += ToFileName(fentry);
    int r = unlink(fname.c_str());
    if (r != 0) {
      return IOError(fname, errno);
    } else {
      return s;
    }
  }

 private:
  std::string root_;
};

}  // namespace pdlfs
