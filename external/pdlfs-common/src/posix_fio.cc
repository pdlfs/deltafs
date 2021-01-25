/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "posix_fio.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

namespace pdlfs {

static std::string PosixName(const Fentry& fentry) {
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

std::string PosixFio::FileName(const Fentry& fentry) {
  return root_ + "/" + PosixName(fentry);
}

#ifndef NDEBUG
class PosixFd : public Fio::Handle {
 public:
  explicit PosixFd(int fd) : fd(fd) {}
  virtual ~PosixFd() {}

  int fd;
};
#endif

static inline Fio::Handle* NewHandle(int fd) {
#ifndef NDEBUG
  return new PosixFd(fd);
#else
  intptr_t tmp = static_cast<intptr_t>(fd);
  Fio::Handle* h = reinterpret_cast<Fio::Handle*>(tmp);
  return h;
#endif
}

static inline void FreeHandle(Fio::Handle* fh) {
#ifndef NDEBUG
  delete dynamic_cast<PosixFd*>(fh);
#endif
}

static inline int ToFd(Fio::Handle* fh) {
#ifndef NDEBUG
  assert(fh != NULL);
  return dynamic_cast<PosixFd*>(fh)->fd;
#else
  intptr_t tmp = reinterpret_cast<intptr_t>(fh);
  return static_cast<int>(tmp);
#endif
}

Status PosixFio::Creat(const Fentry& fentry, bool append_only, Handle** fh) {
  Status s;
  const int o1 = append_only ? O_APPEND : 0;
  std::string fname = FileName(fentry);
  int fd = open(fname.c_str(), O_RDWR | O_CREAT | O_TRUNC | o1, DEFFILEMODE);
  if (fd != -1) {
    *fh = NewHandle(fd);
  } else {
    s = IOError(fname, errno);
  }
  return s;
}

Status PosixFio::Open(const Fentry& fentry, bool create_if_missing,
                      bool truncate_if_exists, bool append_only,
                      uint64_t* mtime, uint64_t* size, Handle** fh) {
  Status s;
  const int o1 = create_if_missing ? O_CREAT : 0;
  const int o2 = truncate_if_exists ? O_TRUNC : 0;
  const int o3 = append_only ? O_APPEND : 0;
  std::string fname = FileName(fentry);
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
      *mtime = 1000LLU * 1000LLU * statbuf.st_mtime;
      *size = statbuf.st_size;
    }
  }

  return s;
}

Status PosixFio::Fstat(const Fentry& fentry, Handle* fh, uint64_t* mtime,
                       uint64_t* size, bool skip_cache) {
  Status s;
  struct stat statbuf;
  int r = fstat(ToFd(fh), &statbuf);
  if (r != 0) return IOError(FileName(fentry), errno);
  *mtime = 1000LLU * 1000LLU * statbuf.st_mtime;
  *size = statbuf.st_size;
  return s;
}

Status PosixFio::Write(const Fentry& fentry, Handle* fh, const Slice& buf) {
  Status s;
  ssize_t n = write(ToFd(fh), buf.data(), buf.size());
  if (n == -1) return IOError(FileName(fentry), errno);
  return s;
}

Status PosixFio::Pwrite(const Fentry& fentry, Handle* fh, const Slice& buf,
                        uint64_t off) {
  Status s;
  ssize_t n = pwrite(ToFd(fh), buf.data(), buf.size(), off);
  if (n == -1) return IOError(FileName(fentry), errno);
  return s;
}

Status PosixFio::Read(const Fentry& fentry, Handle* fh, Slice* result,
                      uint64_t size, char* scratch) {
  Status s;
  *result = Slice();
  ssize_t n = read(ToFd(fh), scratch, size);
  if (n == -1) return IOError(FileName(fentry), errno);
  *result = Slice(scratch, n);
  return s;
}

Status PosixFio::Pread(const Fentry& fentry, Handle* fh, Slice* result,
                       uint64_t off, uint64_t size, char* scratch) {
  Status s;
  *result = Slice();
  ssize_t n = pread(ToFd(fh), scratch, size, off);
  if (n == -1) return IOError(FileName(fentry), errno);
  *result = Slice(scratch, n);
  return s;
}

Status PosixFio::Ftrunc(const Fentry& fentry, Handle* fh, uint64_t size) {
  Status s;
  int r = ftruncate(ToFd(fh), size);
  if (r != 0) return IOError(FileName(fentry), errno);
  return s;
}

Status PosixFio::Flush(const Fentry& fentry, Handle* fh, bool force_sync) {
  Status s;
  if (!force_sync) return s;
  int r = fdatasync(ToFd(fh));
  if (r != 0) return IOError(FileName(fentry), errno);
  return s;
}

Status PosixFio::Close(const Fentry& fentry, Handle* fh) {
  Status s;
  close(ToFd(fh));
  FreeHandle(fh);
  return s;
}

Status PosixFio::Trunc(const Fentry& fentry, uint64_t size) {
  Status s;
  std::string fname = FileName(fentry);
  int r = truncate(fname.c_str(), size);
  if (r != 0) return IOError(fname, errno);
  return s;
}

Status PosixFio::Stat(const Fentry& fentry, uint64_t* mtime, uint64_t* size) {
  Status s;
  std::string fname = FileName(fentry);
  struct stat statbuf;
  int r = stat(fname.c_str(), &statbuf);
  if (r != 0) return IOError(fname, errno);
  *mtime = 1000LLU * 1000LLU * statbuf.st_mtime;
  *size = statbuf.st_size;
  return s;
}

Status PosixFio::Drop(const Fentry& fentry) {
  Status s;
  std::string fname = FileName(fentry);
  int r = unlink(fname.c_str());
  if (r != 0) return IOError(fname, errno);
  return s;
}

}  // namespace pdlfs
