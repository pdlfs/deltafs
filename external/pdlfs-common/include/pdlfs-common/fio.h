#pragma once

/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/fstypes.h"
#include "pdlfs-common/mdb.h"

namespace pdlfs {

// Opaque file descriptor.
typedef int fd_t;

// Metadata information on an open file.
struct Fentry {
  Fentry() {}
  bool DecodeFrom(Slice* input);
  Slice EncodeTo(char* scratch) const;
  DirId pid;          // Parent directory
  std::string nhash;  // Name hash
  int zserver;        // Zeroth server of the parent directory
  Stat stat;          // A snapshot of file stat
};

class FileIO {
 public:
  FileIO() {}
  virtual ~FileIO();

  virtual Status Creat(const Fentry&, fd_t* fd) = 0;
  virtual Status Open(const Fentry&, bool create_if_missing,
                      bool truncate_if_exists, uint64_t* mtime, uint64_t* size,
                      fd_t* fd) = 0;
  virtual Status Write(fd_t fd, const Slice& data) = 0;
  virtual Status Pwrite(fd_t fd, const Slice& data, uint64_t off) = 0;
  virtual Status Read(fd_t fd, Slice* result, uint64_t size, char* scratch) = 0;
  virtual Status Pread(fd_t fd, Slice* result, uint64_t off, uint64_t size,
                       char* scratch) = 0;
  virtual Status Flush(fd_t fd, bool force_sync = false) = 0;
  virtual Status Close(fd_t fd) = 0;

  virtual Status GetInfo(fd_t fd, Fentry* entry, bool* dirty, uint64_t* mtime,
                         uint64_t* size) = 0;

 private:
  // No copying allowed
  void operator=(const FileIO&);
  FileIO(const FileIO&);
};

}  // namespace pdlfs
