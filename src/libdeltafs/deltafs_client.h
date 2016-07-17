#pragma once

/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "blkdb.h"
#include "mds_cli.h"
#include "mds_factory.h"

namespace pdlfs {

class Client {
  typedef MDS::CLI MDSClient;

 public:
  static Status Open(Client**);
  ~Client();

  struct FileInfo {
    uint64_t size;
    int fd;
  };

  Status Wopen(const Slice& path, int mode, FileInfo*);
  Status Pwrite(int fd, const Slice& data, uint64_t off);
  Status Ropen(const Slice& path, FileInfo*);
  Status Pread(int fd, Slice* result, uint64_t off, uint64_t size, char* buf);
  Status Fdatasync(int fd);
  Status Flush(int fd);
  Status Close(int fd);

  Status Mkfile(const Slice& path, int mode);
  Status Mkdir(const Slice& path, int mode);

 private:
  class Builder;
  // No copying allowed
  void operator=(const Client&);
  Client(const Client&);
  Client() {}

  MDSFactoryImpl* mdsfty_;
  MDSClient* mdscli_;
  BlkDB* blkdb_;
  DB* db_;
};

}  // namespace pdlfs
