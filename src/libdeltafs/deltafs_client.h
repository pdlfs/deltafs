#pragma once

/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/fio.h"
#include "pdlfs-common/map.h"

#include "mds_cli.h"
#include "mds_factory.h"

namespace pdlfs {

class Client {
  typedef MDS::CLI MDSClient;

 public:
  static Status Open(Client**);
  ~Client();

  struct FileInfo {
    Stat stat;
    int fd;
  };

  Status Fopen(const Slice& path, int flags, int mode, FileInfo*);
  Status Write(int fd, const Slice& data);
  Status Pwrite(int fd, const Slice& data, uint64_t off);
  Status Read(int fd, Slice* result, uint64_t size, char* buf);
  Status Pread(int fd, Slice* result, uint64_t off, uint64_t size, char* buf);
  Status Ftruncate(int fd, uint64_t len);
  Status Fstat(int fd, Stat*);
  Status Fdatasync(int fd);
  Status Flush(int fd);
  Status Close(int fd);

  Status Getattr(const Slice& path, Stat*);
  Status Mkfile(const Slice& path, int mode);
  Status Mkdir(const Slice& path, int mode);

 private:
  class Builder;
  // No copying allowed
  void operator=(const Client&);
  Client(const Client&);
  Client();

  struct File {
    size_t encoding_length;
    File* next;
    File* prev;
    Fio::Handle* fh;
    mode_t mode;
    uid_t uid;
    gid_t gid;
    int flags;
    uint32_t seq_write;
    uint32_t seq_flush;
    int refs;

    char encoding_data[1];  // Beginning of fentry encoding
    Slice fentry_encoding() const {
      return Slice(encoding_data, encoding_length);
    }
  };

  // State below are protected by mutex_
  port::Mutex mutex_;
  File* FetchFile(int fd);
  size_t Alloc(File*);
  File* Free(size_t idx);
  size_t Open(const Slice& encoding, int flags, const Stat&, Fio::Handle*);
  bool IsWriteOk(const File*);
  bool IsReadOk(const File*);
  void Unref(File*);
  File dummy_;
  File** fds_;
  size_t num_open_fds_;
  size_t fd_cursor_;

  // Constant after construction
  size_t max_open_fds_;
  MDSFactoryImpl* mdsfty_;
  MDSClient* mdscli_;
  Fio* fio_;
};

}  // namespace pdlfs
