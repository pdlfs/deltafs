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
    uint64_t size;
    int fd;
  };

  Status Fopen(const Slice& path, int flags, int mode, FileInfo*);
  Status Write(int fd, const Slice& data);
  Status Pwrite(int fd, const Slice& data, uint64_t off);
  Status Read(int fd, Slice* result, uint64_t size, char* buf);
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
  Client();

  struct File {
    Fio::Handle* fh;
    File* next_hash;
    size_t key_length;
    uint32_t hash;
    uint32_t refs;
    uint32_t seq_write;
    uint32_t seq_flush;
    char key_data[1];  // Beginning of key
    Slice fentry_encoding() const { return key(); }
    Slice key() const {
      return Slice(key_data, key_length);  //
    }
  };

  // State below are protected by mutex_
  port::Mutex mutex_;
  File* FetchFile(int fd);
  size_t Append(File*);
  void Remove(size_t idx);
  size_t OpenFile(const Slice& fentry_encoding, Fio::Handle*);
  Fio::Handle* FetchFileHandle(const Slice& fentry_encoding);
  bool Unref(File*);
  HashTable<File> file_table_;
  File** open_files_;
  size_t num_open_files_;
  size_t next_file_;

  // Constant after construction
  size_t max_open_files_;
  MDSFactoryImpl* mdsfty_;
  MDSClient* mdscli_;
  Fio* fio_;
};

}  // namespace pdlfs
