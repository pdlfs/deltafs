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

#include "deltafs_format.h"
#include "mds_cli.h"
#include "mds_factory.h"

namespace pdlfs {

// Deltafs client API.  Implementation is thread-safe.
class Client {
  typedef MDS::CLI MDSClient;

 public:
  static Status Open(Client**);
  ~Client();

  Status Fopen(const Slice& path, int flags, mode_t mode, FileInfo*);
  Status Write(int fd, const Slice& data);
  Status Pwrite(int fd, const Slice& data, uint64_t off);
  Status Read(int fd, Slice* result, uint64_t size, char* buf);
  Status Pread(int fd, Slice* result, uint64_t off, uint64_t size, char* buf);
  Status Ftruncate(int fd, uint64_t len);
  Status Fstat(int fd, Stat*);
  Status Fdatasync(int fd);
  Status Flush(int fd);
  Status Close(int fd);

  Status Access(const Slice& path, int mode);
  Status Accessdir(const Slice& path, int mode);
  Status Listdir(const Slice& path, std::vector<std::string>* names);
  Status Lstat(const Slice& path, Stat* result);
  Status Getattr(const Slice& path, Stat* result);
  Status Mkfile(const Slice& path, mode_t mode);
  Status Mkdirs(const Slice& path, mode_t mode);
  Status Mkdir(const Slice& path, mode_t mode);
  Status Chmod(const Slice& path, mode_t mode);
  Status Unlink(const Slice& path);

  mode_t Umask(mode_t mode);
  Status Getcwd(char* buf, size_t size);
  Status Chroot(const Slice& path);
  Status Chdir(const Slice& path);

 private:
  class Builder;
  Client(size_t max_open_files);  // Called only by Client::Builder
  // No copying allowed
  void operator=(const Client&);
  Client(const Client&);

  // State for each opened file
  struct File {
    size_t encoding_length;
    File* next;
    File* prev;
    Fio::Handle* fh;
    mode_t mode;
    uid_t uid;
    gid_t gid;
    int flags;
    uint32_t seq_write;  // Latest write
    uint32_t seq_flush;  // Latest metadata update
    int refs;

    char encoding_data[1];  // Beginning of fentry encoding
    Slice fentry_encoding() const {
      return Slice(encoding_data, encoding_length);
    }
  };

  // State below is protected by mutex_
  port::Mutex mutex_;
  mode_t MaskMode(mode_t mode);
  port::AtomicPointer mask_;  // Can be updated by umask
  Status ExpandPath(Slice* path, std::string* scratch);
  port::AtomicPointer has_curroot_set_;
  std::string curroot_;  // Set by chroot
  port::AtomicPointer has_curdir_set_;
  std::string curdir_;  // Set by chdir
  File* FetchFile(int fd);
  size_t Alloc(File*);
  File* Free(size_t idx);
  size_t Open(const Slice& encoding, int flags, const Stat&, Fio::Handle*);
  bool IsWriteOk(const File*);
  bool IsReadOk(const File*);
  void Unref(File*);
  File dummy_;  // File table as a doubly linked list
  File** fds_;  // File descriptor table
  size_t num_open_fds_;
  size_t fd_slot_;

  // Constant after construction
  size_t max_open_fds_;
  MDSFactoryImpl* mdsfty_;
  MDSClient* mdscli_;
  Fio* fio_;
};

}  // namespace pdlfs
