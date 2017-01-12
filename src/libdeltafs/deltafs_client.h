#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
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

  Status Fopen(const char* path, int flags, mode_t mode, FileInfo* result);
  Status Fopenat(int fd, const char* path, int flags, mode_t mode,
                 FileInfo* reuslt);
  Status Write(int fd, const Slice& data);
  Status Pwrite(int fd, const Slice& data, uint64_t off);
  Status Read(int fd, Slice* result, uint64_t size, char* buf);
  Status Pread(int fd, Slice* result, uint64_t off, uint64_t size, char* buf);
  Status Ftruncate(int fd, uint64_t len);
  Status Fstat(int fd, Stat*);
  Status Fdatasync(int fd);
  Status Flush(int fd);
  Status Close(int fd);

  Status Access(const char* path, int mode);
  Status Accessdir(const char* path, int mode);
  Status Listdir(const char* path, std::vector<std::string>* names);
  Status Truncate(const char* path, uint64_t len);
  Status Lstat(const char* path, Stat* result);
  Status Getattr(const char* path, Stat* result);
  Status Mkfile(const char* path, mode_t mode);
  Status Mkdirs(const char* path, mode_t mode);
  Status Mkdir(const char* path, mode_t mode);
  Status Chmod(const char* path, mode_t mode);
  Status Chown(const char* path, uid_t usr, gid_t grp);
  Status Unlink(const char* path);

  Status Getcwd(char* buf, size_t size);
  Status Chroot(const char* path);
  Status Chdir(const char* path);

  mode_t Umask(mode_t mode);

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
    int flags;
    uint32_t seq_flush;  // Latest file metadata update
    uint32_t seq_write;  // Latest data write
    int refs;

    char encoding_data[1];  // Beginning of fentry encoding
    Slice fentry_encoding() const {
      return Slice(encoding_data, encoding_length);
    }
  };

  struct FileAndEntry {
    const Fentry* ent;
    File* file;
  };
  // REQUIRES: mutex_ has been locked
  Status InternalOpen(const Slice& p, int flags, mode_t mode, FileAndEntry* at,
                      FileInfo* result);
  // REQUIRES: mutex_ has been locked
  Status InternalFdatasync(File* file, const Fentry& ent);
  // REQUIRES: mutex_ has been locked
  Status InternalFlush(File* file, const Fentry& ent);

  // State below is protected by mutex_
  port::Mutex mutex_;
  mode_t MaskMode(mode_t mode);
  port::AtomicPointer mask_;  // Can be updated by umask
  Status ExpandPath(Slice* path, std::string* scratch);
  port::AtomicPointer has_curroot_set_;
  std::string curroot_;  // Set by chroot
  port::AtomicPointer has_curdir_set_;
  std::string curdir_;  // Set by chdir
  File* FetchFile(int fd, Fentry*);
  size_t Alloc(File*);
  File* Free(size_t idx);
  size_t Open(const Slice& encoding, int flags, Fio::Handle*);
  bool IsWriteOk(const File*);
  bool IsReadOk(const File*);
  void Unref(File*, const Fentry&);
  File dummy_;  // File table as a doubly linked list
  File** fds_;  // File descriptor table
  size_t num_open_fds_;
  size_t fd_slot_;

  // Constant after construction
  size_t max_open_fds_;
  MDSFactoryImpl* mdsfty_;
  MDSClient* mdscli_;
  Fio* fio_;
  Env* env_;
};

}  // namespace pdlfs
