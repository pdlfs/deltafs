#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/fstypes.h"
#include "pdlfs-common/mdb.h"

namespace pdlfs {
#define DELTAFS_FENTRY_BUFSIZE 200 /* Buffer size for fentry encoding */

// Path and metadata information on an open file.
struct Fentry {
  Fentry() {}  // Intentionally uninitialized for performance

  mode_t file_mode() const { return stat.FileMode(); }
  std::string UntypedKeyPrefix() const;
  std::string DebugString() const;
  Slice EncodeTo(char* scratch) const;
  bool DecodeFrom(Slice* input);

  DirId pid;          // Parent directory
  std::string nhash;  // Name hash
  int zserver;        // Zeroth server of the parent directory
  Stat stat;          // A snapshot of file stat
};

// Abstract service to access file data.
class Fio {
 public:
  Fio() {}
  static Fio* Open(const Slice& fio_name, const Slice& fio_conf);
  // Opaque file handle
  struct Handle {};
  virtual ~Fio();

  virtual Status Creat(const Fentry& fentry, bool append_only, Handle** fh) = 0;
  virtual Status Open(const Fentry& fentry, bool create_if_missing,
                      bool truncate_if_exists, bool append_only,
                      uint64_t* mtime, uint64_t* size, Handle** fh) = 0;
  virtual Status Fstat(const Fentry& fentry, Handle* fh, uint64_t* mtime,
                       uint64_t* size, bool skip_cache = false) = 0;
  virtual Status Write(const Fentry& fentry, Handle* fh, const Slice& data) = 0;
  virtual Status Pwrite(const Fentry& fentry, Handle* fh, const Slice& data,
                        uint64_t off) = 0;
  virtual Status Read(const Fentry& fentry, Handle* fh, Slice* result,
                      uint64_t size, char* scratch) = 0;
  virtual Status Pread(const Fentry& fentry, Handle* fh, Slice* result,
                       uint64_t off, uint64_t size, char* scratch) = 0;
  virtual Status Ftrunc(const Fentry& fentry, Handle* fh, uint64_t size) = 0;
  virtual Status Flush(const Fentry& fentry, Handle* fh,
                       bool force_sync = false) = 0;
  virtual Status Close(const Fentry& fentry, Handle* fh) = 0;

  virtual Status Trunc(const Fentry& fentry, uint64_t size) = 0;
  virtual Status Stat(const Fentry& fentry, uint64_t* mtime,
                      uint64_t* size) = 0;
  virtual Status Drop(const Fentry& fentry) = 0;

 private:
  // No copying allowed
  void operator=(const Fio&);
  Fio(const Fio&);
};

}  // namespace pdlfs
