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
#pragma once

#include "pdlfs-common/fsdbbase.h"
#include "pdlfs-common/status.h"

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

  Stat stat;          // A snapshot of file stat
  std::string nhash;  // Name hash
  DirId pid;          // Parent directory
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
  int zserver;  // Zeroth server of the parent directory
#endif
};

// Abstract service to access file data.
class Fio {
 public:
  static Fio* Open(const char* fio_name, const char* fio_conf);
#ifndef NDEBUG
  class Handle {  // Allows dynamic type checks
   protected:
    virtual ~Handle();
  };
#else
  // Opaque file handle
  struct Handle {};
#endif
  virtual ~Fio();
  Fio() {}

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

#ifndef NDEBUG
inline Fio::Handle::~Handle() {}
#endif

}  // namespace pdlfs
