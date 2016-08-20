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

// Path and metadata information on an open file.
struct Fentry {
  Fentry() {}
  bool DecodeFrom(Slice* input);
  Slice EncodeTo(char* scratch) const;
  static Slice ExtractUntypedKeyPrefix(const Slice& encoding);
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

  virtual Status Creat(const Slice& fentry, Handle** fh) = 0;
  virtual Status Open(const Slice& fentry, bool create_if_missing,
                      bool truncate_if_exists, uint64_t* mtime, uint64_t* size,
                      Handle** fh) = 0;
  virtual Status Stat(const Slice& fentry, Handle* fh, uint64_t* mtime,
                      uint64_t* size, bool skip_cache = false) = 0;
  virtual Status Write(const Slice& fentry, Handle* fh, const Slice& data) = 0;
  virtual Status Pwrite(const Slice& fentry, Handle* fh, const Slice& data,
                        uint64_t off) = 0;
  virtual Status Read(const Slice& fentry, Handle* fh, Slice* result,
                      uint64_t size, char* scratch) = 0;
  virtual Status Pread(const Slice& fentry, Handle* fh, Slice* result,
                       uint64_t off, uint64_t size, char* scratch) = 0;
  virtual Status Truncate(const Slice& fentry, Handle* fh, uint64_t size) = 0;
  virtual Status Flush(const Slice& fentry, Handle* fh,
                       bool force_sync = false) = 0;
  virtual Status Close(const Slice& fentry, Handle* fh) = 0;
  virtual Status Drop(const Slice& fentry) = 0;

 private:
  // No copying allowed
  void operator=(const Fio&);
  Fio(const Fio&);
};

}  // namespace pdlfs
