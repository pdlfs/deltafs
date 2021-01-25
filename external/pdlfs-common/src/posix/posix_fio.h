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

#include "pdlfs-common/fio.h"
#include "posix_env.h"

namespace pdlfs {

class PosixFio : public Fio {
 public:
  explicit PosixFio(const char* root) : root_(root) {
    Env::Default()->CreateDir(root);
  }

  virtual ~PosixFio() {
    // Do nothing
  }

  virtual Status Creat(const Fentry& fentry, bool append_only, Handle** fh);
  virtual Status Open(const Fentry& fentry, bool create_if_missing,
                      bool truncate_if_exists, bool append_only,
                      uint64_t* mtime, uint64_t* size, Handle** fh);
  virtual Status Fstat(const Fentry& fentry, Handle* fh, uint64_t* mtime,
                       uint64_t* size, bool skip_cache = false);
  virtual Status Write(const Fentry& fentry, Handle* fh, const Slice& buf);
  virtual Status Pwrite(const Fentry& fentry, Handle* fh, const Slice& buf,
                        uint64_t off);
  virtual Status Read(const Fentry& fentry, Handle* fh, Slice* result,
                      uint64_t size, char* scratch);
  virtual Status Pread(const Fentry& fentry, Handle* fh, Slice* result,
                       uint64_t off, uint64_t size, char* scratch);
  virtual Status Ftrunc(const Fentry& fentry, Handle* fh, uint64_t size);
  virtual Status Flush(const Fentry& fentry, Handle* fh,
                       bool force_sync = false);
  virtual Status Close(const Fentry& fentry, Handle* fh);
  virtual Status Trunc(const Fentry& fentry, uint64_t size);
  virtual Status Stat(const Fentry& fentry, uint64_t* mtime, uint64_t* size);
  virtual Status Drop(const Fentry& fentry);

 private:
  std::string FileName(const Fentry &fentry);
  std::string root_;
};

}  // namespace pdlfs
