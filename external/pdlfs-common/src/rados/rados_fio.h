#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/rwlock.h"

#include "rados_common.h"
#include "rados_conn.h"

namespace pdlfs {
namespace rados {
class RadosFio;  // File I/O implementation using rados.

class RadosFobj {
 public:
  RadosFobj(RadosFio* fio) : fio(fio), fctx(NULL) {}
  RadosFio* fio;
  rados_ioctx_t fctx;
  uint64_t mtime;  // Cached last file modification time
  uint64_t size;   // Cached file size
  uint64_t off;    // Current read/write position
  int bg_err;
  int refs;
};

class RadosFio : public Fio {
 public:
  virtual ~RadosFio();
  virtual Status Creat(const Slice& fentry, Handle** fh);
  virtual Status Open(const Slice& fentry, bool create_if_missing,
                      bool truncate_if_exists, uint64_t* mtime, uint64_t* size,
                      Handle** fh);
  virtual Status Stat(const Slice& fentry, Handle* fh, uint64_t* mtime,
                      uint64_t* size, bool skip_cache = false);
  virtual Status Write(const Slice& fentry, Handle* fh, const Slice& data);
  virtual Status Pwrite(const Slice& fentry, Handle* fh, const Slice& data,
                        uint64_t off);
  virtual Status Read(const Slice& fentry, Handle* fh, Slice* result,
                      uint64_t size, char* scratch);
  virtual Status Pread(const Slice& fentry, Handle* fh, Slice* result,
                       uint64_t off, uint64_t size, char* scratch);
  virtual Status Truncate(const Slice& fentry, Handle* fh, uint64_t size);
  virtual Status Flush(const Slice& fentry, Handle* fh,
                       bool force_sync = false);
  virtual Status Close(const Slice& fentry, Handle* fh);
  virtual Status Drop(const Slice& fentry);

 private:
  void UpdateAndUnref(RadosFobj*, int err);
  static rados_completion_t New_comp(RadosFobj*);
  static void IO_safe(rados_completion_t, void*);
  void Unref(RadosFobj*);

  RadosFio(port::Mutex* mu) : mutex_(mu), rwl_(mu) {}
  friend class RadosConn;
  port::Mutex* mutex_;
  RWLock rwl_;  // Enforce serialized execution on some operations
  std::string pool_name_;
  bool force_sync_;  // If async I/O should be avoided
  rados_ioctx_t ioctx_;
  rados_t cluster_;
};

}  // namespace rados
}  // namespace pdlfs
