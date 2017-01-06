#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
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
// I/O implementation on top of ceph rados
class RadosFio;

// State of each opened file object
#ifndef NDEBUG
class RadosFobj : public Fio::Handle {
 public:
  virtual ~RadosFobj() {}
#else
class RadosFobj {
 public:
  ~RadosFobj() {}
#endif

 public:
  explicit RadosFobj(RadosFio* fio) : fio(fio), fctx(NULL) {}

  RadosFio* fio;
  rados_ioctx_t fctx;
  uint64_t mtime;  // Cached last file modification time
  uint64_t size;   // Cached file size
  uint64_t off;    // Current read/write position

  bool append_only;
  int bg_err;
  int refs;
};

class RadosFio : public Fio {
 public:
  virtual ~RadosFio();

  virtual Status Creat(const Fentry&, bool o_append, Handle**);
  virtual Status Open(const Fentry&, bool o_creat, bool o_trunc, bool o_append,
                      uint64_t* mtime, uint64_t* size, Handle**);
  virtual Status Fstat(const Fentry&, Handle*, uint64_t* mtime, uint64_t* size,
                       bool skip_cache = false);
  virtual Status Write(const Fentry&, Handle*, const Slice& data);
  virtual Status Pwrite(const Fentry&, Handle*, const Slice& data,
                        uint64_t off);
  virtual Status Read(const Fentry&, Handle*, Slice* result, uint64_t size,
                      char* scratch);
  virtual Status Pread(const Fentry&, Handle*, Slice* result, uint64_t off,
                       uint64_t size, char* scratch);
  virtual Status Ftrunc(const Fentry&, Handle*, uint64_t size);
  virtual Status Flush(const Fentry&, Handle*, bool force_sync = false);
  virtual Status Close(const Fentry&, Handle*);

  virtual Status Trunc(const Fentry&, uint64_t size);
  virtual Status Stat(const Fentry&, uint64_t* mtime, uint64_t* size);
  virtual Status Drop(const Fentry&);

 private:
  void MaybeSetError(RadosFobj*, int err);
  static void IO_safe(rados_completion_t, void*);
  void Unref(RadosFobj*);

  RadosFio(port::Mutex* mu) : mutex_(mu), rw_lock_(mu) {}
  friend class RadosConn;
  port::Mutex* mutex_;
  RWLock rw_lock_;  // Enforce serialization on some operations
  std::string pool_name_;
  bool force_sync_;  // Disable async I/O
  rados_ioctx_t ioctx_;
  rados_t cluster_;
};

}  // namespace rados
}  // namespace pdlfs
