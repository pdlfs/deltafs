#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/coding.h"
#include "pdlfs-common/logging.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/port.h"

#include "rados_conn.h"

#if defined(RADOS)
#include <rados/librados.h>

namespace pdlfs {
namespace rados {

// Async I/O operation context.
class RadosOpCtx {
 public:
  RadosOpCtx(port::Mutex* mu) : mu_(mu), nrefs_(1), err_(0) {}
  bool ok() const { return err_ == 0; }
  int err() const { return err_; }

  void Ref() {
    mu_->AssertHeld();
    nrefs_++;
  }

  void Unref() {
    mu_->AssertHeld();
    assert(nrefs_ > 0);
    nrefs_--;
    if (nrefs_ == 0) {
      delete this;
    }
  }

  static void IO_safe(rados_completion_t comp, void* arg);

 private:
  ~RadosOpCtx() {}
  // No copying allowed
  void operator=(const RadosOpCtx&);
  RadosOpCtx(const RadosOpCtx&);
  port::Mutex* mu_;
  int nrefs_;
  int err_;
};

inline Status RadosError(const char* err_ctx, int err_num) {
  char tmp[100];
  snprintf(tmp, sizeof(tmp), "%s: %s", err_ctx, strerror(-err_num));
  if (err_num != -ENOENT && err_num != -EEXIST) {
    return Status::IOError(tmp);
  } else if (err_num == -EEXIST) {
    return Status::AlreadyExists(tmp);
  } else {
    return Status::NotFound(tmp);
  }
}

class RadosEmptyFile : public RandomAccessFile, public SequentialFile {
 public:
  explicit RadosEmptyFile() {}
  virtual ~RadosEmptyFile() {}
  virtual Status Read(uint64_t off, size_t n, Slice* result,
                      char* scratch) const {
    *result = Slice();
    return Status::OK();
  }
  virtual Status Read(size_t n, Slice* result, char* scratch) {
    *result = Slice();
    return Status::OK();
  }
  virtual Status Skip(uint64_t n) {
    return Status::OK();  // The file is empty anyway
  }
};

class RadosSequentialFile : public SequentialFile {
 private:
  std::string oid_;
  rados_ioctx_t rados_ioctx_;
  bool owns_ioctx_;
  uint64_t off_;

 public:
  RadosSequentialFile(const Slice& fname, rados_ioctx_t ioctx, bool owns_ioctx)
      : rados_ioctx_(ioctx), owns_ioctx_(owns_ioctx), off_(0) {
    oid_ = fname.ToString();
  }

  virtual ~RadosSequentialFile() {
    if (owns_ioctx_) {
      rados_ioctx_destroy(rados_ioctx_);
    }
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    int nbytes = rados_read(rados_ioctx_, oid_.c_str(), scratch, n, off_);
    if (nbytes < 0) {
      return RadosError("rados_read", nbytes);
    } else {
      *result = Slice(scratch, nbytes);
      off_ += nbytes;
      return Status::OK();
    }
  }

  virtual Status Skip(uint64_t n) {
    off_ += n;
    return Status::OK();
  }
};

class RadosRandomAccessFile : public RandomAccessFile {
 private:
  std::string oid_;
  mutable rados_ioctx_t rados_ioctx_;
  bool owns_ioctx_;

 public:
  RadosRandomAccessFile(const Slice& fname, rados_ioctx_t ioctx,
                        bool owns_ioctx)
      : rados_ioctx_(ioctx), owns_ioctx_(owns_ioctx) {
    oid_ = fname.ToString();
  }

  virtual ~RadosRandomAccessFile() {
    if (owns_ioctx_) {
      rados_ioctx_destroy(rados_ioctx_);
    }
  }

  virtual Status Read(uint64_t off, size_t n, Slice* result,
                      char* scratch) const {
    int nbytes = rados_read(rados_ioctx_, oid_.c_str(), scratch, n, off);
    if (nbytes < 0) {
      return RadosError("rados_read", nbytes);
    } else {
      *result = Slice(scratch, nbytes);
      return Status::OK();
    }
  }
};

class RadosWritableFile : public WritableFile {
 private:
  std::string oid_;
  rados_ioctx_t rados_ioctx_;
  bool owns_ioctx_;
  int err_;

 public:
  RadosWritableFile(const Slice& fname, rados_ioctx_t ioctx,
                    bool owns_ioctx = true)
      : rados_ioctx_(ioctx), owns_ioctx_(owns_ioctx), err_(0) {
    oid_ = fname.ToString();
    Truncate();
  }

  virtual ~RadosWritableFile() {
    if (owns_ioctx_) {
      rados_ioctx_destroy(rados_ioctx_);
    }
  }

  virtual Status Append(const Slice& buf) {
    if (err_ == 0) {
      err_ = rados_append(rados_ioctx_, oid_.c_str(), buf.data(), buf.size());
    }
    if (err_ != 0) {
      return RadosError("rados_append", err_);
    } else {
      return Status::OK();
    }
  }

  Status Truncate() {
    if (err_ == 0) {
      err_ = rados_write_full(rados_ioctx_, oid_.c_str(), "", 0);
    }
    if (err_ != 0) {
      return RadosError("rados_write_full", err_);
    } else {
      return Status::OK();
    }
  }

  virtual Status Close() { return Status::OK(); }

  virtual Status Flush() {
    if (err_ != 0) {
      return RadosError("bad_file_state", err_);
    } else {
      return Status::OK();
    }
  }

  virtual Status Sync() {
    if (err_ != 0) {
      return RadosError("bad_file_state", err_);
    } else {
      return Status::OK();
    }
  }
};

class RadosAsyncWritableFile : public WritableFile {
 private:
  port::Mutex* mu_;
  RadosOpCtx* async_op_;
  std::string oid_;
  rados_ioctx_t rados_ioctx_;
  bool owns_ioctx_;

  Status Ref() {
    MutexLock ml(mu_);
    if (!async_op_->ok()) {
      return RadosError("rados_bg_io", async_op_->err());
    } else {
      async_op_->Ref();
      return Status::OK();
    }
  }

 public:
  RadosAsyncWritableFile(const Slice& fname, port::Mutex* mu,
                         rados_ioctx_t ioctx, bool owns_ioctx = true)
      : mu_(mu), rados_ioctx_(ioctx), owns_ioctx_(owns_ioctx) {
    async_op_ = new RadosOpCtx(mu_);
    oid_ = fname.ToString();
    Truncate();
  }

  virtual ~RadosAsyncWritableFile() {
    mu_->Lock();
    async_op_->Unref();
    mu_->Unlock();
    if (owns_ioctx_) {
      rados_ioctx_destroy(rados_ioctx_);
    }
  }

  virtual Status Append(const Slice& buf) {
    Status s = Ref();
    if (s.ok()) {
      rados_completion_t comp;
      rados_aio_create_completion(async_op_, NULL, RadosOpCtx::IO_safe, &comp);
      rados_aio_append(rados_ioctx_, oid_.c_str(), comp, buf.data(),
                       buf.size());
      rados_aio_release(comp);
      return s;
    } else {
      // No more writes if we get errors
      return s;
    }
  }

  Status Truncate() {
    Status s = Ref();
    if (s.ok()) {
      rados_completion_t comp;
      rados_aio_create_completion(async_op_, NULL, RadosOpCtx::IO_safe, &comp);
      rados_aio_write_full(rados_ioctx_, oid_.c_str(), comp, "", 0);
      rados_aio_release(comp);
      return s;
    } else {
      // No more writes if we get errors
      return s;
    }
  }

  virtual Status Close() { return Status::OK(); }

  virtual Status Flush() {
    MutexLock ml(mu_);
    if (!async_op_->ok()) {
      return RadosError("rados_bg_io", async_op_->err());
    } else {
      return Status::OK();
    }
  }

  virtual Status Sync() {
    rados_aio_flush(rados_ioctx_);
    return Flush();
  }
};

}  // namespace rados
}  // namespace pdlfs

#endif  // RADOS
