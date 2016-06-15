#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "rados_api.h"

#if defined(RADOS)
#include <rados/librados.hpp>

namespace pdlfs {
namespace rados {

inline Status RadosError(const Slice& err_context, int err_number) {
  char err[30];
  snprintf(err, sizeof(err), "rados_err=%d", -1 * err_number);
  return Status::IOError(err_context, err);
}

class RadosSequentialFile : public SequentialFile {
 private:
  uint64_t offset_;
  librados::IoCtx io_;
  bool io_shared_;
  std::string filename_;

 public:
  RadosSequentialFile(const Slice& filename, librados::IoCtx io, bool io_shared)
      : offset_(0),
        io_(io),
        io_shared_(io_shared),
        filename_(filename.ToString()) {}

  virtual ~RadosSequentialFile() {
    if (!io_shared_) {
      io_.close();
    }
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    ceph::bufferlist bl;
    int r = io_.read(filename_, bl, n, offset_);
    if (r >= 0) {
      r = bl.length();
      bl.copy(0, r, scratch);
    }

    if (r >= 0) {
      offset_ += r;
      *result = Slice(scratch, r);
      return Status::OK();
    } else {
      return RadosError(filename_, r);
    }
  }

  virtual Status Skip(uint64_t n) {
    offset_ += n;
    return Status::OK();
  }
};

class RadosRandomAccessFile : public RandomAccessFile {
 private:
  mutable librados::IoCtx io_;
  bool io_shared_;
  std::string filename_;

 public:
  RadosRandomAccessFile(const Slice& filename, librados::IoCtx io,
                        bool io_shared)
      : io_(io), io_shared_(io_shared), filename_(filename.ToString()) {}

  virtual ~RadosRandomAccessFile() {
    if (!io_shared_) {
      io_.close();
    }
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    ceph::bufferlist bl;
    int r = io_.read(filename_, bl, n, offset);
    if (r >= 0) {
      r = bl.length();
      bl.copy(0, r, scratch);
    }

    if (r >= 0) {
      *result = Slice(scratch, r);
      return Status::OK();
    } else {
      return RadosError(filename_, r);
    }
  }
};

class RadosWritableFile : public WritableFile {
 private:
  librados::IoCtx io_;
  bool io_shared_;
  std::string filename_;

 public:
  RadosWritableFile(const Slice& filename, librados::IoCtx io, bool io_shared)
      : io_(io), io_shared_(io_shared), filename_(filename.ToString()) {}

  virtual ~RadosWritableFile() {
    if (!io_shared_) {
      io_.close();
    }
  }

  virtual Status Append(const Slice& data) {
    ceph::bufferlist bl;
    bl.append(data.data(), data.size());

    int r = io_.append(filename_, bl, data.size());
    if (r < 0) {
      return RadosError(filename_, r);
    } else {
      return Status::OK();
    }
  }

  virtual Status Close() { return Status::OK(); }
  virtual Status Flush() { return Status::OK(); }
  virtual Status Sync() { return Status::OK(); }
};

class RadosAsyncWritableFile : public WritableFile {
 private:
  librados::IoCtx io_;
  bool io_shared_;
  std::string filename_;

 public:
  RadosAsyncWritableFile(const Slice& filename, librados::IoCtx io,
                         bool io_shared)
      : io_(io), io_shared_(io_shared), filename_(filename.ToString()) {}

  virtual ~RadosAsyncWritableFile() {
    if (!io_shared_) {
      io_.close();
    }
  }

  virtual Status Append(const Slice& data) {
    ceph::bufferlist bl;
    bl.append(data.data(), data.size());

    librados::AioCompletion* comp = librados::Rados::aio_create_completion();
    int r = io_.aio_append(filename_, comp, bl, data.size());
    delete comp;

    if (r < 0) {
      return RadosError(filename_, r);
    } else {
      return Status::OK();
    }
  }

  virtual Status Close() { return Status::OK(); }

  virtual Status Flush() { return Status::OK(); }

  virtual Status Sync() {
    int r = io_.aio_flush();
    if (r < 0) {
      return RadosError(filename_, r);
    } else {
      return Status::OK();
    }
  }
};

}  // namespace rados
}  // namespace pdlfs

#endif  // #ifdef RADOS
