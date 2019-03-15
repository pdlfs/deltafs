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

#include "rados_osd.h"

namespace pdlfs {
namespace rados {

RadosOsd::~RadosOsd() {
  rados_aio_flush(ioctx_);  // Wait until all async IO operations to finish
  rados_ioctx_destroy(ioctx_);
}

Status RadosOsd::CreateIoCtx(rados_ioctx_t* result) {
  int r = rados_ioctx_create(cluster_, pool_name_.c_str(), result);
  if (r != 0) {
    return RadosError("rados_ioctx_create", r);
  } else {
    return Status::OK();
  }
}

// Return true iff the named object exists.
bool RadosOsd::Exists(const char* name) {
  uint64_t ignored_size;
  time_t ignored_mtime;
  int r = rados_stat(ioctx_, name, &ignored_size, &ignored_mtime);
  if (r != 0) {
    return false;
  } else {
    return true;
  }
}

Status RadosOsd::Size(const char* name, uint64_t* obj_size) {
  time_t ignored_mtime;
  int r = rados_stat(ioctx_, name, obj_size, &ignored_mtime);
  if (r != 0) {
    return RadosError("rados_stat", r);
  } else {
    return Status::OK();
  }
}

Status RadosOsd::NewSequentialObj(const char* name, SequentialFile** r) {
  const bool owns_ioctx = false;
  uint64_t obj_size;
  Status s = Size(name, &obj_size);
  if (s.ok()) {
    if (obj_size != 0) {
      *r = new RadosSequentialFile(name, ioctx_, owns_ioctx);
    } else {
      *r = new RadosEmptyFile();
    }
  } else {
    *r = NULL;
  }

  return s;
}

Status RadosOsd::NewRandomAccessObj(const char* name, RandomAccessFile** r) {
  const bool owns_ioctx = false;
  uint64_t obj_size;
  Status s = Size(name, &obj_size);
  if (s.ok()) {
    if (obj_size != 0) {
      *r = new RadosRandomAccessFile(name, ioctx_, owns_ioctx);
    } else {
      *r = new RadosEmptyFile();
    }
  } else {
    *r = NULL;
  }

  return s;
}

Status RadosOsd::NewWritableObj(const char* name, WritableFile** r) {
  rados_ioctx_t ioctx;
  Status s = CreateIoCtx(&ioctx);
  if (s.ok()) {
    if (!force_sync_) {
      *r = new RadosAsyncWritableFile(name, mutex_, ioctx);
    } else {
      *r = new RadosWritableFile(name, ioctx);
    }
  } else {
    *r = NULL;
  }

  return s;
}

Status RadosOsd::Delete(const char* name) {
  int r = rados_remove(ioctx_, name);  // Synchronous removal
  if (r != 0) {
    return RadosError("rados_remove", r);
  } else {
    return Status::OK();
  }
}

Status RadosOsd::Copy(const char* src, const char* dst) {
  uint64_t obj_size;
  Status s = Size(src, &obj_size);
  if (s.ok()) {
    rados_ioctx_t ioctx;
    s = CreateIoCtx(&ioctx);
    if (s.ok()) {
      WritableFile* target;
      if (!force_sync_) {
        target = new RadosAsyncWritableFile(dst, mutex_, ioctx);
      } else {
        target = new RadosWritableFile(dst, ioctx);
      }
      const size_t io_size = 1024 * 1024;  // 1MB
      char* buf = new char[io_size];
      uint64_t off = 0;
      while (s.ok() && obj_size != 0) {
        int nbytes = rados_read(ioctx_, src, buf, io_size, off);
        if (nbytes > 0) {
          size_t n = static_cast<size_t>(nbytes);
          s = target->Append(Slice(buf, n));
        } else if (nbytes < 0) {
          s = RadosError("rados_read", nbytes);
        } else {
          break;
        }
        if (s.ok()) {
          assert(obj_size >= nbytes);
          obj_size -= nbytes;
          off += nbytes;
        }
      }
      if (s.ok()) {
        s = target->Sync();
      }
      delete target;
    }
  }

  return s;
}

Status RadosOsd::Put(const char* name, const Slice& buf) {
  int r = rados_write_full(ioctx_, name, buf.data(), buf.size());  // Atomic
  if (r != 0) {
    return RadosError("rados_write_full", r);
  } else {
    return Status::OK();
  }
}

Status RadosOsd::Get(const char* name, std::string* data) {
  uint64_t obj_size;
  Status s = Size(name, &obj_size);
  if (s.ok() && obj_size != 0) {
    char* buf = new char[obj_size];
    uint64_t off = 0;
    while (s.ok() && obj_size != 0) {
      int nbytes = rados_read(ioctx_, name, buf, obj_size, off);
      if (nbytes > 0) {
        size_t n = static_cast<size_t>(nbytes);
        data->append(buf, n);
      } else if (nbytes < 0) {
        s = RadosError("rados_read", nbytes);
      } else {
        break;
      }
      if (s.ok()) {
        assert(obj_size >= nbytes);
        obj_size -= nbytes;
        off += nbytes;
      }
    }
  }

  return s;
}

}  // namespace rados
}  // namespace pdlfs
