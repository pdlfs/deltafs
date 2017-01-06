/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/pdlfs_config.h"

#include "rados_osd.h"

namespace pdlfs {
namespace rados {

RadosOsd::~RadosOsd() {
  // Wait until all async IO operations to finish
  rados_aio_flush(ioctx_);
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

bool RadosOsd::Exists(const Slice& name) {
  uint64_t ignored_size;
  time_t ignored_mtime;
  int r = rados_stat(ioctx_, name.c_str(), &ignored_size, &ignored_mtime);
  if (r != 0) {
    return false;
  } else {
    return true;
  }
}

Status RadosOsd::Size(const Slice& name, uint64_t* obj_size) {
  time_t ignored_mtime;
  int r = rados_stat(ioctx_, name.c_str(), obj_size, &ignored_mtime);
  if (r != 0) {
    return RadosError("rados_stat", r);
  } else {
    return Status::OK();
  }
}

Status RadosOsd::NewSequentialObj(const Slice& name, SequentialFile** result) {
  uint64_t obj_size;
  Status s = Size(name, &obj_size);
  if (s.ok()) {
    if (obj_size != 0) {
      const bool owns_ioctx = false;
      *result = new RadosSequentialFile(name, ioctx_, owns_ioctx);
    } else {
      *result = new RadosEmptyFile();
    }
  }

  return s;
}

Status RadosOsd::NewRandomAccessObj(const Slice& name,
                                    RandomAccessFile** result) {
  uint64_t obj_size;
  Status s = Size(name, &obj_size);
  if (s.ok()) {
    if (obj_size != 0) {
      const bool owns_ioctx = false;
      *result = new RadosRandomAccessFile(name, ioctx_, owns_ioctx);
    } else {
      *result = new RadosEmptyFile();
    }
  }

  return s;
}

Status RadosOsd::NewWritableObj(const Slice& name, WritableFile** result) {
  Status s;
  rados_ioctx_t ioctx;
  s = CreateIoCtx(&ioctx);
  if (!force_sync_) {
    *result = new RadosAsyncWritableFile(name, mutex_, ioctx);
  } else {
    *result = new RadosWritableFile(name, ioctx);
  }
  return s;
}

Status RadosOsd::Delete(const Slice& name) {
  int r = rados_remove(ioctx_, name.c_str());
  if (r != 0) {
    return RadosError("rados_remove", r);
  } else {
    return Status::OK();
  }
}

Status RadosOsd::Copy(const Slice& src, const Slice& dst) {
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
      char* buf = new char[1024 * 1024];  // 1m
      uint64_t off = 0;
      while (s.ok() && obj_size != 0) {
        int nbytes = rados_read(ioctx_, src.c_str(), buf, 1024 * 1024, off);
        if (nbytes > 0) {
          s = target->Append(Slice(buf, nbytes));
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

Status RadosOsd::Put(const Slice& name, const Slice& buf) {
  int r = rados_write_full(ioctx_, name.c_str(), buf.data(), buf.size());
  if (r != 0) {
    return RadosError("rados_write_full", r);
  } else {
    return Status::OK();
  }
}

Status RadosOsd::Get(const Slice& name, std::string* data) {
  uint64_t obj_size;
  Status s = Size(name, &obj_size);
  if (s.ok() && obj_size != 0) {
    char* buf = new char[obj_size];
    uint64_t off = 0;
    while (s.ok() && obj_size != 0) {
      int nbytes = rados_read(ioctx_, name.c_str(), buf, obj_size, off);
      if (nbytes > 0) {
        data->append(buf, nbytes);
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
