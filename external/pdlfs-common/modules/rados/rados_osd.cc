/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "rados_osd.h"

#include <stdio.h>
#include <stdlib.h>

namespace pdlfs {
namespace rados {

#if defined(RADOS)
class RadosOSD : public OSD {
 private:
  librados::IoCtx io_;
  librados::Rados cluster_;
  bool async_io_;

  // No copying allowed
  RadosOSD(const RadosOSD&);
  void operator=(const RadosOSD&);

  static void RadosCall(const char* label, int result) {
    // Update the code if future rados includes rados_strerror()
    if (result != 0) {
      fprintf(stderr, "rados::%s: errno=%d\n", label, -1 * result);
      abort();
    }
  }

  static int RadosConfSet(librados::Rados& cluster, const char* opt, int val) {
    char sp[20];
    snprintf(sp, sizeof(sp), "%d", val);
    return cluster.conf_set(opt, sp);
  }

 public:
  RadosOSD(const RadosOptions& options);

  virtual ~RadosOSD() {
    io_.close();
    cluster_.shutdown();
  }

  virtual Status NewSequentialObj(const Slice& name, SequentialFile** f) {
    if (Exists(name)) {
      *f = new RadosSequentialFile(name, io_, true);
      return Status::OK();
    } else {
      return Status::NotFound(Slice());
    }
  }

  virtual Status NewRandomAccessObj(const Slice& name, RandomAccessFile** f) {
    if (Exists(name)) {
      *f = new RadosRandomAccessFile(name, io_, true);
      return Status::OK();
    } else {
      return Status::NotFound(Slice());
    }
  }

  virtual Status NewWritableObj(const Slice& name, WritableFile** f) {
    ceph::bufferlist bl;
    int r = io_.write_full(name.ToString(), bl);  // Truncate
    if (r < 0) {
      return RadosError(name, r);
    } else {
      librados::IoCtx new_io;
      new_io.dup(io_);
      if (async_io_) {
        *f = new RadosAsyncWritableFile(name, new_io, false);
        return Status::OK();
      } else {
        *f = new RadosWritableFile(name, new_io, false);
        return Status::OK();
      }
    }
  }

  virtual bool Exists(const Slice& name) {
    time_t ignored_time;
    uint64_t ignored_size;
    int r = io_.stat(name.ToString(), &ignored_size, &ignored_time);
    if (r == 0) {
      return true;
    } else {
      return false;
    }
  }

  virtual Status Size(const Slice& name, uint64_t* obj_size) {
    time_t ignored_time;
    int r = io_.stat(name.ToString(), obj_size, &ignored_time);
    if (r == 0) {
      return Status::OK();
    } else {
      return RadosError(name, r);
    }
  }

  virtual Status Delete(const Slice& name) {
    if (Exists(name)) {
      int r = io_.remove(name.ToString());
      if (r == 0) {
        return Status::OK();
      } else {
        return RadosError(name, r);
      }
    } else {
      return Status::NotFound(Slice());
    }
  }

  virtual Status Copy(const Slice& src, const Slice& target) {
    librados::ObjectWriteOperation op;
    op.copy_from(src.ToString(), io_, 0);
    int r = io_.operate(target.ToString(), &op);
    if (r == 0) {
      return Status::OK();
    } else {
      return RadosError(target, r);
    }
  }

  virtual Status Put(const Slice& name, const Slice& data) {
    ceph::bufferlist bl;
    bl.append(data.data(), data.size());
    int r = io_.write_full(name.ToString(), bl);
    if (r == 0) {
      return Status::OK();
    } else {
      return RadosError(name, r);
    }
  }

  virtual Status Get(const Slice& name, std::string* data) {
    uint64_t size;
    std::string oid = name.ToString();
    Status s = Size(oid, &size);
    if (s.ok()) {
      ceph::bufferlist bl;
      int r = io_.read(oid, bl, size, 0);
      if (r >= 0) {
        r = bl.length();
        bl.copy(0, r, *data);
        return s;
      } else {
        return RadosError(name, r);
      }
    } else {
      return s;
    }
  }
};

RadosOSD::RadosOSD(const RadosOptions& options) : async_io_(options.async_io) {
  RadosCall("init", cluster_.init(NULL));
  RadosCall("conf_read_file", cluster_.conf_read_file(options.conf_path));
  RadosCall("conf_set", RadosConfSet(cluster_, "client_mount_timeout",
                                     options.client_mount_timeout));
  RadosCall("conf_set", RadosConfSet(cluster_, "rados_mon_op_timeout",
                                     options.mon_op_timeout));
  RadosCall("conf_set", RadosConfSet(cluster_, "rados_osd_op_timeout",
                                     options.osd_op_timeout));
  RadosCall("connect", cluster_.connect());
  RadosCall("ioctx_create", cluster_.ioctx_create(options.pool_name, io_));
}

OSD* NewRadosOSD(const RadosOptions& options) {
  // If the rados library is present during compile time
  return new RadosOSD(options);
}
#else
OSD* NewRadosOSD(const RadosOptions& options) {
  char msg[] = "Not possible: rados not built\n";
  fwrite(msg, 1, sizeof(msg), stderr);
  abort();
}
#endif

}  // namespace rados
}  // namespace pdlfs
