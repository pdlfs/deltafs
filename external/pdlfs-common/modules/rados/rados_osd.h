#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#if defined(RADOS)
#include "rados_api.h"
#include "rados_common.h"

namespace pdlfs {
namespace rados {

// OSD implementation atop Rados.
class RadosOsd : public OSD {
 public:
  virtual ~RadosOsd();
  virtual Status NewSequentialObj(const Slice& name, SequentialFile** result);
  virtual Status NewRandomAccessObj(const Slice& name,
                                    RandomAccessFile** result);
  virtual Status NewWritableObj(const Slice& name, WritableFile** result);
  virtual bool Exists(const Slice& name);
  virtual Status Size(const Slice& name, uint64_t* obj_size);
  virtual Status Delete(const Slice& name);
  virtual Status Put(const Slice& name, const Slice& data);
  virtual Status Get(const Slice& name, std::string* data);
  virtual Status Copy(const Slice& src, const Slice& dst);

 private:
  RadosOsd() {}
  friend class RadosConn;
  Status CloneIoCtx(rados_ioctx_t*);
  bool force_sync_;  // If async I/O should be disabled
  port::Mutex* mutex_;
  rados_ioctx_t ioctx_;
  rados_t cluster_;
};

}  // namespace rados
}  // namespace pdlfs

#endif  // RADOS
