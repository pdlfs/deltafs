/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "rados_common.h"
#include "rados_conn.h"

namespace pdlfs {
namespace rados {

// Osd implementation using Rados api. Rados async I/O is used by default unless
// explicitly disabled by the caller.
class RadosOsd : public OSD {
 public:
  virtual ~RadosOsd();
  virtual Status NewSequentialObj(const char* name, SequentialFile** r);
  virtual Status NewRandomAccessObj(const char* name, RandomAccessFile** r);
  virtual Status NewWritableObj(const char* name, WritableFile** r);
  virtual bool Exists(const char* name);
  virtual Status Size(const char* name, uint64_t* obj_size);
  virtual Status Delete(const char* name);
  virtual Status Copy(const char* src, const char* dst);
  virtual Status Put(const char* name, const Slice& data);
  virtual Status Get(const char* name, std::string* data);

 private:
  RadosOsd() {}
  friend class RadosConn;
  Status CreateIoCtx(rados_ioctx_t*);
  port::Mutex* mutex_;
  std::string pool_name_;
  bool force_sync_;  // If async I/O should be disabled
  rados_ioctx_t ioctx_;
  rados_t cluster_;
};

}  // namespace rados
}  // namespace pdlfs
