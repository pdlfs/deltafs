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

#include "rados_comm.h"

#include "pdlfs-common/rados/rados_connmgr.h"

#include "pdlfs-common/ofs.h"
#include "pdlfs-common/port.h"

namespace pdlfs {
namespace rados {

// An osd implementation on top of rados. rados async I/O is utilized by default
// unless explicitly disabled by the caller.
class RadosOsd : public Osd {
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
  RadosOsd() {}  // Construction is done through RadosConnMgr
  friend class RadosConnMgr;
  Status CreateIoCtx(rados_ioctx_t* result);
  // Constant after construction
  std::string pool_name_;
  bool force_syncio_;  // If async I/O is off
  RadosConnMgr* connmgr_;
  RadosConn* conn_;
  // State beblow protected by *mutex_
  port::Mutex mutex_;
  rados_ioctx_t ioctx_;
};

}  // namespace rados
}  // namespace pdlfs
