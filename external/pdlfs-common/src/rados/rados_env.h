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

class RadosEnv : public Env {
 public:
  Ofs* TEST_GetOfs() { return ofs_; }
  virtual ~RadosEnv();
  virtual Status NewSequentialFile(const char* f, SequentialFile** r);
  virtual Status NewRandomAccessFile(const char* f, RandomAccessFile** r);
  virtual Status NewWritableFile(const char* f, WritableFile** r);
  virtual bool FileExists(const char* f);
  virtual Status GetFileSize(const char* f, uint64_t* size);
  virtual Status DeleteFile(const char* f);
  virtual Status CopyFile(const char* src, const char* dst);

  // The following operations are emulated through file sets.
  virtual Status GetChildren(const char* dir, std::vector<std::string>* r);
  virtual Status CreateDir(const char* dir);
  virtual Status AttachDir(const char* dir);
  virtual Status DeleteDir(const char* dir);
  virtual Status DetachDir(const char* dir);

  // The following operations are not supported.
  virtual Status RenameFile(const char* src, const char* dst);
  virtual Status LockFile(const char* f, FileLock** l);
  virtual Status UnlockFile(FileLock* l);
  virtual void Schedule(void (*f)(void*), void* a);
  virtual void StartThread(void (*f)(void*), void* a);
  virtual Status GetTestDirectory(std::string* path);
  virtual Status NewLogger(const char* fname, Logger** result);

 private:
  Status MountDir(const char* dir, bool create_dir);
  Status UnmountDir(const char* dir, bool also_delete_dir);
  // No copy allowed
  void operator=(const RadosEnv& other);
  RadosEnv(const RadosEnv&);
  explicit RadosEnv(
      const RadosEnvOptions&
          options);  // Full construction is done through RadosConnMgr
  friend class RadosConnMgr;
  // Constant after construction
  RadosEnvOptions options_;
  Ofs* ofs_;
  bool owns_osd_;
  Osd* osd_;
};

}  // namespace rados
}  // namespace pdlfs
