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

#include "pdlfs-common/leveldb/dbfiles.h"

#include "rados_common.h"
#include "rados_conn.h"

namespace pdlfs {
namespace rados {

class RadosEnv : public EnvWrapper {
 public:
  virtual ~RadosEnv();
  virtual Status NewSequentialFile(const char* f, SequentialFile** r);
  virtual Status NewRandomAccessFile(const char* f, RandomAccessFile** r);
  virtual Status NewWritableFile(const char* f, WritableFile** r);
  virtual bool FileExists(const char* f);
  virtual Status GetFileSize(const char* f, uint64_t* size);
  virtual Status DeleteFile(const char* f);
  virtual Status CopyFile(const char* src, const char* dst);
  virtual Status RenameFile(const char* src, const char* dst);

  virtual Status GetChildren(const char* dir, std::vector<std::string>* r);
  virtual Status CreateDir(const char* dir);
  virtual Status AttachDir(const char* dir);
  virtual Status DeleteDir(const char* dir);
  virtual Status DetachDir(const char* dir);

 private:
  Status MountDir(const char* dir, bool force_create);
  Status UnmountDir(const char* dir, bool force_delete);
  Status RenameLocalTmpToRados(const char* tmp, const char* dst);
  bool PathOnRados(const char* pathname);
  bool FileOnRados(const char* fname);

  RadosEnv(Env* e) : EnvWrapper(e) {}
  friend class RadosConn;
  size_t wal_buf_size_;
  std::string rados_root_;
  Ofs* ofs_;
  bool owns_osd_;
  Osd* osd_;
};

inline FileType TryResolveFileType(const char* fname) {
  FileType type;
  uint64_t number;
  Slice path = fname;
  // We only interest in the last component of the file path.
  const char* p = strrchr(fname, '/');
  if (p != NULL) {
    path.remove_prefix(static_cast<size_t>(p - fname) + 1);
  }
  // Handle foreign files by returning an invalid type code.
  if (!ParseFileName(path, &number, &type)) {
    return static_cast<FileType>(kMaxFileType + 1);
  } else {
    return type;
  }
}

inline bool TypeOnRados(FileType type) {
  switch (type) {
    case kTableFile:
    case kLogFile:
    case kDescriptorFile:
    case kCurrentFile:
      return true;
    case kDBLockFile:
    case kTempFile:
    case kInfoLogFile:
    default:  // This includes all foreign files
      return false;
  }
}

}  // namespace rados
}  // namespace pdlfs
