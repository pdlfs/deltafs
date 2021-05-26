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

#include "rados_env.h"

#include "pdlfs-common/leveldb/filenames.h"

namespace pdlfs {
namespace rados {

class RadosDbEnvWrapper : public EnvWrapper {
 public:
  Env* TEST_GetRadosEnv() { return env_; }
  virtual ~RadosDbEnvWrapper();
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
  Status LocalCreateOrAttachDir(const char* dir, const Status& rs);
  Status LocalDeleteDir(const char* dir, const Status& rs);
  bool PathOnRados(const char* pathname);
  bool FileOnRados(const char* fname);
  Status RenameLocalTmpToRados(const char* tmp, const char* dst);
  RadosDbEnvWrapper(
      const RadosDbEnvOptions& options,
      Env* base_env);  // Full construction is done through RadosConnMgr
  friend class RadosConnMgr;
  // Constant after construction
  RadosDbEnvOptions options_;
  // Rados mount point. In general, files and directories beneath the mount
  // point are stored in rados. Files and directories out of it are stored in a
  // local env. Some db files (db info log files, db LOCK files, and db tmp
  // files) are stored locally (in a locally-mirrored directory) even if they
  // are within the rados mount point.
  const std::string* rados_root_;  // Cache for &env_->options_.rados_root
  bool owns_env_;
  RadosEnv* env_;  // The raw rados env
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
