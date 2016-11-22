#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/osd_env.h"

#include "rados_conn.h"

namespace pdlfs {
namespace rados {

class RadosEnv : public EnvWrapper {
 public:
  virtual ~RadosEnv();
  virtual bool FileExists(const Slice& f);
  virtual Status NewSequentialFile(const Slice& f, SequentialFile** r);
  virtual Status NewRandomAccessFile(const Slice& f, RandomAccessFile** r);
  virtual Status NewWritableFile(const Slice& f, WritableFile** r);
  virtual Status GetChildren(const Slice& d, std::vector<std::string>* r);
  virtual Status DeleteFile(const Slice& f);
  virtual Status CreateDir(const Slice& d);
  virtual Status AttachDir(const Slice& d);
  virtual Status DeleteDir(const Slice& d);
  virtual Status DetachDir(const Slice& d);
  virtual Status GetFileSize(const Slice& f, uint64_t* s);
  virtual Status CopyFile(const Slice& s, const Slice& t);
  virtual Status RenameFile(const Slice& s, const Slice& t);

 private:
  Status MountDir(const Slice& d, bool create_dir);
  Status UnmountDir(const Slice& d, bool delete_dir);
  bool FileOnRados(const Slice& f);

  RadosEnv(Env* e) : EnvWrapper(e) {}
  friend class RadosConn;
  size_t wal_buf_size_;
  std::string rados_root_;
  OSDEnv* osd_env_;
  bool owns_osd_;
  OSD* osd_;
};

inline FileType TryResolveFileType(const Slice& fname) {
  FileType type;
  uint64_t number;
  Slice path = fname;
  // We only interest in the last component of the file path.
  const char* p = strrchr(fname.data(), '/');
  if (p != NULL) {
    path.remove_prefix(static_cast<size_t>(p - fname.data()) + 1);
  }
  // Handle foreign files by returning an invalid type code.
  if (!ParseFileName(path, &number, &type)) {
    return static_cast<FileType>(kMaxFileType + 1);
  } else {
    return type;
  }
}

inline bool OnRados(FileType type) {
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
