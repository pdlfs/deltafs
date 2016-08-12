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
  size_t wal_write_buffer_;
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

// Buffer a certain amount of data before writing to Rados.
// The "Flush" call is always ignored and only "Sync" is respected.
// May lose data for clients that only call "Flush" to ensure
// data durability. To avoid losing application states, clients
// are expected to call "Sync" at a certain time interval,
// such as every 5 seconds.
class RadosUnsafeBufferedWritableFile : public WritableFile {
 private:
  size_t buffer_size_;
  std::string space_;
  WritableFile* file_;

  Status DoFlush() {
    if (!space_.empty()) {
      Status s = file_->Append(space_);
      if (s.ok()) {
        space_.clear();
      }
      return s;
    } else {
      return Status::OK();
    }
  }

 public:
  RadosUnsafeBufferedWritableFile(WritableFile* file, size_t buffer_size)
      : buffer_size_(buffer_size), file_(file) {
    space_.reserve(2 * buffer_size_);
  }

  virtual ~RadosUnsafeBufferedWritableFile() { delete file_; }

  virtual Status Append(const Slice& data) {
    if (space_.size() + data.size() <= 2 * buffer_size_) {
      AppendSliceTo(&space_, data);
      if (space_.size() >= buffer_size_) {
        return DoFlush();
      } else {
        return Status::OK();
      }
    } else {
      Status s = DoFlush();
      if (s.ok()) {
        if (data.size() <= buffer_size_) {
          AppendSliceTo(&space_, data);
        } else {
          s = file_->Append(data);
        }
      }
      return s;
    }
  }

  virtual Status Close() {
    Status s = DoFlush();
    if (s.ok()) {
      s = file_->Close();
    }
    return s;
  }

  virtual Status Flush() {
    assert(space_.size() < buffer_size_);
    return Status::OK();
  }

  virtual Status Sync() {
    Status s = DoFlush();
    if (s.ok()) {
      s = file_->Sync();
    }
    return s;
  }
};

}  // namespace rados
}  // namespace pdlfs
