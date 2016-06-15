#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/dbfiles.h"

#include "rados_api.h"

namespace pdlfs {
namespace rados {

inline FileType TryResolveFileType(const Slice& fname) {
  FileType type;
  uint64_t number;
  Slice input = fname;
  const char* p = strrchr(fname.data(), '/');
  if (p != NULL) {
    input.remove_prefix(static_cast<size_t>(p - fname.data()) + 1);
  }
  if (ParseFileName(input, &number, &type)) {
    return type;
  } else {
    return static_cast<FileType>(kMaxFileType + 1);
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
    default:  // This includes all unrecognizable files
      return false;
  }
}

class BufferedWritableFile : public WritableFile {
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
  BufferedWritableFile(WritableFile* file, size_t buffer_size)
      : buffer_size_(buffer_size), file_(file) {
    space_.reserve(2 * buffer_size_);
  }

  virtual ~BufferedWritableFile() { delete file_; }

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
