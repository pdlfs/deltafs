#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/env.h"

#include <string>

namespace pdlfs {

// Hold a certain amount of data before eventually writing to *base.
// Ignore flush calls and only sync calls are respected.
// May lose data for clients that rely on flushes to ensure data durability.
// To avoid losing data, clients may call sync at a certain time interval,
// such as 5 seconds. Clients may also call FFlush to force data flush.
class UnsafeBufferedWritableFile : public WritableFile {
 public:
  UnsafeBufferedWritableFile(WritableFile* base, size_t buf_size)
      : base_(base), max_buf_size_(buf_size) {
    buf_.reserve(max_buf_size_);
  }

  virtual ~UnsafeBufferedWritableFile() {
    if (base_ != NULL) {
      base_->Close();
      delete base_;
    }
  }

  virtual Status Close() {
    Status s = FFlush();
    base_->Close();
    delete base_;
    base_ = NULL;
    return s;
  }

  virtual Status Append(const Slice& data) {
    Status s;
    Slice chunk = data;
    while (buf_.size() + chunk.size() >= max_buf_size_) {
      size_t left = max_buf_size_ - buf_.size();
      buf_.append(chunk.data(), left);
      s = FFlush();
      if (s.ok()) {
        chunk.remove_prefix(left);
      } else {
        break;
      }
    }
    if (s.ok() && chunk.size() != 0) {
      buf_.append(chunk.data(), chunk.size());
    }
    return s;
  }

  virtual Status Sync() {
    Status s = FFlush();
    if (s.ok()) {
      s = base_->Sync();
    }
    return s;
  }

  virtual Status Flush() {
    Status s;
    assert(buf_.size() <= max_buf_size_);
    return s;
  }

  Status FFlush() {
    Status s;
    assert(buf_.size() <= max_buf_size_);
    if (!buf_.empty()) {
      s = base_->Append(buf_);
      if (s.ok()) {
        buf_.clear();
      }
    }
    return s;
  }

 private:
  WritableFile* base_;
  const size_t max_buf_size_;
  std::string buf_;
};

}  // namespace pdlfs
