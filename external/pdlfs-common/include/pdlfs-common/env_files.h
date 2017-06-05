/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/env.h"

#include <string>

namespace pdlfs {

// Buffer a certain amount of data before eventually writing to *base.
// Ignore flush calls and only sync calls are respected.
// May lose data for clients that rely on flushes to ensure data durability.
// To avoid losing data, clients may call sync at a certain time interval,
// or call FFlush() to force data flush.
// NOTE: *base will be deleted when this wrapper is deleted
// NOTE: using write buffer will cause an extra copy of data in memory
// NOTE: implementation is not thread safe
class UnsafeBufferedWritableFile : public WritableFile {
 public:
  std::string* buffer_store() { return &buf_; }

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

// Measure the total amount of data written into *base and store
// the result in an external counter.
// NOTE: *base will be deleted when this wrapper is deleted
// NOTE: implementation is not thread safe
class MeasuredWritableFile : public WritableFile {
 public:
  MeasuredWritableFile(WritableFile* base, uint64_t* dst)
      : base_(base), bytes_(dst) {}

  virtual ~MeasuredWritableFile() { delete base_; }

  virtual Status Close() { return base_->Close(); }
  virtual Status Flush() { return base_->Flush(); }
  virtual Status Sync() { return base_->Sync(); }

  virtual Status Append(const Slice& data) {
    Status s = base_->Append(data);
    if (s.ok()) {
      *bytes_ += data.size();
    }
    return s;
  }

 private:
  WritableFile* base_;
  uint64_t* bytes_;
};

// Measure the I/O activity accessing an underlying random access file
// and store the results in a set of atomic counters.
class AtomicMeasuredRandomAccessFile : public RandomAccessFile {
 public:
  explicit AtomicMeasuredRandomAccessFile(RandomAccessFile* base);
  virtual ~AtomicMeasuredRandomAccessFile();

  // Safe for concurrent use by multiple threads.
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const;

  // Total number of bytes read out.
  uint64_t TotalBytes() const;

  // Total number of read operations witnessed.
  uint64_t TotalOps() const;

 private:
  class Rep;
  RandomAccessFile* base_;
  Rep* rep_;
};

// Convert a sequential file into a fully buffered random access file by
// pre-fetching all file contents into memory and use that to serve all future
// read requests to the underlying file. At most "max_buf_size_" worth of data
// will be fetched and buffered in memory. Callers must explicitly call Load()
// to pre-populate the file contents in memory.
class WholeFileBufferedRandomAccessFile : public RandomAccessFile {
 public:
  WholeFileBufferedRandomAccessFile(SequentialFile* base, size_t buf_size,
                                    size_t io_size = 4096)
      : base_(base), max_buf_size_(buf_size), io_size_(io_size) {
    buf_ = new char[max_buf_size_];
    buf_size_ = 0;
  }

  virtual ~WholeFileBufferedRandomAccessFile() {
    delete[] buf_;
    if (base_ != NULL) {
      delete base_;
    }
  }

  // The returned slice will remain valid as long as the file is not deleted.
  // Safe for concurrent use by multiple threads.
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    if (offset < buf_size_) {
      if (n > buf_size_ - offset) n = buf_size_ - offset;
      *result = Slice(buf_ + offset, n);
    } else {
      *result = Slice();
    }

    return Status::OK();
  }

  // REQUIRES: Load() has not been called before.
  Status Load();

 private:
  SequentialFile* base_;
  const size_t max_buf_size_;
  const size_t io_size_;
  size_t buf_size_;
  char* buf_;
};

}  // namespace pdlfs
