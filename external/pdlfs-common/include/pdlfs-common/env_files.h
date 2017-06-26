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

#include <assert.h>
#include <string>

namespace pdlfs {

// An enhanced WritableFile abstraction with richer semantics
// on durability control.
class SynchronizableFile : public WritableFile {
 public:
  SynchronizableFile() {}
  virtual ~SynchronizableFile();

  // Force file data [0, offset) to be flushed to the underlying storage
  // hardware. After this call, file data at [offset, ...) may still be buffered
  // in memory.
  virtual Status SyncBefore(uint64_t offset) = 0;

  // Flush file buffering and force data to be sent to the underlying storage
  // software, but not necessarily the hardware.
  virtual Status EmptyBuffer() = 0;
};

// Passively buffer a certain amount of data before eventually flushing data to
// a given *base. Ignore all explicit Flush() calls, but EmptyBuffer(), Sync(),
// and SyncBefore() calls are respected. May lose data for clients that only
// use Flush() calls to ensure data durability. To avoid losing data, clients
// may choose to call Sync() at a certain time interval, or use EmptyBuffer()
// calls to force data flush.
// Implementation is not thread-safe and requires external synchronization for
// use by multiple threads.
// Write buffering will cause an extra copy of data in memory
class UnsafeBufferedWritableFile : public SynchronizableFile {
 public:
  std::string* buffer_store() { return &buf_; }

  // *base must remain alive during the lifetime of this class and will be
  // closed and deleted when the destructor of this class is called.
  UnsafeBufferedWritableFile(WritableFile* base, size_t buf_size)
      : base_(base), offset_(0), max_buf_size_(buf_size) {
    buf_.reserve(max_buf_size_);
  }

  virtual ~UnsafeBufferedWritableFile() {
    if (base_ != NULL) {
      base_->Close();
      delete base_;
    }
  }

  virtual Status Close() {
    Status status = EmptyBuffer();
    if (status.ok()) {
      status = base_->Close();
    }
    delete base_;
    base_ = NULL;
    return status;
  }

  virtual Status Append(const Slice& data) {
    Status status;
    Slice chunk = data;
    while (buf_.size() + chunk.size() >= max_buf_size_) {
      size_t left = max_buf_size_ - buf_.size();
      buf_.append(chunk.data(), left);
      status = EmptyBuffer();
      if (status.ok()) {
        chunk.remove_prefix(left);
      } else {
        break;
      }
    }
    if (!status.ok()) {
      return status;  // Error
    } else if (chunk.size() != 0) {
      buf_.append(chunk.data(), chunk.size());
      assert(buf_.size() <= max_buf_size_);
      return status;
    } else {
      return status;
    }
  }

  virtual Status SyncBefore(uint64_t offset) {
    if (offset_ >= offset) {
      return Status::OK();  // Data already flushed out
    } else {
      return EmptyBuffer();
    }
  }

  virtual Status Sync() {
    Status status = EmptyBuffer();
    if (status.ok()) {
      status = base_->Sync();
    }
    return status;
  }

  virtual Status Flush() {
    return Status::OK();  // Ignore all Flush() calls
  }

  virtual Status EmptyBuffer() {
    Status status;
    const size_t buf_size = buf_.size();
    assert(buf_size <= max_buf_size_);
    if (buf_size != 0) {
      status = base_->Append(buf_);
      if (status.ok()) {
        offset_ += buf_size;
        buf_.clear();
      }
    }
    return status;
  }

 private:
  WritableFile* base_;
  uint64_t offset_;  // Number of bytes flushed
  const size_t max_buf_size_;
  std::string buf_;
};

// Measurements used by a MeasuredWritableFile.
class WritableFileStats {
 public:
  WritableFileStats() { Reset(); }

  // Total number of bytes written out.
  uint64_t TotalBytes() const { return bytes_; }

  // Total number of write operations witnessed.
  uint64_t TotalOps() const { return ops_; }

 private:
  friend class MeasuredWritableFile;
  void Reset();

  uint32_t num_syncs_;
  uint32_t num_flushes_;
  uint64_t bytes_;
  uint64_t ops_;
};

// Measure the I/O activity accessing an underlying writable file and store the
// results in an external stats object.
// Implementation is not thread-safe and requires external synchronization for
// use by multiple threads.
class MeasuredWritableFile : public WritableFile {
 public:
  // *base must remain alive during the lifetime of this class and will be
  // closed and deleted when the destructor of this class is called.
  MeasuredWritableFile(WritableFileStats* stats, WritableFile* base)
      : stats_(stats) {
    Reset(base);
  }
  virtual ~MeasuredWritableFile() {
    if (base_ != NULL) {
      base_->Close();
      delete base_;
    }
  }

  // REQUIRES: External synchronization.
  virtual Status Flush() {
    if (base_ == NULL) {
      return Status::Disconnected(Slice());
    } else {
      Status status = base_->Flush();
      if (status.ok()) {
        stats_->num_flushes_++;
      }
      return status;
    }
  }

  // REQUIRES: External synchronization.
  virtual Status Sync() {
    if (base_ == NULL) {
      return Status::Disconnected(Slice());
    } else {
      Status status = base_->Sync();
      if (status.ok()) {
        stats_->num_syncs_++;
      }
      return status;
    }
  }

  // REQUIRES: External synchronization.
  virtual Status Append(const Slice& data) {
    if (base_ == NULL) {
      return Status::Disconnected(Slice());
    } else {
      Status status = base_->Append(data);
      if (status.ok()) {
        stats_->bytes_ += data.size();
        stats_->ops_ += 1;
      }
      return status;
    }
  }

  // REQUIRES: External synchronization.
  virtual Status Close() {
    if (base_ != NULL) {
      Status status = base_->Close();
      delete base_;
      base_ = NULL;
      return status;
    } else {
      return Status::OK();
    }
  }

 private:
  // Reset the counters and the base target.
  void Reset(WritableFile* base) {
    stats_->Reset();
    base_ = base;
  }

  WritableFileStats* stats_;
  WritableFile* base_;
};

// Measurements used by a MeasuredSequentialFile.
class SequentialFileStats {
 public:
  SequentialFileStats() { Reset(); }

  // Total number of bytes read in.
  uint64_t TotalBytes() const { return bytes_; }

  // Total number of read operations witnessed.
  uint64_t TotalOps() const { return ops_; }

 private:
  friend class MeasuredSequentialFile;
  void Reset();

  uint64_t bytes_;
  uint64_t ops_;
};

// Measure the I/O activity accessing an underlying sequential readable file and
// store the results in an external stats object.
// Implementation is not thread-safe and requires external synchronization for
// use by multiple threads.
class MeasuredSequentialFile : public SequentialFile {
 public:
  MeasuredSequentialFile(SequentialFileStats* stats, SequentialFile* base)
      : stats_(stats) {
    Reset(base);
  }
  virtual ~MeasuredSequentialFile() { delete base_; }

  // REQUIRES: External synchronization.
  virtual Status Read(size_t n, Slice* result, char* scratch) {
    if (base_ == NULL) {
      return Status::Disconnected(Slice());
    } else {
      Status status = base_->Read(n, result, scratch);
      if (status.ok()) {
        stats_->bytes_ += result->size();
        stats_->ops_ += 1;
      }
      return status;
    }
  }

  // REQUIRES: External synchronization.
  virtual Status Skip(uint64_t n) {
    if (base_ == NULL) {
      return Status::Disconnected(Slice());
    } else {
      return base_->Skip(n);
    }
  }

 private:
  // Reset the counters and the base target.
  void Reset(SequentialFile* base) {
    stats_->Reset();
    base_ = base;
  }

  SequentialFileStats* stats_;
  SequentialFile* base_;
};

// Measurements used by an MeasuredRandomAccessFile.
class RandomAccessFileStats {
 public:
  RandomAccessFileStats();
  ~RandomAccessFileStats();

  // Total number of bytes read in.
  uint64_t TotalBytes() const;

  // Total number of read operations witnessed.
  uint64_t TotalOps() const;

 private:
  friend class MeasuredRandomAccessFile;
  void AcceptRead(uint64_t n);
  void Reset();

  struct Rep;
  Rep* rep_;
};

// Measure the I/O activity accessing an underlying random access file and store
// the results in a set of external atomic counters.
class MeasuredRandomAccessFile : public RandomAccessFile {
 public:
  MeasuredRandomAccessFile(RandomAccessFileStats* stats, RandomAccessFile* base)
      : stats_(stats) {
    Reset(base);
  }
  virtual ~MeasuredRandomAccessFile() { delete base_; }

  // Safe for concurrent use by multiple threads.
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status status = base_->Read(offset, n, result, scratch);
    if (status.ok()) {
      stats_->AcceptRead(result->size());
    }
    return status;
  }

 private:
  // Reset the counters and the base target.
  void Reset(RandomAccessFile* base) {
    stats_->Reset();
    base_ = base;
  }

  RandomAccessFileStats* stats_;
  RandomAccessFile* base_;
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
