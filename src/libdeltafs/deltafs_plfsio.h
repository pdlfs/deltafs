#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stddef.h>

#include "pdlfs-common/env.h"

namespace pdlfs {
namespace plfsio {

struct Options {
  Options();

  // Approximate size of user data packed per block.
  // This usually corresponds to the size of each I/O request
  // sent to the underlying storage.
  // Default: 64K
  size_t block_size;

  // Approximate size of user data packed per table.
  // This corresponds to the size of the in-memory write buffer
  // we must allocate for each log stream.
  // Default: 32M
  size_t table_size;

  // Thread pool used to run background compaction jobs.
  // Set to NULL to disable background jobs so all compactions will
  // run as foreground jobs.
  // Default: NULL
  ThreadPool* compaction_pool;

  // True if write operations should be performed in a non-blocking manner,
  // in which case a special status is returned instead of blocking the
  // writer to wait for buffer space.
  // Default: false
  bool non_blocking;

  // Number of microseconds to slowdown if a writer cannot make progress
  // because the system has run out of its buffer space.
  // Default: 0
  uint64_t slowdown_micros;

  // Number of partitions to divide the data. Specified in logarithmic
  // number so each x will give 2**x partitions.
  // REQUIRES: 0 <= lg_parts <= 8
  // Default: 0
  int lg_parts;

  // Rank of the process.
  // Default: 0
  int rank;

  // Env instance used to access raw files stored in the underlying
  // storage system. If NULL, Env::Default() will be used.
  // Default: NULL
  Env* env;
};

// Abstraction for a thread-unsafe and possibly-buffered
// append-only log stream.
class LogSink {
 public:
  LogSink(WritableFile* f, uint64_t s) : file_(f), offset_(s), refs_(0) {}
  explicit LogSink(WritableFile* f) : file_(f), offset_(0), refs_(0) {}

  uint64_t Ltell() const { return offset_; }

  Status Lwrite(const Slice& data) {
    Status result = file_->Append(data);
    if (result.ok()) {
      result = file_->Flush();
      if (result.ok()) {
        offset_ += data.size();
      }
    }
    return result;
  }

  void Ref() { refs_++; }
  void Unref();

 private:
  enum { kSyncBeforeClosing = true };

  ~LogSink();
  // No copying allowed
  void operator=(const LogSink&);
  LogSink(const LogSink&);

  WritableFile* file_;
  uint64_t offset_;
  uint32_t refs_;
};

// Abstraction for a thread-unsafe and possibly-buffered
// random access log file.
class LogSource {
 public:
  explicit LogSource(RandomAccessFile* f) : file_(f) {}

  // Read the entire file into a string
  Status Load(std::string* dst) {
    Status s;
    char* space = new char[kBufferSize];
    uint64_t offset = 0;
    while (true) {
      Slice fragment;
      s = file_->Read(offset, kBufferSize, &fragment, space);
      if (!s.ok()) {
        break;
      }
      if (!fragment.empty()) {
        AppendSliceTo(dst, fragment);
        offset += fragment.size();
      } else {
        break;
      }
    }
    delete[] space;
    return s;
  }

 private:
  // Larger I/O gives less operations
  enum { kBufferSize = 1024 * 1024 };

  ~LogSource();
  // No copying allowed
  void operator=(const LogSource&);
  LogSource(const LogSource&);

  RandomAccessFile* file_;
};

// Destroy the contents of the specified directory.
// Be very careful using this method.
extern Status DestroyDir(const std::string& dirname, const Options& options);

// Deltafs plfs-style N-1 I/O writer api.
class Writer {
 public:
  Writer() {}
  virtual ~Writer();

  // Open an I/O writer against a specified plfs-style directory.
  // Return OK on success, or a non-OK status on errors.
  static Status Open(const Options& options, const std::string& dirname,
                     Writer** result);

  // Append a piece of data to a specified file under a given plfs directory.
  // REQUIRES: Finish() has not been called.
  virtual Status Append(const Slice& fname, const Slice& data) = 0;

  // Sync data to storage.  Not supported so far.
  // REQUIRES: Finish() has not been called.
  virtual Status Sync() = 0;

  // Flush data to make an epoch that benefits future read operations.
  // REQUIRES: Finish() has not been called.
  virtual Status MakeEpoch() = 0;

  // Flush data and finalize all indexing.
  virtual Status Finish() = 0;

 private:
  // No copying allowed
  void operator=(const Writer&);
  Writer(const Writer&);
};

// Deltafs plfs-style N-1 I/O reader api.
class Reader {
 public:
  Reader() {}
  virtual ~Reader();

  // Open an I/O reader against a specific plfs-style directory.
  // Return OK on success, or a non-OK status on errors.
  static Status Open(const Options& options, const std::string& dirname,
                     Reader* result);

  // List files under a given plfs directory.  Not supported so far.
  virtual void List(std::vector<std::string>* names) = 0;

  // Fetch the entire data from a specific file under a given plfs directory.
  virtual Status Retrieve(const Slice& fname, std::string* dst) = 0;

  // Return true iff a specific file exists under a given plfs directory.
  virtual bool Touch(const Slice& fname) = 0;

 private:
  // No copying allowed
  void operator=(const Reader&);
  Reader(const Reader&);
};

}  // namespace plfsio
}  // namespace pdlfs
