#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
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
  // Approximate size of user data packed per block.
  // This usually corresponds to the size of each I/O request
  // sent to the underlying storage.
  // Default: 128K
  size_t block_size;

  // Approximate size of user data packed per table.
  // This corresponds to the size of the in-memory write buffer
  // we must allocate for each log stream.
  // Default: 2M
  size_t table_size;

  // Thread pool used to run background compaction jobs
  // Set to NULL to disable background jobs.
  // Default: NULL
  ThreadPool* compaction_pool;
};

// Abstraction for a non-thread-safe un-buffered
// append-only log file.
class LogSink {
 public:
  LogSink(WritableFile* f, uint64_t s) : file_(f), offset_(s) {}
  LogSink(WritableFile* f) : file_(f), offset_(0) {}

  // XXX: Keep the file open
  ~LogSink() {}

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

 private:
  WritableFile* file_;
  uint64_t offset_;
};

class Writer {
 public:
  Writer() {}
  virtual ~Writer();

  virtual Status Append(const Slice& fname, const Slice& data) = 0;
  virtual Status MakeEpoch() = 0;

 private:
  // No copying allowed
  void operator=(const Writer&);
  Writer(const Writer&);
};

}  // namespace plfsio
}  // namespace plfs
