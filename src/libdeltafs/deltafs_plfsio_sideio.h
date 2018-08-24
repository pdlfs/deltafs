/*
 * Copyright (c) 2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/env.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/status.h"

#include <string>

namespace pdlfs {
namespace plfsio {

// Shared options object from which we obtain the thread pool for executing
// background compactions.
struct DirOptions;

// Directly write data into a directory.
// That is, data is written to a log file without any indexing.
class DirectWriter {
 public:
  // BUG: deadlock when the write size is greater than the write buffer size.
  // This is because the current version of the code will flush buffer if the
  // buffer is not large enough to accept the incoming write. If the write size
  // itself is larger than the buffer size, the current code is going to keep
  // flushing buffers and never stop. Is this a problem? Likely not.
  DirectWriter(const DirOptions& opts, WritableFile* dst, size_t buf_size);
  ~DirectWriter();

  // Append data into the directory.
  // Return OK on success, or a non-OK status on errors.
  // REQUIRES: Finish() has not been called.
  Status Append(const Slice& data);

  // Force a buffer flush (compaction) and maybe wait for it.
  // Compaction does not force data to be sync'ed. Sync() does.
  // Return OK on success, or a non-OK status on errors.
  // REQUIRES: Finish() has not been called.
  struct FlushOptions {
    FlushOptions() : wait(false) {}
    // Wait for the compaction to complete.
    // Default: false
    bool wait;
  };
  Status Flush(const FlushOptions& options);

  // Wait for all on-going compactions to finish.
  // Return immediately if Finish() has been called.
  // Return OK on success, or a non-OK status on errors.
  Status Wait();

  // Sync data to storage. By default, only data already scheduled for
  // compaction is sync'ed. Data in write buffer that is not yet scheduled for
  // compaction is not sync'ed. Return immediately if Finish() has been called.
  // Return OK on success, or a non-OK status on errors.
  struct SyncOptions {
    SyncOptions() : do_flush(false) {}
    // Force a write buffer flush.
    // Default: false
    bool do_flush;
  };
  Status Sync(const SyncOptions& options);

  // Finalize the writes because all writes are done.
  // All data in write buffer is scheduled for compaction and is sync'ed to
  // storage after the compaction. Return OK on success, or a non-OK status on
  // errors. No more write operations after this call.
  Status Finish();

 private:
  const DirOptions& options_;
  WritableFile* const dst_;
  port::Mutex mu_;
  port::CondVar bg_cv_;
  size_t buf_threshold_;  // Threshold for write buffer flush
  // Memory pre-reserved for each write buffer
  size_t buf_reserv_;

  void WaitForCompaction();
  Status Prepare(const Slice& data, bool force = false);
  static void BGWork(void*);
  void MaybeScheduleCompaction();
  void DoCompaction();

  // No copying allowed
  void operator=(const DirectWriter& dw);
  DirectWriter(const DirectWriter&);

  // State below is protected by mu_
  uint32_t num_flush_requested_;  // Incremented by Flush()
  uint32_t num_flush_completed_;
  bool finished_;  // If Finish() has been called
  // True if compaction is forced by Flush()
  bool is_compaction_forced_;
  bool has_bg_compaction_;
  Status bg_status_;
  std::string* mem_buf_;
  std::string* imm_buf_;
  std::string buf0_;
  std::string buf1_;
};

// A simple wrapper on top of a RandomAccessFile.
class DirectReader {
 public:
  DirectReader(const DirOptions& options, RandomAccessFile* src);
  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const;

 private:
  const DirOptions& options_;
  RandomAccessFile* const src_;

  // No copying allowed
  void operator=(const DirectReader& dr);
  DirectReader(const DirectReader&);
};

}  // namespace plfsio
}  // namespace pdlfs
