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
#include "pdlfs-common/env_files.h"
#include "pdlfs-common/port.h"

#include <map>
#include <string>

// This module provides the abstraction for accessing data stored in
// an underlying storage using a log-structured format. Data is written,
// append-only, into a "sink", and is read from a "source".

namespace pdlfs {
namespace plfsio {

// Log types
enum LogType {
  // Default type, contains data blocks
  kData = 0x00,  // Optimized for random read access

  // Index log with table indexes, filters, and other index blocks
  kIndex = 0x01  // Sequential reads expected
};

// Log rotation types.
// Store logs as separated files.
enum RotationType {
  // Do not rotate log files
  kNoRotation = 0x00,
  // Log rotation is controlled by external user code
  kExtCtrl = 0x01
};

// Accumulate a certain amount of data before writing
typedef MinMaxBufferedWritableFile BufferedFile;

class RollingLogFile;

// Abstraction for writing data to storage.
// Implementation is not thread-safe. External synchronization is needed for
// multi-threaded access.
class LogSink {
 public:
  // Options for monitoring, naming, write buffering,
  // and file rotation.
  struct LogOptions {
    LogOptions();

    // Rank # of the calling process
    int rank;

    // Sub-partition index # of the log
    // Set to "-1" to indicate there is no sub-partitions
    int sub_partition;

    // Max write buffering in bytes
    // Set to "0" to disable
    size_t max_buf;

    // Min write buffering in bytes
    // Set to "0" to disable
    size_t min_buf;

    // Log rotation
    RotationType rotation;

    // Type of the log
    LogType type;

    // Allow synchronization among multiple threads
    port::Mutex* mu;

    // Enable i/o stats monitoring
    WritableFileStats* stats;

    // Low-level storage abstraction
    Env* env;
  };

 private:
  LogSink(const LogOptions& opts, const std::string& p,  // Parent directory
          BufferedFile* buf, RollingLogFile* vf)
      : opts_(opts),
        prefix_(p),
        buf_file_(buf),
        rlog_(vf),
        mu_(opts_.mu),
        env_(opts_.env),
        prev_off_(0),
        off_(0),
        file_(NULL),  // Initialized by Open()
        refs_(0) {}

 public:
  // Create a log sink instance for writing data according to the given set of
  // options. Return OK on success, or a non-OK status on errors.
  static Status Open(const LogOptions& opts, const std::string& prefix,
                     LogSink** result);

  // Return the current logic write offset.
  uint64_t Ltell() const {
    if (mu_ != NULL) mu_->AssertHeld();
    return off_;
  }

  void Lock() {
    if (mu_ != NULL) {
      mu_->Lock();
    }
  }

  // Append data into the storage.
  // Return OK on success, or a non-OK status on errors.
  // May lose data until the next Lsync().
  // REQUIRES: Lclose() has not been called.
  Status Lwrite(const Slice& data) {
    if (file_ == NULL) {
      return Status::AssertionFailed("Log already closed", filename_);
    } else {
      if (mu_ != NULL) {
        mu_->AssertHeld();
      }
      Status result = file_->Append(data);
      if (result.ok()) {
        // File implementation may ignore the flush
        result = file_->Flush();
        if (result.ok()) {
          off_ += data.size();
        }
      }
      return result;
    }
  }

  // Force data to be written to storage.
  // Return OK on success, or a non-OK status on errors.
  // Data previously buffered will be forcefully flushed out.
  Status Lsync() {
    Status status;
    if (file_ != NULL) {
      if (mu_ != NULL) mu_->AssertHeld();
      status = file_->Sync();
    }
    return status;
  }

  void Unlock() {
    if (mu_ != NULL) {
      mu_->Unlock();
    }
  }

  // Return the memory space for write buffering.
  std::string* buffer_store() {
    if (buf_file_ != NULL) {
      return buf_file_->buffer_store();
    } else {
      return NULL;
    }
  }

  // Close the log so no further writes will be accepted.
  // Return OK on success, or a non-OK status on errors.
  // If sync is set to true, will force data sync before closing the log.
  Status Lclose(bool sync = false);
  // Flush and close the current log file and redirect
  // all future writes to a new log file.
  Status Lrotate(int index, bool sync = false);
  uint64_t Ptell() const;  // Return the current physical log offset
  void Ref() { refs_++; }
  void Unref();

 private:
  ~LogSink();  // Triggers Finish()
  // No copying allowed
  void operator=(const LogSink& s);
  LogSink(const LogSink&);
  Status Finish();  // Internally used by Lclose()

  // Constant after construction
  const LogOptions opts_;
  const std::string prefix_;  // Parent directory name
  // NULL if write buffering is disabled
  BufferedFile* const buf_file_;
  // NULL if log rotation is disabled
  RollingLogFile* const rlog_;
  port::Mutex* const mu_;
  Env* const env_;

  // State below is protected by mu_
  Status finish_status_;
  uint64_t prev_off_;
  uint64_t off_;  // Logic write offset, monotonically increasing
  // NULL if Finish() has been called
  WritableFile* file_;
  std::string filename_;  // Name of the current log file
  uint32_t refs_;
};

// Abstraction for reading data from a log file, which may
// consist of several pieces due to log rotation.
class LogSource {
 public:
  // Options for naming, pre-fetching, and monitoring.
  struct LogOptions {
    LogOptions();

    // Rank # of the calling process
    int rank;

    // Sub-partition index # of the log.
    // Set to "-1" to indicate there is no sub-partitions
    int sub_partition;

    // Number of log rotations performed.
    // Set to "-1" to indicate the log was never rotated
    int num_rotas;

    // Type of the log.
    // For index logs, the entire log data will be eagerly fetched
    // and cached in memory
    LogType type;

    // For i/o stats monitoring (sequential reads)
    SequentialFileStats* seq_stats;

    // For i/o stats monitoring
    RandomAccessFileStats* stats;

    // Bulk read size
    size_t io_size;

    // Low-level storage abstraction
    Env* env;
  };

  // Create a log source instance for reading data according to a given set of
  // options. Return OK on success, or a non-OK status on errors.
  static Status Open(const LogOptions& opts, const std::string& prefix,
                     LogSource** result);

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch,
              size_t index = 0) {
    Status status;
    if (index < num_files_) {
      RandomAccessFile* const f = files_[index].first;
      status = f->Read(offset, n, result, scratch);  // May return cached data
    } else {
      *result = Slice();  // Return empty data
    }
    return status;
  }

  // Return the total data size
  uint64_t Size() const {
    uint64_t result = 0;
    for (size_t i = 0; i < num_files_; i++) {
      result += files_[i].second;
    }
    return result;
  }

  void Ref() { refs_++; }
  void Unref();

 private:
  LogSource(const LogOptions& opts, const std::string& p)
      : opts_(opts), prefix_(p), files_(NULL), num_files_(0), refs_(0) {}
  ~LogSource();
  // No copying allowed
  void operator=(const LogSource& s);
  LogSource(const LogSource&);

  // Constant after construction
  const LogOptions opts_;
  const std::string prefix_;  // Parent directory name
  std::pair<RandomAccessFile*, uint64_t>* files_;
  size_t num_files_;
  uint32_t refs_;
};

}  // namespace plfsio
}  // namespace pdlfs
