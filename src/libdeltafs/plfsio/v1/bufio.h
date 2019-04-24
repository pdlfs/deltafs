/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "doublebuf.h"

#include <string>

namespace pdlfs {
namespace plfsio {

// Shared options object from which we obtain the thread pool for executing
// background compactions.
struct DirOptions;

// Directly write data into a directory.
// That is, data is written to a log file without any indexing.
class DirectWriter : public DoubleBuffering {
 public:
  // Deadlock when the write size is greater than the write buffer size.
  // This is because the current version of the code will flush buffer if the
  // buffer is not large enough to accept the incoming write. If the write size
  // itself is larger than the buffer size, the current code is going to keep
  // flushing buffers and never stop. Is this a problem? Likely not.
  DirectWriter(const DirOptions& opts, WritableFile* dst, size_t buf_size);

  // REQUIRES: Finish() has NOT been called.
  // Insert data into the writer.
  Status Append(const Slice& data);
  // Wait until there is no outstanding compactions.
  Status Wait();
  // Force a compaction.
  Status Flush();
  // Sync data to storage.
  Status Sync();

  // Finalize the writer.
  Status Finish();

  ~DirectWriter();

 private:
  const DirOptions& options_;
  WritableFile* const dst_;
  port::Mutex mu_;
  port::CondVar bg_cv_;
  const size_t buf_threshold_;  // Threshold for write buffer flush
  // Memory pre-reserved for each write buffer
  size_t buf_reserv_;

  friend class DoubleBuffering;
  Status Compact(uint32_t seq, void* buf);
  Status SyncBackend(bool close = false);
  void ScheduleCompaction(uint32_t seq, void* buf);
  void Clear(void* buf) { static_cast<std::string*>(buf)->resize(0); }
  void AddToBuffer(void* buf, const Slice& k, const Slice& v) {
    std::string* const s = static_cast<std::string*>(buf);
    s->append(k.data(), k.size());
    s->append(v.data(), v.size());
  }
  bool HasRoom(const void* buf, const Slice& k, const Slice& v) {
    return (static_cast<const std::string*>(buf)->size() + k.size() +
                v.size() <=
            buf_threshold_);
  }
  bool IsEmpty(const void* buf) {
    return static_cast<const std::string*>(buf)->empty();
  }
  static void BGWork(void*);

  std::string str0_;
  std::string str1_;
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
