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

#include <deque>
#include <string>

#include "compaction_mgr.h"

namespace pdlfs {
namespace plfsio {

// Directly write data into a directory.
// That is, data is written to a log file without any indexing.
class DirectWriter {
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
  Status Wait() {
    MutexLock ml(&mu_);          // dtor unlocks
    return cmgr_.WaitForAll();
  }

  // Force a compaction.
  Status Flush() {
    MutexLock ml(&mu_);          // dtor unlocks
    return StartAllCompactions();
  }

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
  bool finished_;                          // are finished writing?
  CompactionMgr cmgr_;                     // our compaction manager
  std::deque<std::string*> bufs_;          // free buffer(s)
  std::string* membuf_;                    // current active buffer
  std::string str_[2];                     // use two strings for buffers

  //
  // start compaction on active block.  lock should be held.
  //
  Status StartAllCompactions();

  //
  // static method compaction callback for compaction threads.
  // we simply redirect it to the owner's compact method...
  //
  static Status CompactCB(uint32_t cseq, void *item, void *owner) {
    std::string* buf = reinterpret_cast<std::string*>(item);
    DirectWriter* us = reinterpret_cast<DirectWriter*>(owner);
    return us->Compact(cseq, buf);
  }

  //
  // compact a block and return the status.  we serialize writes
  // to backing store using the cmgr_ and cseq number.   lock not held.
  //
  Status Compact(uint32_t cseq, std::string* buf);
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
