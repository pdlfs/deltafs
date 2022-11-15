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

#include "builder.h"
#include "compaction_mgr.h"
#include "filter.h"

namespace pdlfs {
namespace plfsio {

#if 0 // XXXCDC
// Block type.
class ArrayBlockBuilder;
#endif

// Directly write data as formatted data blocks.
// Incoming key-value pairs are assumed to be fixed sized.
class BufferedBlockWriter {
 public:
  BufferedBlockWriter(const DirOptions& options, WritableFile* dst,
                      size_t buf_size, size_t n);

  // REQUIRES: Finish() has NOT been called.
  // Insert data into the writer.
  Status Add(const Slice& k, const Slice& v);

  // Wait until there is no outstanding compactions.
  Status Wait() {
    MutexLock ml(&mu_);          // dtor unlocks
    return cmgr_.WaitForAll();
  }

  // Force an epoch flush.
  Status EpochFlush();

  // Force a compaction.
  Status Flush() {
    MutexLock ml(&mu_);          // dtor unlocks
    return StartAllCompactions();
  }

  // Sync data to storage.
  Status Sync();

  // Finalize the writer.
  Status Finish();

  ~BufferedBlockWriter();

 private:
  typedef ArrayBlockBuilder BlockBuf;
  typedef ArrayBlock Block;
  typedef BloomBlock BloomBuilder;
  const DirOptions& options_;
  WritableFile* const dst_;
  port::Mutex mu_;
  port::CondVar bg_cv_;
  const size_t buf_threshold_;  // Threshold for write buffer flush
  // Memory pre-reserved for each write buffer
  size_t buf_reserv_;
  uint64_t offset_;  // Current write offset
  std::string bloomfilter_;
  std::string indexes_;
  bool finished_;                          // are finished writing?
  CompactionMgr cmgr_;                     // our compaction manager
  std::deque<BlockBuf*> bufs_;             // free buffer(s)
  BlockBuf* membuf_;                       // current active buffer
  BlockHandle bloomfilter_handle_;
  BlockHandle index_handle_;
  BlockBuf** bbs_;
  size_t n_;

  //
  // start compaction on active block.  lock should be held.
  //
  Status StartAllCompactions();

  //
  // static method compaction callback for compaction threads.
  // we simply redirect it to the owner's compact method...
  //
  static Status CompactCB(uint32_t cseq, void *item, void *owner) {
    BlockBuf* buf = reinterpret_cast<BlockBuf*>(item);
    BufferedBlockWriter* us = reinterpret_cast<BufferedBlockWriter*>(owner);
    return us->Compact(cseq, buf);
  }

  //
  // compact a block and return the status.  we serialize writes
  // to backing store using the cmgr_ and cseq number.   lock not held.
  //
  Status Compact(uint32_t cseq, BlockBuf* buf);

  Status DumpIndexesAndFilters();
  Status Close();
};

// Read data written by a BufferedBlockWriter.
class BufferedBlockReader {
 public:
  BufferedBlockReader(const DirOptions& options, RandomAccessFile* src,
                      uint64_t src_sz);

  Status Get(const Slice& k, std::string* result);

 private:
  typedef ArrayBlock Block;
  const DirOptions& options_;
  RandomAccessFile* const src_;
  uint64_t src_sz_;
  Status cache_status_;  // OK if cache is ready
  Slice cache_contents_;
  std::string cache_;

  bool GetFrom(Status* status, const Slice& k, std::string* result,
               uint64_t off, size_t n);
  Status LoadIndexesAndFilters(Slice* footer);
  Status MaybeLoadCache();

  Slice bloomfilter_;
  Slice indexes_;
};

}  // namespace plfsio
}  // namespace pdlfs
