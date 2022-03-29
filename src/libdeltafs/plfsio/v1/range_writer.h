/*
 * Copyright (c) 2020 Carnegie Mellon University,
 * Copyright (c) 2020 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

//
// Created by Ankush J on 9/2/20.
//
#pragma once

#include <deque>

#include "types.h"
#include "compaction_mgr.h"
#include "ordered_builder.h"

namespace pdlfs {
namespace plfsio {

class PartitionManifestWriter;
class RangeWriterPerfLogger;

//
// a range writer manages a set of ordered blocks (OrderedBlockBuilder).
// this includes handling ranges/bounds, creating manifests,
// managing compaction, and writing it all to backing store.
// in the event of an async error (e.g. disk full, I/O error on writing)
// the compaction manager will store the error in its bg_status_
// and we'll return that to the caller.
//
class RangeWriter {
 public:

  //
  // the dir options contain key size, value size, thread pool info, etc.
  // manifests and data get written out to dst.   we evenly divide
  // our assigned key range into nsubpart sub partition blocks (you
  // can set nsubpart to 1 if subpartitions are not desired).   we also
  // track our nprevious assigned key ranges (not subparitioned) with
  // blocks and send added out of bounds data there.   we compact
  // blocks if adding data would cause them to grow beyond the
  // compact_threshold.   we also compact all active blocks
  // (nsubpart+nprevious) for flushes regardless of how full they are.
  // we will close dst at Finish() time.
  //
  RangeWriter(const DirOptions& options, WritableFile* dst,
              int nsubpart, int nprevious, size_t compact_threshold);
  ~RangeWriter();

  //
  // add a key/value pair to a block (hopefully one of our active
  // subpartition blocks, otherwise the data goes to a block associated
  // with one of our previous ranges).   if adding data to the block
  // would cause it to grow past the compaction threshold, then we
  // schedule a compaction on that block and allocate a free block
  // to replace it.  we must not be finished.  returns status.
  //
  Status Add(const Slice& k, const Slice& v);

  //
  // wait for all compaction I/O to finish
  //
  Status Wait() {
    MutexLock ml(&mu_);          // dtor unlocks
    return cmgr_.WaitForAll();
  }

  //
  // start compacting all in memory block data to backing store.
  // I/O may continue in the background.  we must not be finished.
  //
  Status Flush() {
    MutexLock ml(&mu_);          // dtor unlocks
    return StartAllCompactions();
  }

  //
  // flush out current epoch.   this will compact all active blocks,
  // wait for the compaction to finish, and update the manifest.
  //
  Status EpochFlush();

  //
  // compact all block data to backing store and wait for it to finsh.
  // then sync the backend dst_.
  //
  Status Sync();

  //
  // this starts all compactions (causing all blocks in bactive_[]
  // to be replaced with empty ones), then it retires the current
  // expected range to the set of previous ranges (discarding the
  // oldest previous range we currently have).   we set the
  // current range to rmin and rmax with the requested number of
  // subpartitions.
  //
  Status UpdateBounds(const float rmin, const float rmax);

  //
  // we are finished adding data to the range writer.  flush
  // out all data (waiting for it to finish), write the manifest,
  // write the footer, sync backing file, and then close backing file.
  // report logging (if enabled).  we will be marked finished
  // after this and no more I/O will be possible (dst_ is closed!).
  //
  Status Finish();

 private:
  const DirOptions& options_;              // config options we operate under
  const int nsubpart_;                     // # of subpartitions to use
  const int nprevious_;                    // # of prev blk ranges to keep
  const int nactive_;                      // sum of the above
  const int nstore_;                       // #allocated (>= to nactive_)
  const size_t compact_threshold_;         // blk size that triggers compact
  const size_t value_size_;                // from DirOptions

  port::Mutex mu_;                         // protects fields below
  port::CondVar bg_cv_;                    // tied to mu_
  bool finished_;                          // are finished writing?
  WritableFile* dst_;                      // backing file we output to
  uint64_t dst_offset_;                    // current offset in dst
  PartitionManifestWriter* manifest_;      // manifest mgt and writing
  CompactionMgr cmgr_;                     // our compaction manager
  RangeWriterPerfLogger* logger_;          // perf log, if not NULL
  OrderedBlockBuilder** block_store_;      // all blocks, created w/new[]
  std::deque<OrderedBlockBuilder*> bfree_; // free blocks
  OrderedBlockBuilder** bactive_;          // currently active blocks, new[]

  //
  // start compactions on all active blocks.  lock should be held.
  //
  Status StartAllCompactions();

  //
  // search through active blocks to find the one that the given key
  // belongs to.  we order the bactive_[] array so that all nsubpart_
  // blocks are first, followed by the nprevious_ block covering
  // previous ranges (oldest last).  if we can't find a good match,
  // we choose the oldest previous block.  lock should be held.
  //
  int FindBlock(float key) {
    mu_.AssertHeld();
    for (int lcv = 0 ; lcv < nactive_ ; lcv++) {
      if (bactive_[lcv]->Inside(key))
        return lcv;
    }
    return nactive_ - 1;   /* no match, choose oldest block we have */
  }

  //
  // static method compaction callback for compaction threads.
  // we simply redirect it to the owner's compact method...
  //
  static Status CompactCB(uint32_t cseq, void *item, void *owner) {
    OrderedBlockBuilder* blk = reinterpret_cast<OrderedBlockBuilder*>(item);
    RangeWriter* us = reinterpret_cast<RangeWriter*>(owner);
    return us->Compact(cseq, blk);
  }

  //
  // compact a block and return the status.  we serialize writes
  // to backing store using the cmgr_ and cseq number.   lock not held.
  //
  Status Compact(uint32_t cseq, OrderedBlockBuilder* blk);
};

}  // namespace plfsio
}  // namespace pdlfs
