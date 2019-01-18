/*
 * Copyright (c) 2015-2019 Carnegie Mellon University and
 *         Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/env.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/status.h"

namespace pdlfs {
namespace plfsio {

class DoubleBuffering {
 public:
  DoubleBuffering(port::Mutex*, port::CondVar*, void* buf0, void* buf1);

  // Append data into the buffer. Return OK on success, or a non-OK status on
  // errors. REQUIRES: Finish() has not been called.
  template <typename T>
  Status Add(const Slice& k, const Slice& v);

  // Force a buffer flush (compaction) and maybe wait for it.
  // Compaction does not force data to be sync'ed. Sync() does.
  // Return OK on success, or a non-OK status on errors.
  // REQUIRES: Finish() has not been called.
  template <typename T>
  Status Flush(bool wait);

  // Wait for all outstanding compactions to clear. Return OK on success,
  // or a non-OK status on errors.
  // REQUIRES: Finish() has not been called.
  template <typename T>
  Status Wait();

  // Sync data to storage. By default, only data already scheduled for
  // compaction is sync'ed. Data in write buffer that is not yet scheduled for
  // compaction is not sync'ed, unless do_flush is set to true. Will wait
  // until all outstanding compactions are done before performing the sync.
  // Return OK on success, or a non-OK status on errors.
  // REQUIRES: Finish() has not been called.
  template <typename T>
  Status Sync(bool do_flush);

  // Finalize the writes because all writes are done.
  // All data in write buffer will be scheduled for compaction and will be
  // sync'ed to storage after the compaction. Return OK on success, or a non-OK
  // status on errors. No more write operations after this call.
  template <typename T>
  Status Finish();

 protected:
  port::Mutex* mu_;
  port::CondVar* bg_cv_;

  template <typename T>
  Status Prepare(bool force = true, const Slice& k = Slice(),
                 const Slice& v = Slice());
  template <typename T>
  void MaybeScheduleCompaction();
  template <typename T>
  void WaitForCompaction();
  template <typename T>
  void DoCompaction();

  // State below is protected by mu_
  uint32_t num_flush_requested_;
  uint32_t num_flush_completed_;
  bool finished_;  // If Finish() has been called
  // True if the current compaction is forced by Flush()
  bool is_compaction_forced_;
  bool has_bg_compaction_;
  Status bg_status_;
  void* mem_buf_;
  void* imm_buf_;
  void* buf0_;
  void* buf1_;
};

}  // namespace plfsio
}  // namespace pdlfs
