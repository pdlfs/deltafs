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
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/status.h"

#include <assert.h>
#include <stddef.h>

namespace pdlfs {
namespace plfsio {

class DoubleBuffering {
 public:
  DoubleBuffering(port::Mutex*, port::CondVar*, void* buf0, void* buf1);

  // Append data into the buffer. Return OK on success, or a non-OK status on
  // errors. REQUIRES: __Finish() has NOT been called.
  template <typename T>
  Status __Add(const Slice& k, const Slice& v);

  // Force a buffer flush (compaction) and maybe wait for it.
  // Compaction does not force data to be sync'ed. Sync() does.
  // Return OK on success, or a non-OK status on errors.
  // REQUIRES: __Finish() has NOT been called.
  template <typename T>
  Status __Flush(bool wait);

  // Sync data to storage. By default, only data that is already scheduled
  // for compaction is sync'ed. Data that is in the write buffer and not yet
  // scheduled for compaction is not sync'ed, unless do_flush is set to true.
  // Will wait until all outstanding compactions are done before performing
  // the sync. Return OK on success, or a non-OK status on errors.
  // REQUIRES: __Finish() has NOT been called.
  template <typename T>
  Status __Sync(bool do_flush);

  // Wait until there is no outstanding compactions.
  // REQUIRES: __Finish() has NOT been called.
  Status __Wait();

  // Finalize the writes because all writes are done. All data in write buffer
  // will be scheduled for compaction and will be sync'ed to storage after
  // the compaction. Return OK on success, or a non-OK status on errors.
  // NOTE: No more write operations after this call.
  template <typename T>
  Status __Finish();

 protected:
  port::Mutex* mu_;
  port::CondVar* bg_cv_;

  template <typename T>
  Status Prepare(uint32_t* compac_seq, bool force = true,
                 const Slice& k = Slice(), const Slice& v = Slice());
  void WaitFor(uint32_t compac_seq);
  void WaitForCompactions();
  template <typename T>
  void TryScheduleCompaction(uint32_t*);
  template <typename T>
  void DoCompaction();

  // State below is protected by mu_
  uint32_t num_compac_scheduled_;
  uint32_t num_compac_completed_;
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

#define __this static_cast<T*>(this)

// Finalize all writes and sync all remaining data in the write buffer to
// storage. Return OK on success, or a non-OK status on errors.
// REQUIRES: mu_ has been LOCKed.
template <typename T>
Status DoubleBuffering::__Finish() {
  mu_->AssertHeld();
  Status finish_status;
  if (finished_)  // __Finish() has already been called.
    return bg_status_;
  else {
    __Flush<T>(false);
  }

  // Wait until !has_bg_compaction_
  WaitForCompactions();
  if (bg_status_.ok()) {  // Sync and close
    bg_status_ = __this->SyncBackend(true /* close */);
    finish_status = bg_status_;
    bg_status_ =
        Status::AssertionFailed("Already finished", finish_status.ToString());
  } else {
    finish_status = bg_status_;
  }

  finished_ = true;
  return finish_status;
}

// Sync data so data hits storage, but will wait until all outstanding
// compactions are completed before performing the sync operation.
// Return OK on success, or a non-OK status on errors.
// REQUIRES: __Finish() has NOT been called.
// REQUIRES: mu_ has been LOCKed.
template <typename T>
Status DoubleBuffering::__Sync(bool flush) {
  mu_->AssertHeld();
  uint32_t my_compac_seq = 0;
  Status status;
  if (finished_)  // __Finish() has already been called
    status = bg_status_;
  else {
    status = Prepare<T>(&my_compac_seq, flush);
  }

  if (!status.ok()) {
    return status;
  } else {
    // If compaction is scheduled, wait for it until num_compac_completed_
    // >= my_compac_seq, otherwise my_compac_seq is 0 and
    // WaitFor(seq) will return immediately.
    WaitFor(my_compac_seq);
    // Then, wait until !has_bg_compaction_
    WaitForCompactions();
    if (bg_status_.ok()) {
      bg_status_ = __this->SyncBackend();
    }
    return bg_status_;
  }
}

// Force a compaction and maybe wait for it to complete.
// REQUIRES: __Finish() has NOT been called.
// REQUIRES: mu_ has been LOCKed.
template <typename T>
Status DoubleBuffering::__Flush(bool wait) {
  mu_->AssertHeld();
  uint32_t my_compac_seq = 0;
  Status status;
  if (finished_)  // __Finish() has already been called
    status = bg_status_;
  else {
    status = Prepare<T>(&my_compac_seq);
  }

  if (status.ok() && wait) {  // Wait for compaction to clear
    WaitFor(my_compac_seq);
    return bg_status_;
  } else {
    return status;
  }
}

// Insert data into the buffer.
// Return OK on success, or a non-OK status on errors.
// REQUIRES: __Finish() has NOT been called.
// REQUIRES: mu_ has been LOCKed.
template <typename T>
Status DoubleBuffering::__Add(const Slice& k, const Slice& v) {
  mu_->AssertHeld();
  uint32_t ignored_compac_seq;
  Status status;
  if (finished_)  // __Finish() has already been called
    status = bg_status_;
  else {
    status = Prepare<T>(&ignored_compac_seq, false /* !force */, k, v);
    if (status.ok()) {  // Subclass performs the actual insertion
      __this->AddToBuffer(mem_buf_, k, v);
    }
  }

  return status;
}

// REQUIRES: mu_ has been LOCKed.
template <typename T>
Status DoubleBuffering::Prepare(uint32_t* seq, bool force, const Slice& k,
                                const Slice& v) {
  mu_->AssertHeld();
  Status status;
  while (true) {
    assert(mem_buf_);
    if (!bg_status_.ok()) {
      status = bg_status_;
      break;
    } else if (!force && __this->HasRoom(mem_buf_, k, v)) {
      // There is room in current write buffer
      break;
    } else if (has_bg_compaction_) {
      bg_cv_->Wait();  // Wait for background compactions to finish
    } else {
      // Attempt to switch to a new write buffer
      force = false;
      assert(!imm_buf_);
      imm_buf_ = mem_buf_;
      TryScheduleCompaction<T>(seq);
      void* const current_buf = mem_buf_;
      if (current_buf == buf0_) {
        mem_buf_ = buf1_;
      } else {
        mem_buf_ = buf0_;
      }
    }
  }

  return status;
}

// REQUIRES: mu_ has been LOCKed.
template <typename T>
void DoubleBuffering::TryScheduleCompaction(uint32_t* compac_seq) {
  mu_->AssertHeld();

  *compac_seq = ++num_compac_scheduled_;
  has_bg_compaction_ = true;

  if (__this->IsEmpty(imm_buf_)) {
    // Buffer is empty so compaction should be quick. As such we directly
    // execute the compaction in the current thread
    DoCompaction<T>();  // No context switch
  } else {
    __this->ScheduleCompaction();
  }
}

// REQUIRES: mu_ has been LOCKed.
template <typename T>
void DoubleBuffering::DoCompaction() {
  mu_->AssertHeld();
  assert(has_bg_compaction_);
  assert(imm_buf_);
  Status status = __this->Compact(imm_buf_);
  assert(bg_status_.ok());
  bg_status_ = status;
  __this->Clear(imm_buf_);
  imm_buf_ = NULL;
  ++num_compac_completed_;
  has_bg_compaction_ = false;
  // Just finished one compaction
  // Try another one
  uint32_t ignored_compac_seq;
  Prepare<T>(&ignored_compac_seq, false /* !force */);
  bg_cv_->SignalAll();
}

#undef __this

}  // namespace plfsio
}  // namespace pdlfs
