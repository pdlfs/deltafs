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
  void WaitForCompaction();
  template <typename T>
  void MaybeScheduleCompaction();
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

#define THIS static_cast<T*>(this)

// Finalize all writes and sync all remaining data to storage.
// Return OK on success, or a non-OK status on errors.
template <typename T>
Status DoubleBuffering::Finish() {
  MutexLock ml(mu_);
  if (finished_)
    return bg_status_;
  else {
    Status status = Prepare<T>();
    if (status.ok()) {
      WaitForCompaction();
    }
  }

  bg_status_ = THIS->SyncBackend(true /* close */);
  finished_ = true;
  return bg_status_;
}

// Sync data so data hits storage, but will wait until all outstanding
// compactions are completed before performing the sync operation.
// Return OK on success, or a non-OK status on errors.
// REQUIRES: Finish() has not been called.
template <typename T>
Status DoubleBuffering::Sync(bool force_flush) {
  MutexLock ml(mu_);
  if (finished_)
    return Status::AssertionFailed("Already finished");
  else {
    Status status = Prepare<T>(force_flush);
    if (status.ok()) {
      WaitForCompaction();
    }
  }

  bg_status_ = THIS->SyncBackend();
  return bg_status_;
}

// Wait until all outstanding compactions to clear. INVARIANT: no
// compaction has been scheduled at the moment this function returns.
// Return OK on success, or a non-OK status on errors.
template <typename T>
Status DoubleBuffering::Wait() {
  MutexLock ml(mu_);
  if (finished_)
    return Status::AssertionFailed("Already finished");
  else {
    WaitForCompaction();
  }

  return bg_status_;
}

// Force a buffer flush.
// REQUIRES: Finish() has not been called.
template <typename T>
Status DoubleBuffering::Flush(bool wait) {
  MutexLock ml(mu_);
  if (finished_)
    return Status::AssertionFailed("Already finished");
  else {
    // Wait for buffer space
    while (imm_buf_ != NULL) {
      bg_cv_->Wait();
    }
  }

  Status status;
  if (!bg_status_.ok()) {
    status = bg_status_;
  } else {
    num_flush_requested_++;
    const uint32_t my = num_flush_requested_;
    status = Prepare<T>();
    if (status.ok()) {
      if (wait) {
        while (num_flush_completed_ < my) {
          bg_cv_->Wait();
        }
      }
    }
  }

  return status;
}

// Insert data into the buffer.
// Return OK on success, or a non-OK status on errors.
// REQUIRES: Finish() has not been called.
template <typename T>
Status DoubleBuffering::Add(const Slice& k, const Slice& v) {
  MutexLock ml(mu_);
  Status status;
  if (finished_)
    status = Status::AssertionFailed("Already finished");
  else {
    status = Prepare<T>(false /* force */, k, v);
    if (status.ok()) {  // Subclass performs the actual data insertion
      THIS->AddToBuffer(mem_buf_, k, v);
    }
  }

  return status;
}

// REQUIRES: mu_ has been locked.
template <typename T>
Status DoubleBuffering::Prepare(bool force, const Slice& k, const Slice& v) {
  mu_->AssertHeld();
  assert(!finished_);
  Status status;
  assert(mem_buf_);
  while (true) {
    if (!bg_status_.ok()) {
      status = bg_status_;
      break;
    } else if (!force && THIS->HasRoom(mem_buf_, k, v)) {
      // There is room in current write buffer
      break;
    } else if (imm_buf_) {
      bg_cv_->Wait();  // Wait for background compaction to finish
    } else {
      // Attempt to switch to a new write buffer
      is_compaction_forced_ = force;
      force = false;
      assert(!imm_buf_);
      imm_buf_ = mem_buf_;
      MaybeScheduleCompaction<T>();
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

// REQUIRES: mu_ has been LOCKED.
template <typename T>
void DoubleBuffering::MaybeScheduleCompaction() {
  mu_->AssertHeld();

  // Do not schedule more if we are in error status
  if (!bg_status_.ok()) {
    return;
  }
  // Skip if there is one already scheduled
  if (has_bg_compaction_) {
    return;
  }
  // Nothing to be scheduled
  if (!imm_buf_) {
    return;
  }

  // Schedule it
  has_bg_compaction_ = true;

  if (THIS->IsEmpty(imm_buf_)) {
    // Buffer is empty so compaction should be quick. As such we directly
    // execute the compaction in the current thread
    DoCompaction<T>();  // No context switch
  } else {
    THIS->ScheduleCompaction();
  }
}

// REQUIRES: mu_ has been LOCKED.
template <typename T>
void DoubleBuffering::DoCompaction() {
  mu_->AssertHeld();
  assert(has_bg_compaction_);
  assert(imm_buf_);
  Status status = THIS->Compact(imm_buf_);
  num_flush_completed_ += is_compaction_forced_;
  is_compaction_forced_ = false;
  assert(bg_status_.ok());
  bg_status_ = status;
  THIS->Clear(imm_buf_);
  imm_buf_ = NULL;
  has_bg_compaction_ = false;
  MaybeScheduleCompaction<T>();
  bg_cv_->SignalAll();
}

#undef THIS

}  // namespace plfsio
}  // namespace pdlfs
