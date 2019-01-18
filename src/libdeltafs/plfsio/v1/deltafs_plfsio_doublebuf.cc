/*
 * Copyright (c) 2015-2019 Carnegie Mellon University and
 *         Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_doublebuf.h"

#include "pdlfs-common/mutexlock.h"

#include <assert.h>
#include <stddef.h>

namespace pdlfs {
namespace plfsio {

DoubleBuffering::DoubleBuffering(port::Mutex* mu, port::CondVar* cv, void* buf0,
                                 void* buf1)
    : mu_(mu),
      bg_cv_(cv),
      num_flush_requested_(0),
      num_flush_completed_(0),
      finished_(false),
      is_compaction_forced_(false),
      has_bg_compaction_(false),
      mem_buf_(NULL),
      imm_buf_(NULL),
      buf0_(buf0),
      buf1_(buf1) {}

// Wait until compaction is done if there is one scheduled. Data remaining in
// the buffer will not be flushed and will be lost.
DoubleBuffering::~DoubleBuffering() {
  MutexLock ml(mu_);
  while (has_bg_compaction_) {
    bg_cv_->Wait();
  }
}

// Finalize all writes and sync all remaining data to storage.
// Return OK on success, or a non-OK status on errors.
Status DoubleBuffering::Finish() {
  MutexLock ml(mu_);
  if (finished_)
    return bg_status_;
  else {
    Status status = Prepare();
    if (status.ok()) {
      WaitForCompaction();
    }
  }

  bg_status_ = SyncBackend(true /* close */);
  finished_ = true;
  return bg_status_;
}

// Sync data so data hits storage, but will wait until all outstanding
// compactions are completed before performing the sync operation.
// Return OK on success, or a non-OK status on errors.
// REQUIRES: Finish() has not been called.
Status DoubleBuffering::Sync(bool force_flush) {
  MutexLock ml(mu_);
  if (finished_)
    return Status::AssertionFailed("Already finished");
  else {
    Status status = Prepare(force_flush);
    if (status.ok()) {
      WaitForCompaction();
    }
  }

  bg_status_ = SyncBackend();
  return bg_status_;
}

// Wait until all outstanding compactions to clear. INVARIANT: no
// compaction has been scheduled at the moment this function returns.
// Return OK on success, or a non-OK status on errors.
Status DoubleBuffering::Wait() {
  MutexLock ml(mu_);
  if (finished_)
    return Status::AssertionFailed("Already finished");
  else {
    WaitForCompaction();
  }

  return bg_status_;
}

// Wait for one or more outstanding compactions to clear.
// REQUIRES: Finish() has not been called.
// REQUIRES: mu_ has been locked.
void DoubleBuffering::WaitForCompaction() {
  mu_->AssertHeld();
  assert(!finished_);  // Finish() has not been called
  while (bg_status_.ok() && has_bg_compaction_) {
    bg_cv_->Wait();
  }
}

// Force a buffer flush.
// REQUIRES: Finish() has not been called.
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
    status = Prepare();
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
Status DoubleBuffering::Add(const Slice& k, const Slice& v) {
  MutexLock ml(mu_);
  Status status;
  if (finished_)
    status = Status::AssertionFailed("Already finished");
  else {
    status = Prepare(false /* force */, k, v);
    if (status.ok()) {  // Subclass performs the actual data insertion
      AddToBuffer(mem_buf_, k, v);
    }
  }

  return status;
}

// REQUIRES: mu_ has been locked.
Status DoubleBuffering::Prepare(bool force, const Slice& k, const Slice& v) {
  mu_->AssertHeld();
  assert(!finished_);
  Status status;
  assert(mem_buf_);
  while (true) {
    if (!bg_status_.ok()) {
      status = bg_status_;
      break;
    } else if (!force && HasRoom(mem_buf_, k, v)) {
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
      MaybeScheduleCompaction();
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

  if (IsEmpty(imm_buf_)) {
    // Buffer is empty so compaction should be quick. As such we directly
    // execute the compaction in the current thread
    DoCompaction();  // No context switch
  } else {
    ScheduleCompaction();
  }
}

// REQUIRES: mu_ has been LOCKED.
void DoubleBuffering::DoCompaction() {
  mu_->AssertHeld();
  assert(has_bg_compaction_);
  assert(imm_buf_);
  Status status = Compact(imm_buf_);
  num_flush_completed_ += is_compaction_forced_;
  is_compaction_forced_ = false;
  assert(bg_status_.ok());
  bg_status_ = status;
  Clear(imm_buf_);
  imm_buf_ = NULL;
  has_bg_compaction_ = false;
  MaybeScheduleCompaction();
  bg_cv_->SignalAll();
}

}  // namespace plfsio
}  // namespace pdlfs
