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

#include "pdlfs-common/env.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/status.h"

#include <assert.h>
#include <deque>
#include <stddef.h>

namespace pdlfs {
namespace plfsio {

class MultiBuffering {
 public:
  MultiBuffering(port::Mutex* mu, port::CondVar* cv);

  // Append data into the buffer specified by membuf. Return OK on success, or a
  // non-OK status on errors. REQUIRES: __Finish() has NOT been called.
  // membuf MUST BE an lvalue pointer, as its value may be replaced by an
  // available bucket
  template <typename T>
  Status __Add(void** membuf, const Slice& k, const Slice& v, bool nowait);

  // Force a compaction, and wait for it to complete if "synchronous" is on.
  // Compactions do not force data to be sync'ed. Sync() does.
  // Return OK on success, or a non-OK status on errors.
  // REQUIRES: __Finish() has NOT been called.
  template <typename T>
  Status __Flush(bool synchronous);

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
  Status PrepareAll(uint32_t* compac_seq, bool force = true,
                    bool nowait = false, const Slice& k = Slice(),
                    const Slice& v = Slice());

  template <typename T>
  Status Prepare(void** membuf, uint32_t* compac_seq, bool force = true,
                 bool nowait = false, const Slice& k = Slice(),
                 const Slice& v = Slice());
  void WaitFor(uint32_t compac_seq);
  void WaitForAny();
  template <typename T>
  void TryScheduleCompaction(uint32_t* compac_seq, void*);
  template <typename T>
  void DoCompaction(uint32_t compac_seq, void*);

  // State below is protected by mu_
  uint32_t num_compac_scheduled_;
  uint32_t num_compac_completed_;
  bool finished_;  // If Finish() has been called
  uint32_t num_bg_compactions_;
  Status bg_status_;
  std::deque<void*> bufs_;
  // Vector below must be initialized by the inheritor
  std::vector<void**> bufs_active_;
};

#define __this static_cast<T*>(this)

// Finalize all writes and sync all remaining data in the write buffer to
// storage. Return OK on success, or a non-OK status on errors.
// REQUIRES: mu_ has been LOCKed.
template <typename T>
Status MultiBuffering::__Finish() {
  mu_->AssertHeld();
  Status finish_status;
  if (finished_)  // __Finish() has already been called.
    return bg_status_;
  else {
    __Flush<T>(true);
  }

  // Wait until !num_bg_compactions_
  WaitForAny();
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
Status MultiBuffering::__Sync(bool flush) {
  mu_->AssertHeld();
  uint32_t my_compac_seq = 0;
  Status status;
  if (finished_)  // __Finish() has already been called
    status = bg_status_;
  else {
    status = PrepareAll<T>(&my_compac_seq, flush);
  }

  if (!status.ok()) {
    return status;
  } else {
    // If compaction is scheduled, wait for it until num_compac_completed_
    // >= my_compac_seq, otherwise my_compac_seq is 0 and
    // WaitFor(seq) will return immediately.
    WaitFor(my_compac_seq);
    // Then, wait until !num_bg_compactions_
    WaitForAny();
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
Status MultiBuffering::__Flush(bool synchronous) {
  mu_->AssertHeld();
  uint32_t my_compac_seq = 0;
  Status status;
  if (finished_)  // __Finish() has already been called
    status = bg_status_;
  else {
    status = PrepareAll<T>(&my_compac_seq);
  }

  if (status.ok() && synchronous) {  // Wait for the compaction to complete
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
Status MultiBuffering::__Add(void** membuf, const Slice& k, const Slice& v,
                             bool nowait) {
  mu_->AssertHeld();
  uint32_t ignored_compac_seq;
  Status status;
  if (finished_)  // __Finish() has already been called
    status = bg_status_;
  else {
    status = Prepare<T>(membuf, &ignored_compac_seq, false /* !force */, nowait,
                        k, v);
    if (status.ok()) {
      __this->AddToBuffer(membuf, k, v);
    }
  }

  return status;
}
// REQUIRES: mu_ has been LOCKed.
template <typename T>
Status MultiBuffering::PrepareAll(uint32_t* seq, bool force, bool nowait,
                                  const Slice& k, const Slice& v) {
  mu_->AssertHeld();
  Status s = Status::OK();
  for (size_t i = 0; i < bufs_active_.size(); i++) {
    s = Prepare<T>(bufs_active_[i], seq, force, nowait, k, v);
    if (!s.ok()) break;
  }
  return s;
}

// REQUIRES: mu_ has been LOCKed.
template <typename T>
Status MultiBuffering::Prepare(void** membuf, uint32_t* seq, bool force,
                               bool nowait, const Slice& k, const Slice& v) {
  mu_->AssertHeld();
  Status status;
  assert(membuf);
  while (true) {
    assert(*membuf);
    if (!bg_status_.ok()) {
      status = bg_status_;
      break;
    } else if (!force && __this->HasRoom(*membuf, k, v)) {
      // There is room in current write buffer
      break;
    } else if (!bufs_.empty()) {
      // Attempt to switch to a new write buffer
      force = false;
      TryScheduleCompaction<T>(seq, *membuf);
      *membuf = bufs_.back();
      bufs_.pop_back();
    } else if (!nowait) {
      bg_cv_->Wait();  // Wait for background compactions to finish
    } else {
      status = Status::TryAgain("");
      break;
    }
  }

  return status;
}

// REQUIRES: mu_ has been LOCKed.
template <typename T>
void MultiBuffering::TryScheduleCompaction(uint32_t* compac_seq, void* immbuf) {
  mu_->AssertHeld();

  *compac_seq = ++num_compac_scheduled_;
  ++num_bg_compactions_;

  if (__this->IsEmpty(immbuf) && *compac_seq == num_compac_completed_ + 1) {
    // Buffer is empty so compaction should be quick. As such we directly
    // execute the compaction in the current thread
    DoCompaction<T>(*compac_seq, immbuf);  // Avoid context switches
  } else {
    __this->ScheduleCompaction(*compac_seq, immbuf);
  }
}

// REQUIRES: mu_ has been LOCKed.
template <typename T>
void MultiBuffering::DoCompaction(uint32_t seq, void* immbuf) {
  mu_->AssertHeld();
  assert(immbuf);
  Status status = __this->Compact(seq, immbuf);
  ++num_compac_completed_;
  assert(bg_status_.ok());
  bg_status_ = status;
  __this->Clear(immbuf);
  bufs_.push_back(immbuf);
  assert(num_bg_compactions_ > 0);
  --num_bg_compactions_;
#if 0
  // Compaction done. New buffer space available.
  // Try scheduling another.
  uint32_t ignored_compac_seq;
  Prepare<T>(&ignored_compac_seq, false /* !force */);
#endif
  bg_cv_->SignalAll();
}

#undef __this

}  // namespace plfsio
}  // namespace pdlfs
