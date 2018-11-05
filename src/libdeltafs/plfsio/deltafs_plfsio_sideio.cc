/*
 * Copyright (c) 2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_sideio.h"
#include "deltafs_plfsio.h"

#include "pdlfs-common/mutexlock.h"

#include <assert.h>

namespace pdlfs {
namespace plfsio {

DirectWriter::DirectWriter(const DirOptions& options, WritableFile* dst,
                           size_t buf_size)
    : options_(options),
      dst_(dst),  // Not owned by us
      bg_cv_(&mu_),
      buf_threshold_(buf_size),
      buf_reserv_(buf_size),
      num_flush_requested_(0),
      num_flush_completed_(0),
      finished_(false),
      is_compaction_forced_(false),
      has_bg_compaction_(false),
      mem_buf_(NULL),
      imm_buf_(NULL) {
  buf0_.reserve(buf_reserv_);
  buf1_.reserve(buf_reserv_);

  mem_buf_ = &buf0_;
}

// Wait until compaction is done if there's one scheduled.
// Won't flush memory or schedule new compactions.
DirectWriter::~DirectWriter() {
  MutexLock ml(&mu_);
  while (has_bg_compaction_) {
    bg_cv_.Wait();
  }
}

// Insert data into the directory.
// Return OK on success, or a non-OK status on errors.
// REQUIRES: Finish() has not been called.
Status DirectWriter::Append(const Slice& slice) {
  MutexLock ml(&mu_);
  Status status;
  if (finished_)
    status = Status::AssertionFailed("Already finished");
  else
    status = Prepare(slice);
  if (status.ok()) {
    mem_buf_->append(slice.data(), slice.size());
  }
  return status;
}

// Finalize the writes and sync data to storage.
// Return OK on success, or a non-OK status on errors.
Status DirectWriter::Finish() {
  MutexLock ml(&mu_);
  if (finished_) return bg_status_;
  if (bg_status_.ok()) Prepare(Slice(), true /* force */);
  if (bg_status_.ok()) WaitForCompaction();
  if (bg_status_.ok()) bg_status_ = dst_->Sync();
  if (bg_status_.ok()) dst_->Close();

  finished_ = true;
  return bg_status_;
}

// Sync data so data is persisted to storage. Will wait until all on-going
// compactions are completed before performing the sync operation.
// Return OK on success, or a non-OK status on errors.
Status DirectWriter::Sync(const SyncOptions& sync_options) {
  MutexLock ml(&mu_);
  if (finished_) return bg_status_;
  if (sync_options.do_flush && bg_status_.ok())
    Prepare(Slice(), true /* force */);
  if (bg_status_.ok()) WaitForCompaction();
  if (bg_status_.ok()) bg_status_ = dst_->Sync();
  return bg_status_;
}

// Wait until compaction is done.
// Return OK on success, or a non-OK status on errors.
// INVARIANT: no compaction has been scheduled at the moment this function
// returns.
Status DirectWriter::Wait() {
  MutexLock ml(&mu_);
  if (finished_) return bg_status_;
  if (bg_status_.ok()) WaitForCompaction();
  return bg_status_;
}

// Wait for one or more on-going compactions to complete.
// REQUIRES: mu_ has been locked.
void DirectWriter::WaitForCompaction() {
  mu_.AssertHeld();
  assert(!finished_);  // Finish() has not been called
  while (bg_status_.ok() && has_bg_compaction_) {
    bg_cv_.Wait();
  }
}

// Force a compaction.
// REQUIRES: Finish() has not been called.
Status DirectWriter::Flush(const FlushOptions& flush_options) {
  MutexLock ml(&mu_);
  if (finished_)
    return Status::AssertionFailed("Already finished");
  else {
    // Wait for buffer space
    while (imm_buf_ != NULL) {
      bg_cv_.Wait();
    }
  }

  Status status;
  if (!bg_status_.ok()) {
    status = bg_status_;
  } else {
    num_flush_requested_++;
    const uint32_t my = num_flush_requested_;
    status = Prepare(Slice(), true /* force */);
    if (status.ok()) {
      if (flush_options.wait) {
        while (num_flush_completed_ < my) {
          bg_cv_.Wait();
        }
      }
    }
  }

  return status;
}

// REQUIRES: mu_ has been locked.
Status DirectWriter::Prepare(const Slice& data, bool force) {
  mu_.AssertHeld();
  assert(!finished_);  // Finish() has not been called
  Status status;
  assert(mem_buf_ != NULL);
  while (true) {
    if (!bg_status_.ok()) {
      status = bg_status_;
      break;
    } else if (!force && mem_buf_->size() + data.size() < buf_threshold_) {
      // There is room in current write buffer
      break;
    } else if (imm_buf_ != NULL) {
      bg_cv_.Wait();  // Wait for background compactions to finish
    } else {
      // Attempt to switch to a new write buffer
      assert(imm_buf_ == NULL);
      is_compaction_forced_ = force;
      force = false;
      imm_buf_ = mem_buf_;
      MaybeScheduleCompaction();
      std::string* const current_buf = mem_buf_;
      if (current_buf == &buf0_) {
        mem_buf_ = &buf1_;
      } else {
        mem_buf_ = &buf0_;
      }
    }
  }

  return status;
}

// REQUIRES: mu_ has been locked.
void DirectWriter::MaybeScheduleCompaction() {
  mu_.AssertHeld();

  // Do not schedule more if we are in error status
  if (!bg_status_.ok()) {
    return;
  }
  // Skip if there is one already scheduled
  if (has_bg_compaction_) {
    return;
  }
  // Nothing to be scheduled
  if (imm_buf_ == NULL) {
    return;
  }

  // Schedule it
  has_bg_compaction_ = true;

  if (imm_buf_->empty()) {
    // Buffer is empty so compaction should be quick. As such we directly
    // execute the compaction in the current thread
    DoCompaction();  // No context switch
  } else if (options_.compaction_pool != NULL) {
    options_.compaction_pool->Schedule(DirectWriter::BGWork, this);
  } else if (options_.allow_env_threads) {
    Env::Default()->Schedule(DirectWriter::BGWork, this);
  } else {
    DoCompaction();
  }
}

void DirectWriter::BGWork(void* arg) {
  DirectWriter* const ins = reinterpret_cast<DirectWriter*>(arg);
  MutexLock ml(&ins->mu_);
  ins->DoCompaction();
}

// REQUIRES: mu_ has been locked.
void DirectWriter::DoCompaction() {
  mu_.AssertHeld();
  assert(has_bg_compaction_);
  assert(imm_buf_ != NULL);
  assert(dst_ != NULL);
  mu_.Unlock();  // Unlock during I/O operations
  Status status = dst_->Append(*imm_buf_);
  // Compaction does not sync data to storage. Sync() does.
  if (status.ok()) {
    status = dst_->Flush();
  }
  mu_.Lock();
  assert(bg_status_.ok());
  bg_status_ = status;
  imm_buf_->resize(0);
  imm_buf_ = NULL;
  has_bg_compaction_ = false;
  num_flush_completed_ += is_compaction_forced_;
  is_compaction_forced_ = false;
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
}

DirectReader::DirectReader(const DirOptions& options, RandomAccessFile* src)
    : options_(options), src_(src) {  // src_ is not owned by us
}
// Directly read data from the source.
Status DirectReader::Read(uint64_t off, size_t n, Slice* result,
                          char* scratch) const {
  return src_->Read(off, n, result, scratch);
}

}  // namespace plfsio
}  // namespace pdlfs
