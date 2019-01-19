/*
 * Copyright (c) 2015-2019 Carnegie Mellon University and
 *         Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_pdb.h"

namespace pdlfs {
namespace plfsio {

BufferedBlockWriter::BufferedBlockWriter(const DirOptions& options,
                                         WritableFile* dst, size_t buf_size)
    : DoubleBuffering(&mu_, &bg_cv_, &bb0_, &bb1_),
      options_(options),
      dst_(dst),  // Not owned by us
      bg_cv_(&mu_),
      buf_threshold_(buf_size),
      buf_reserv_(buf_size),
      bb0_(options),
      bb1_(options) {
  bb0_.Reserve(buf_reserv_);
  bb1_.Reserve(buf_reserv_);

  mem_buf_ = &bb0_;
}

// Wait for all outstanding compactions to clear.
BufferedBlockWriter::~BufferedBlockWriter() {
  MutexLock ml(&mu_);
  while (has_bg_compaction_) {
    bg_cv_.Wait();
  }
}

// Insert data into the writer.
// REQUIRES: Finish() has NOT been called.
Status BufferedBlockWriter::Add(const Slice& k, const Slice& v) {
  MutexLock ml(&mu_);
  return __Add<BufferedBlockWriter>(k, v);
}

// Force a compaction but do not wait for the compaction to clear.
// REQUIRES: Finish() has NOT been called.
Status BufferedBlockWriter::Flush() {
  MutexLock ml(&mu_);
  return __Flush<BufferedBlockWriter>(false);
}

// Sync data to storage. Data still buffered in memory is not sync'ed.
// REQUIRES: Finish() has NOT been called.
Status BufferedBlockWriter::Sync() {
  MutexLock ml(&mu_);
  return __Sync<BufferedBlockWriter>(false);
}

// Wait until there is no outstanding compactions.
// REQUIRES: Finish() has NOT been called.
Status BufferedBlockWriter::Wait() {
  MutexLock ml(&mu_);  // Wait until !has_bg_compaction_
  return __Wait();
}

// Finalize the writer. Expected to be called ONLY once.
Status BufferedBlockWriter::Finish() {
  MutexLock ml(&mu_);
  return __Finish<BufferedBlockWriter>();
}

// REQUIRES: mu_ has been LOCKED.
Status BufferedBlockWriter::Compact(void* buf) {
  mu_.AssertHeld();
  assert(dst_);
  BlockBuf* const bb = static_cast<BlockBuf*>(buf);
  // Skip empty buffers
  if (bb->empty()) return Status::OK();
  mu_.Unlock();  // Unlock during I/O operations
  Status status = dst_->Append(bb->Finish());
  // Does not sync data to storage.
  // Sync() does.
  if (status.ok()) {
    status = dst_->Flush();
  }
  mu_.Lock();
  return status;
}

// REQUIRES: mu_ has been LOCKED.
Status BufferedBlockWriter::SyncBackend(bool close) {
  mu_.AssertHeld();
  assert(dst_);
  Status status = dst_->Sync();
  if (close) {
    dst_->Close();
  }
  return status;
}

// REQUIRES: mu_ has been LOCKED.
void BufferedBlockWriter::ScheduleCompaction() {
  mu_.AssertHeld();

  assert(has_bg_compaction_);

  if (options_.compaction_pool) {
    options_.compaction_pool->Schedule(BufferedBlockWriter::BGWork, this);
  } else if (options_.allow_env_threads) {
    Env::Default()->Schedule(BufferedBlockWriter::BGWork, this);
  } else {
    DoCompaction<BufferedBlockWriter>();
  }
}

void BufferedBlockWriter::BGWork(void* arg) {
  BufferedBlockWriter* const ins = reinterpret_cast<BufferedBlockWriter*>(arg);
  MutexLock ml(&ins->mu_);
  ins->DoCompaction<BufferedBlockWriter>();
}

}  // namespace plfsio
}  // namespace pdlfs
