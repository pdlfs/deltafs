/*
 * Copyright (c) 2015-2019 Carnegie Mellon University and
 *         Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_bulkio.h"
#include "deltafs_plfsio_types.h"

namespace pdlfs {
namespace plfsio {

DirectWriter::DirectWriter(const DirOptions& options, WritableFile* dst,
                           size_t buf_size)
    : DoubleBuffering(&mu_, &bg_cv_, &str0_, &str1_),
      options_(options),
      dst_(dst),  // Not owned by us
      bg_cv_(&mu_),
      buf_threshold_(buf_size),
      buf_reserv_(buf_size) {
  str0_.reserve(buf_reserv_);
  str1_.reserve(buf_reserv_);

  mem_buf_ = &str0_;
}

// Wait for all outstanding compactions to clear.
DirectWriter::~DirectWriter() {
  MutexLock ml(&mu_);
  while (has_bg_compaction_) {
    bg_cv_.Wait();
  }
}

// REQUIRES: mu_ has been LOCKED.
Status DirectWriter::Compact(void* buf) {
  mu_.AssertHeld();
  assert(dst_);
  std::string* const s = static_cast<std::string*>(buf);
  // Skip empty buffers
  if (s->empty()) return Status::OK();
  mu_.Unlock();  // Unlock during I/O operations
  Status status = dst_->Append(*s);
  // Does not sync data to storage.
  // Sync() does.
  if (status.ok()) {
    status = dst_->Flush();
  }
  mu_.Lock();
  return status;
}

// REQUIRES: mu_ has been LOCKED.
Status DirectWriter::SyncBackend(bool close) {
  mu_.AssertHeld();
  assert(dst_);
  Status status = dst_->Sync();
  if (close) {
    dst_->Close();
  }
  return status;
}

// REQUIRES: mu_ has been LOCKED.
void DirectWriter::ScheduleCompaction() {
  mu_.AssertHeld();

  assert(has_bg_compaction_);

  if (options_.compaction_pool) {
    options_.compaction_pool->Schedule(DirectWriter::BGWork, this);
  } else if (options_.allow_env_threads) {
    Env::Default()->Schedule(DirectWriter::BGWork, this);
  } else {
    DoCompaction<DirectWriter>();
  }
}

void DirectWriter::BGWork(void* arg) {
  DirectWriter* const ins = reinterpret_cast<DirectWriter*>(arg);
  MutexLock ml(&ins->mu_);
  ins->DoCompaction<DirectWriter>();
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
