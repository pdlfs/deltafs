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

#include "pdlfs-common/env_files.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"

// If c++11 or newer, directly use c++ std atomic counters.
#if __cplusplus >= 201103L
#include <atomic>
#endif

namespace pdlfs {

SynchronizableFile::~SynchronizableFile() {}

void WritableFileStats::Reset() {
  num_syncs_ = 0;
  num_flushes_ = 0;
  bytes_ = 0;
  ops_ = 0;
}

void SequentialFileStats::Reset() {
  bytes_ = 0;
  ops_ = 0;
}

#if __cplusplus >= 201103L
struct RandomAccessFileStats::Rep {
  Rep() : bytes(0), ops(0) {}
  std::atomic_uint_fast64_t bytes;
  std::atomic_uint_fast64_t ops;

  void AcceptRead(uint64_t n) {
    bytes += n;
    ops += 1;
  }
};
#else
struct RandomAccessFileStats::Rep {
  Rep() : bytes(0), ops(0) {}
  port::Mutex mutex;
  uint64_t bytes;
  uint64_t ops;

  void AcceptRead(uint64_t n) {
    MutexLock ml(&mutex);
    bytes += n;
    ops += 1;
  }
};
#endif

uint64_t RandomAccessFileStats::TotalBytes() const {
  return static_cast<uint64_t>(rep_->bytes);
}

uint64_t RandomAccessFileStats::TotalOps() const {
  return static_cast<uint64_t>(rep_->ops);
}

RandomAccessFileStats::RandomAccessFileStats() { rep_ = new Rep(); }

RandomAccessFileStats::~RandomAccessFileStats() { delete rep_; }

void RandomAccessFileStats::AcceptRead(uint64_t n) { rep_->AcceptRead(n); }

void RandomAccessFileStats::Reset() {
  rep_->bytes = 0;
  rep_->ops = 0;
}

Status WholeFileBufferedRandomAccessFile::Load() {
  Status status;
  assert(base_ != NULL);
  buf_size_ = 0;
  while (buf_size_ < max_buf_size_) {  // Reload until we reach max_buf_size_
    size_t n = io_size_;
    if (n > max_buf_size_ - buf_size_) {
      n = max_buf_size_ - buf_size_;
    }
    Slice sli;
    char* p = buf_ + buf_size_;
    status = base_->Read(n, &sli, p);
    if (!status.ok()) break;  // Error
    if (sli.empty()) break;   // EOF
    if (sli.data() != p) {
      // File implementation gave us pointer to some other data.
      // Explicitly copy it into our buffer.
      memcpy(p, sli.data(), sli.size());
    }
    buf_size_ += sli.size();
  }
  delete base_;
  base_ = NULL;

  return status;
}

}  // namespace pdlfs
