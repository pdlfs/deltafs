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

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */
#pragma once

#include "pdlfs-common/env.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"

#include <sys/mman.h>

namespace pdlfs {

// Helper class to limit mmap file usage so that we do not end up
// running out virtual memory or running into kernel performance
// problems for very large datasets.
class MmapLimiter {
 public:
  MmapLimiter();

  // If another mmap slot is available, acquire it and return true.
  // Else return false.
  bool Acquire() {
    if (GetAllowed() <= 0) {
      return false;
    }
    MutexLock l(&mu_);
    intptr_t x = GetAllowed();
    if (x <= 0) {
      return false;
    } else {
      SetAllowed(x - 1);
      return true;
    }
  }

  // Release a slot acquired by a previous call to
  // Acquire() that returned true.
  void Release() {
    MutexLock l(&mu_);
    SetAllowed(GetAllowed() + 1);
  }

 private:
  MmapLimiter(const MmapLimiter& limiter);
  void operator=(const MmapLimiter&);

  intptr_t GetAllowed() const {
    return reinterpret_cast<intptr_t>(allowed_.Acquire_Load());
  }

  void SetAllowed(intptr_t v) {
    mu_.AssertHeld();
    allowed_.Release_Store(reinterpret_cast<void*>(v));
  }

  port::AtomicPointer allowed_;
  port::Mutex mu_;
};

// A random access file implementation for files that are pre-mapped to memory.
// All read operations return data directly without copying it into
// user-supplied buffers.
class PosixMmapReadableFile : public RandomAccessFile {
 private:
  MmapLimiter* const limiter_;
  std::string filename_;
  void* mmapped_region_;
  size_t length_;

 public:
  PosixMmapReadableFile(const char* fname, void* base, size_t length,
                        MmapLimiter* limiter)
      : limiter_(limiter),
        filename_(fname),
        mmapped_region_(base),
        length_(length) {}

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    if (offset + n > length_) {
      *result = Slice();
      s = Status::InvalidArgument(filename_, "Read beyond eof");
    } else {
      *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
    }
    return s;
  }

  virtual ~PosixMmapReadableFile() {
    munmap(mmapped_region_, length_);
    limiter_->Release();
  }
};

}  // namespace pdlfs
