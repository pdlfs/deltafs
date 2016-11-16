/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio.h"
#include "deltafs_plfsio_api.h"

#include "pdlfs-common/hash.h"
#include "pdlfs-common/mutexlock.h"

namespace pdlfs {
namespace plfsio {

class WriterImpl : public Writer {
 public:
  WriterImpl(int lg_parts, IOLogger** io);
  virtual ~WriterImpl();

  virtual Status Append(const Slice& fname, const Slice& data);
  virtual Status MakeEpoch();

 private:
  void MaybeSlowdown();

  friend class Writer;
  Options options_;
  port::Mutex mutex_;
  size_t num_parts_;
  uint32_t part_mask_;
  IOLogger** io_;
};

WriterImpl::WriterImpl(int lg_parts, IOLogger** io)
    : num_parts_(1u << lg_parts), part_mask_(num_parts_ - 1), io_(io) {}

WriterImpl::~WriterImpl() {
  mutex_.Lock();
  // XXX: TODO
  mutex_.Unlock();
}

void WriterImpl::MaybeSlowdown() {
  Env* env = options_.env;
  uint64_t micros = options_.slowdown_micros;
  if (micros != 0) {
    env->SleepForMicroseconds(micros);
  }
}

Status WriterImpl::MakeEpoch() {
  Status status;
  {
    MutexLock l(&mutex_);
    for (size_t i = 0; i < num_parts_; i++) {
      status = io_[i]->MakeEpoch(false);
      if (!status.ok()) {
        break;
      }
    }
  }
  if (status.IsBufferFull()) {
    MaybeSlowdown();
  }
  return status;
}

Status WriterImpl::Append(const Slice& fname, const Slice& data) {
  Status status;
  uint32_t hash = Hash(fname.data(), fname.size(), 0);
  uint32_t part = hash & part_mask_;
  {
    MutexLock l(&mutex_);
    status = io_[part]->Add(fname, data);
  }
  if (status.IsBufferFull()) {
    MaybeSlowdown();
  }
  return status;
}

Writer::~Writer() {}

}  // namespace plfsio
}  // namespace pdlfs
