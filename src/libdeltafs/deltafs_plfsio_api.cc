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
  WriterImpl(const Options& options, IOLogger** io);
  virtual ~WriterImpl();

  virtual Status Append(const Slice& fname, const Slice& data);
  virtual Status MakeEpoch();

 private:
  void MaybeSlowdown();

  friend class Writer;
  const Options options_;
  port::Mutex mutex_;
  port::CondVar cond_var_;
  size_t num_parts_;
  uint32_t part_mask_;
  IOLogger** io_;
};

WriterImpl::WriterImpl(const Options& options, IOLogger** io)
    : options_(options),
      cond_var_(&mutex_),
      num_parts_(1u << options.lg_parts),
      part_mask_(num_parts_ - 1),
      io_(io) {}

WriterImpl::~WriterImpl() {
  mutex_.Lock();
  for (size_t i = 0; i < num_parts_; i++) {
    delete io_[i];
  }
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

    bool dry_run = true;
    while (true) {
      // XXX: Check partition status in a single pass
      for (size_t i = 0; i < num_parts_; i++) {
        status = io_[i]->MakeEpoch(dry_run);
        if (!status.ok()) {
          break;
        }
      }
      if (status.IsBufferFull() && !options_.non_blocking) {
        cond_var_.Wait();
      } else {
        break;
      }
    }

    if (status.ok()) {
      dry_run = false;
      for (size_t i = 0; i < num_parts_; i++) {
        status = io_[i]->MakeEpoch(dry_run);
        if (!status.ok()) {
          break;
        }
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
