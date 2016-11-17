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

#include <stdio.h>
#include <string>
#include <vector>

namespace pdlfs {
namespace plfsio {

static const int kMaxNumProcesses = 100000000;  // 100 million

void LogSink::Unref() {
  assert(refs_ > 0);
  refs_--;
  if (refs_ == 0) {
    delete this;
  }
}

LogSink::~LogSink() {
  Status status;
#if 0
  status = file_->Sync();
#endif
  if (status.ok()) {
    file_->Close();
  }
  delete file_;
}

static std::string PartitionIndexFileName(const Slice& dirname, int rank,
                                          int partition) {
  std::string parent = dirname.ToString();
  char tmp[20];
  assert(rank < kMaxNumProcesses);
  snprintf(tmp, sizeof(tmp), "/%08d-%03d.idx", rank, partition);
  return parent + tmp;
}

static std::string DataFileName(const Slice& dirname, int rank) {
  std::string parent = dirname.ToString();
  char tmp[20];
  assert(rank < kMaxNumProcesses);
  snprintf(tmp, sizeof(tmp), "/%08d.dat", rank);
  return parent + tmp;
}

class WriterImpl : public Writer {
 public:
  WriterImpl(const Options& options);
  virtual ~WriterImpl();

  virtual Status Append(const Slice& fname, const Slice& data);
  virtual Status MakeEpoch();
  virtual Status Finish();

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

WriterImpl::WriterImpl(const Options& options)
    : options_(options),
      cond_var_(&mutex_),
      num_parts_(0),
      part_mask_(~static_cast<uint32_t>(0)),
      io_(NULL) {}

WriterImpl::~WriterImpl() {
  MutexLock l(&mutex_);
  for (size_t i = 0; i < num_parts_; i++) {
    delete io_[i];
  }
  delete[] io_;
}

void WriterImpl::MaybeSlowdown() {
  Env* env = options_.env;
  uint64_t micros = options_.slowdown_micros;
  if (micros != 0) {
    env->SleepForMicroseconds(micros);
  }
}

Status WriterImpl::Finish() {
  Status status;
  {
    MutexLock l(&mutex_);

    bool dry_run = true;
    // XXX: Check partition status in a single pass
    while (true) {
      for (size_t i = 0; i < num_parts_; i++) {
        status = io_[i]->Finish(dry_run);
        if (!status.ok()) {
          break;
        }
      }
      if (status.IsBufferFull() && !options_.non_blocking) {
        // XXX: Wait for buffer space
        cond_var_.Wait();
      } else {
        break;
      }
    }

    // XXX: Do it
    if (status.ok()) {
      dry_run = false;
      for (size_t i = 0; i < num_parts_; i++) {
        status = io_[i]->Finish(dry_run);
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

Status WriterImpl::MakeEpoch() {
  Status status;
  {
    MutexLock l(&mutex_);

    bool dry_run = true;
    // XXX: Check partition status in a single pass
    while (true) {
      for (size_t i = 0; i < num_parts_; i++) {
        status = io_[i]->MakeEpoch(dry_run);
        if (!status.ok()) {
          break;
        }
      }
      if (status.IsBufferFull() && !options_.non_blocking) {
        // XXX: Wait for buffer space
        cond_var_.Wait();
      } else {
        break;
      }
    }

    // XXX: Do it
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
  if (part < num_parts_) {
    MutexLock l(&mutex_);
    status = io_[part]->Add(fname, data);
  }
  if (status.IsBufferFull()) {
    MaybeSlowdown();
  }
  return status;
}

Writer::~Writer() {}

static Options SanitizeWriteOptions(const Options& options) {
  Options result = options;
  if (result.env == NULL) result.env = Env::Default();
  if (result.lg_parts < 0) result.lg_parts = 0;
  if (result.lg_parts > 8) result.lg_parts = 8;
  return result;
}

static Status NewLogStream(const std::string& name, Env* env, LogSink** sink) {
  WritableFile* file;
  Status status = env->NewWritableFile(name, &file);
  if (status.ok()) {
    *sink = new LogSink(file);
    (*sink)->Ref();
  } else {
    *sink = NULL;
  }

  return status;
}

Status Writer::Open(const Options& opts, const Slice& name, Writer** ptr) {
  *ptr = NULL;
  Options options = SanitizeWriteOptions(opts);
  size_t num_parts = 1u << options.lg_parts;
  int rank = options.rank;
  Env* env = options.env;
  Status status;

  WriterImpl* impl = new WriterImpl(options);
  std::vector<LogSink*> index(num_parts, NULL);
  std::vector<LogSink*> data(1, NULL);
  status = NewLogStream(DataFileName(name, rank), env, &data[0]);
  for (size_t part = 0; part < num_parts; part++) {
    if (status.ok()) {
      status = NewLogStream(PartitionIndexFileName(name, rank, part), env,
                            &index[part]);
    } else {
      break;
    }
  }

  if (status.ok()) {
    IOLogger** io = new IOLogger*[num_parts];
    for (size_t part = 0; part < num_parts; part++) {
      io[part] = new IOLogger(impl->options_, &impl->mutex_, &impl->cond_var_,
                              data[0], index[part]);
    }
    impl->part_mask_ = num_parts - 1;
    impl->num_parts_ = num_parts;
    impl->io_ = io;
  } else {
    delete impl;
  }

  for (size_t i = 0; i < index.size(); i++) {
    if (index[i] != NULL) {
      index[i]->Unref();
    }
  }
  for (size_t i = 0; i < data.size(); i++) {
    if (data[i] != NULL) {
      data[i]->Unref();
    }
  }

  return status;
}

}  // namespace plfsio
}  // namespace pdlfs
