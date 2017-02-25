/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio.h"
#include "deltafs_plfsio_internal.h"

#include "pdlfs-common/buffered_file.h"
#include "pdlfs-common/hash.h"
#include "pdlfs-common/logging.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/strutil.h"

#include <stdio.h>
#include <string>
#include <vector>

namespace pdlfs {
namespace plfsio {

DirStats::DirStats() : data_size(0), index_size(0), write_micros(0) {}

DirOptions::DirOptions()
    : memtable_size(32 << 20),
      key_size(8),
      value_size(32),
      bf_bits_per_key(8),
      block_size(128 << 10),
      block_util(0.999),
      data_buffer(2 << 20),
      index_buffer(2 << 20),
      compaction_pool(NULL),
      non_blocking(false),
      slowdown_micros(0),
      unique_keys(true),
      verify_checksums(false),
      lg_parts(0),
      rank(0),
      env(NULL) {}

DirOptions ParseDirOptions(const char* input) {
  DirOptions options;
  std::vector<std::string> conf_segments;
  size_t n = SplitString(&conf_segments, input, '&');  // k1=v1 & k2=v2 & k3=v3
  std::vector<std::string> conf_pair;
  for (size_t i = 0; i < n; i++) {
    conf_pair.resize(0);
    SplitString(&conf_pair, conf_segments[i].c_str(), '=', 1);
    if (conf_pair.size() != 2) {
      continue;
    }
    Slice conf_key = conf_pair[0];
    Slice conf_value = conf_pair[1];
    uint64_t num;
    bool flag;
    if (conf_key == "lg_parts") {
      if (ConsumeDecimalNumber(&conf_value, &num)) {
        options.lg_parts = int(num);
      }
    } else if (conf_key == "rank") {
      if (ConsumeDecimalNumber(&conf_value, &num)) {
        options.rank = int(num);
      }
    } else if (conf_key == "memtable_size") {
      if (ParsePrettyNumber(conf_value, &num)) {
        options.memtable_size = num;
      }
    } else if (conf_key == "data_buffer") {
      if (ParsePrettyNumber(conf_value, &num)) {
        options.data_buffer = num;
      }
    } else if (conf_key == "index_buffer") {
      if (ParsePrettyNumber(conf_value, &num)) {
        options.index_buffer = num;
      }
    } else if (conf_key == "verify_checksums") {
      if (ParsePrettyBool(conf_value, &flag)) {
        options.verify_checksums = flag;
      }
    } else if (conf_key == "unique_keys") {
      if (ParsePrettyBool(conf_value, &flag)) {
        options.unique_keys = flag;
      }
    } else if (conf_key == "filter_bits_per_key") {
      if (ParsePrettyNumber(conf_value, &num)) {
        options.bf_bits_per_key = num;
      }
    } else if (conf_key == "value_size") {
      if (ParsePrettyNumber(conf_value, &num)) {
        options.value_size = num;
      }
    } else if (conf_key == "key_size") {
      if (ParsePrettyNumber(conf_value, &num)) {
        options.key_size = num;
      }
    }
  }

  return options;
}

static const int kMaxNumProcesses = 100000000;  // 100 million

void LogSink::Unref() {
  assert(refs_ > 0);
  refs_--;
  if (refs_ == 0) {
    delete this;
  }
}

LogSink::~LogSink() {
  if (file_ != NULL) {
    if (kSyncBeforeClosing) {
      Status s = file_->Sync();
      if (!s.ok()) {
        Error(__LOG_ARGS__, "%s", s.ToString().c_str());
      }
    }
    file_->Close();
    delete file_;
  }
}

void LogSource::Unref() {
  assert(refs_ > 0);
  refs_--;
  if (refs_ == 0) {
    delete this;
  }
}

LogSource::~LogSource() {
  if (file_ != NULL) {
    delete file_;
  }
}

static std::string PartitionIndexFileName(const std::string& parent, int rank,
                                          int partition) {
  char tmp[20];
  assert(rank < kMaxNumProcesses);
  snprintf(tmp, sizeof(tmp), "/r%08d-p%03d.idx", rank, partition);
  return parent + tmp;
}

static std::string DataFileName(const std::string& parent, int rank) {
  char tmp[20];
  assert(rank < kMaxNumProcesses);
  snprintf(tmp, sizeof(tmp), "/r%08d.dat", rank);
  return parent + tmp;
}

class WriterImpl : public Writer {
 public:
  WriterImpl(const DirOptions& options);
  virtual ~WriterImpl();

  virtual const DirStats* stats() const { return &stats_; }
  virtual Status Append(const Slice& fname, const Slice& data);
  virtual Status Sync();
  virtual Status MakeEpoch();
  virtual Status Finish();

 private:
  void MaybeSlowdownCaller();
  friend class Writer;

  DirStats stats_;
  const DirOptions options_;
  port::Mutex mutex_;
  port::CondVar cond_var_;
  size_t num_parts_;
  uint32_t part_mask_;
  PlfsIoLogger** io_;
};

WriterImpl::WriterImpl(const DirOptions& options)
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

void WriterImpl::MaybeSlowdownCaller() {
  Env* const env = options_.env;
  const uint64_t micros = options_.slowdown_micros;
  if (micros != 0) {
    env->SleepForMicroseconds(micros);
  }
}

Status WriterImpl::Finish() {
  Status status;
  {
    MutexLock ml(&mutex_);

    bool dry_run = true;
    // Check partition status in a single pass
    while (true) {
      for (size_t i = 0; i < num_parts_; i++) {
        status = io_[i]->Finish(dry_run);
        if (!status.ok()) {
          break;
        }
      }
      if (status.IsBufferFull() && !options_.non_blocking) {
        // Wait for buffer space
        cond_var_.Wait();
      } else {
        break;
      }
    }

    // Do it
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
    MaybeSlowdownCaller();
  }
  return status;
}

Status WriterImpl::MakeEpoch() {
  Status status;
  {
    MutexLock ml(&mutex_);

    bool dry_run = true;
    // Check partition status in a single pass
    while (true) {
      for (size_t i = 0; i < num_parts_; i++) {
        status = io_[i]->MakeEpoch(dry_run);
        if (!status.ok()) {
          break;
        }
      }
      if (status.IsBufferFull() && !options_.non_blocking) {
        // Wait for buffer space
        cond_var_.Wait();
      } else {
        break;
      }
    }

    // Do it
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
    MaybeSlowdownCaller();
  }
  return status;
}

Status WriterImpl::Append(const Slice& fname, const Slice& data) {
  Status status;
  uint32_t hash = Hash(fname.data(), fname.size(), 0);
  uint32_t part = hash & part_mask_;
  if (part < num_parts_) {
    MutexLock ml(&mutex_);
    status = io_[part]->Add(fname, data);
  }
  if (status.IsBufferFull()) {
    MaybeSlowdownCaller();
  }
  return status;
}

Status WriterImpl::Sync() {
  // TODO
  return Status::NotSupported(Slice());
}

Writer::~Writer() {}

static DirOptions SanitizeWriteOptions(const DirOptions& options) {
  DirOptions result = options;
  if (result.env == NULL) result.env = Env::Default();
  if (result.lg_parts < 0) result.lg_parts = 0;
  if (result.lg_parts > 8) result.lg_parts = 8;
  return result;
}

static void PrintLogStream(const std::string& name, size_t buf_size) {
#if VERBOSE >= 3
  Verbose(__LOG_ARGS__, 3, "Open plfs io log: %s (buffer=%s)", name.c_str(),
          PrettySize(buf_size).c_str());
#endif
}

static Status NewLogSink(const std::string& name, Env* env, size_t buf_size,
                         LogSink** ptr) {
  WritableFile* file;
  Status status = env->NewWritableFile(name, &file);
  if (status.ok()) {
    UnsafeBufferedWritableFile* buffered_file =
        new UnsafeBufferedWritableFile(file, buf_size);
    PrintLogStream(name, buf_size);
    LogSink* sink = new LogSink(buffered_file);
    sink->Ref();
    *ptr = sink;
  } else {
    *ptr = NULL;
  }

  return status;
}

Status Writer::Open(const DirOptions& opts, const std::string& name,
                    Writer** ptr) {
  *ptr = NULL;
  DirOptions options = SanitizeWriteOptions(opts);
  const size_t num_parts = 1u << options.lg_parts;
  const int my_rank = options.rank;
  Env* const env = options.env;
#if VERBOSE >= 2
  Verbose(__LOG_ARGS__, 2, "plfsdir.name -> %s", name.c_str());
  Verbose(__LOG_ARGS__, 2, "plfsdir.memtable_size -> %s",
          PrettySize(options.memtable_size).c_str());
  Verbose(__LOG_ARGS__, 2, "plfsdir.key_size -> %s",
          PrettySize(options.key_size).c_str());
  Verbose(__LOG_ARGS__, 2, "plfsdir.value_size -> %s",
          PrettySize(options.value_size).c_str());
  Verbose(__LOG_ARGS__, 2, "plfsdir.bf_bits_per_key -> %d",
          int(options.bf_bits_per_key));
  Verbose(__LOG_ARGS__, 2, "plfsdir.block_size -> %s",
          PrettySize(options.block_size).c_str());
  Verbose(__LOG_ARGS__, 2, "plfsdir.block_util -> %.2f%%",
          100 * options.block_util);
  Verbose(__LOG_ARGS__, 2, "plfsdir.data_buffer -> %s",
          PrettySize(options.data_buffer).c_str());
  Verbose(__LOG_ARGS__, 2, "plfsdir.index_buffer -> %s",
          PrettySize(options.index_buffer).c_str());
  Verbose(__LOG_ARGS__, 2, "plfsdir.unique_keys -> %d",
          int(options.unique_keys));
  Verbose(__LOG_ARGS__, 2, "plfsdir.num_parts_per_rank -> %u",
          static_cast<unsigned>(num_parts));
  Verbose(__LOG_ARGS__, 2, "plfsdir.my_rank -> %d", my_rank);
#endif
  Status status;
  // Ignore error since it may already exist
  env->CreateDir(name);

  WriterImpl* impl = new WriterImpl(options);
  std::vector<LogSink*> index(num_parts, NULL);
  std::vector<LogSink*> data(1, NULL);  // Shared among all partitions
  status = NewLogSink(DataFileName(name, my_rank), env, options.data_buffer,
                      &data[0]);
  for (size_t part = 0; part < num_parts; part++) {
    if (status.ok()) {
      status = NewLogSink(PartitionIndexFileName(name, my_rank, part), env,
                          options.index_buffer, &index[part]);
    } else {
      break;
    }
  }

  if (status.ok()) {
    PlfsIoLogger** io = new PlfsIoLogger*[num_parts];
    for (size_t i = 0; i < num_parts; i++) {
      io[i] = new PlfsIoLogger(impl->options_, &impl->mutex_, &impl->cond_var_,
                               data[0], index[i], &impl->stats_);
    }
    impl->part_mask_ = num_parts - 1;
    impl->num_parts_ = num_parts;
    impl->io_ = io;
    *ptr = impl;
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

class ReaderImpl : public Reader {
 public:
  ReaderImpl(const DirOptions& options, const std::string& dirname,
             LogSource* data);
  virtual ~ReaderImpl();

  virtual void List(std::vector<std::string>* names);
  virtual Status ReadAll(const Slice& fname, std::string* dst);
  virtual bool Exists(const Slice& fname);

 private:
  friend class Reader;

  const DirOptions options_;
  const std::string dirname_;
  port::Mutex mutex_;
  size_t num_parts_;
  uint32_t part_mask_;
  LogSource* data_;
};

ReaderImpl::ReaderImpl(const DirOptions& options, const std::string& dirname,
                       LogSource* data)
    : options_(options),
      dirname_(dirname),
      num_parts_(0),
      part_mask_(~static_cast<uint32_t>(0)),
      data_(data) {
  assert(data_ != NULL);
  data_->Ref();
}

ReaderImpl::~ReaderImpl() { data_->Unref(); }

void ReaderImpl::List(std::vector<std::string>* names) {
  // TODO
}

bool ReaderImpl::Exists(const Slice& fname) {
  // TODO
  return true;
}

static Status NewLogSrc(const std::string& fname, Env* env, LogSource** ptr) {
  *ptr = NULL;
  SequentialFile* file;
  uint64_t size;
  Status status = env->NewSequentialFile(fname, &file);
  if (status.ok()) {
    status = env->GetFileSize(fname, &size);
    if (!status.ok()) {
      delete file;
    }
  }
  if (status.ok()) {
    const size_t io_size = 1 << 20;  // Less physical I/O ops
    WholeFileBufferedRandomAccessFile* buffered_file =
        new WholeFileBufferedRandomAccessFile(file, size, io_size);
    status = buffered_file->Load();
    if (!status.ok()) {
      delete buffered_file;
    } else {
      PrintLogStream(fname, size);
      LogSource* src = new LogSource(buffered_file, size);
      src->Ref();
      *ptr = src;
    }
  }

  return status;
}

static Status NewUnbufferedLogSrc(const std::string& fname, Env* env,
                                  LogSource** ptr) {
  RandomAccessFile* file;
  uint64_t size;
  Status status = env->NewRandomAccessFile(fname, &file);
  if (status.ok()) {
    status = env->GetFileSize(fname, &size);
    if (!status.ok()) {
      delete file;
    }
  }
  if (status.ok()) {
    PrintLogStream(fname, 0);
    LogSource* src = new LogSource(file, size);
    src->Ref();
    *ptr = src;
  } else {
    *ptr = NULL;
  }

  return status;
}

Status ReaderImpl::ReadAll(const Slice& fname, std::string* dst) {
  Status status;
  PlfsIoReader* reader = NULL;
  LogSource* index = NULL;
  uint32_t hash = Hash(fname.data(), fname.size(), 0);
  uint32_t part = hash & part_mask_;
  if (part < num_parts_) {
    status = NewLogSrc(PartitionIndexFileName(dirname_, options_.rank, part),
                       options_.env, &index);
    MutexLock ml(&mutex_);
    if (status.ok()) {
      status = PlfsIoReader::Open(options_, data_, index, &reader);
      if (status.ok()) {
        status = reader->Gets(fname, dst);
      }
    }
  }

  if (reader != NULL) {
    delete reader;
  }
  if (index != NULL) {
    index->Unref();
  }

  return status;
}

Reader::~Reader() {}

static DirOptions SanitizeReadOptions(const DirOptions& options) {
  DirOptions result = options;
  if (result.env == NULL) {
    result.env = Env::Default();
  }
  return result;
}

Status Reader::Open(const DirOptions& opts, const std::string& dirname,
                    Reader** ptr) {
  *ptr = NULL;
  DirOptions options = SanitizeReadOptions(opts);
  const size_t num_parts = 1u << options.lg_parts;
  const int my_rank = options.rank;
  Env* const env = options.env;
#if VERBOSE >= 2
  Verbose(__LOG_ARGS__, 2, "plfsdir.name -> %s", dirname.c_str());
  Verbose(__LOG_ARGS__, 2, "plfsdir.verify_checksums -> %d",
          int(options.verify_checksums));
  Verbose(__LOG_ARGS__, 2, "plfsdir.unique_keys -> %d",
          int(options.unique_keys));
  Verbose(__LOG_ARGS__, 2, "plfsdir.num_parts_per_rank -> %u",
          static_cast<unsigned>(num_parts));
  Verbose(__LOG_ARGS__, 2, "plfsdir.my_rank -> %d", my_rank);
#endif
  Status status;
  LogSource* data = NULL;
  status = NewUnbufferedLogSrc(DataFileName(dirname, my_rank), env, &data);
  if (status.ok()) {
    ReaderImpl* impl = new ReaderImpl(options, dirname, data);
    impl->part_mask_ = num_parts - 1;
    impl->num_parts_ = num_parts;
    *ptr = impl;
  }

  if (data != NULL) {
    data->Unref();
  }

  return status;
}

static Status DeleteLogStream(const std::string& fname, Env* env) {
#if VERBOSE >= 3
  Verbose(__LOG_ARGS__, 3, "Delete plfs io log: %s", fname.c_str());
#endif
  return env->DeleteFile(fname);
}

Status DestroyDir(const std::string& dirname, const DirOptions& opts) {
  Status status;
  DirOptions options = SanitizeReadOptions(opts);
  Env* const env = options.env;
#if 0
  std::vector<std::string> names;
  status = env->GetChildren(dirname, &names);
  if (status.ok()) {
    for (size_t i = 0; i < names.size(); i++) {
      if (!Slice(names[i]).starts_with(".")) {
        status = DeleteLogStream(dirname + "/" + names[i], env);
        if (!status.ok()) {
          break;
        }
      }
    }

    env->DeleteDir(dirname);
  }
#else
  const size_t num_parts = 1u << options.lg_parts;
  const int my_rank = options.rank;
  std::vector<std::string> names;
  names.push_back(DataFileName(dirname, my_rank));
  for (size_t part = 0; part < num_parts; part++) {
    names.push_back(PartitionIndexFileName(dirname, my_rank, part));
  }
  if (status.ok()) {
    for (size_t i = 0; i < names.size(); i++) {
      status = DeleteLogStream(names[i], env);
      if (!status.ok()) {
        break;
      }
    }
  }
#endif
  return status;
}

}  // namespace plfsio
}  // namespace pdlfs
