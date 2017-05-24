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

#include "pdlfs-common/env_buf.h"
#include "pdlfs-common/hash.h"
#include "pdlfs-common/logging.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/strutil.h"

#include <stdio.h>
#include <string>
#include <vector>

namespace pdlfs {
namespace plfsio {

CompactionStats::CompactionStats()
    : write_micros(0),
      index_written(0),
      index_size(0),
      data_written(0),
      data_size(0) {}

DirOptions::DirOptions()
    : memtable_buffer(32 << 20),
      key_size(8),
      value_size(32),
      bf_bits_per_key(8),
      block_size(128 << 10),
      block_util(0.999),
      uberblock_depth(16),
      data_buffer(4 << 20),
      index_buffer(4 << 20),
      tail_padding(false),
      compaction_pool(NULL),
      non_blocking(false),
      slowdown_micros(0),
      unique_keys(true),
      verify_checksums(false),
      lg_parts(0),
      env(NULL),
      is_env_pfs(true),
      rank(0) {}

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
        options.memtable_buffer = num;
      }
    } else if (conf_key == "memtable_buffer") {
      if (ParsePrettyNumber(conf_value, &num)) {
        options.memtable_buffer = num;
      }
    } else if (conf_key == "data_buffer") {
      if (ParsePrettyNumber(conf_value, &num)) {
        options.data_buffer = num;
      }
    } else if (conf_key == "index_buffer") {
      if (ParsePrettyNumber(conf_value, &num)) {
        options.index_buffer = num;
      }
    } else if (conf_key == "tail_padding") {
      if (ParsePrettyBool(conf_value, &flag)) {
        options.tail_padding = flag;
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

void LogSink::Unref() {
  assert(refs_ > 0);
  refs_--;
  if (refs_ == 0) {
    delete this;
  }
}

LogSink::~LogSink() {
  if (file_ != NULL) {
    Lclose();
  }
}

Status LogSink::Lclose(bool sync) {
  Status status;
  if (file_ != NULL) {
    if (sync) status = file_->Sync();
    if (status.ok()) {
      status = file_->Close();
    }
    if (!status.ok()) {
      Error(__LOG_ARGS__, "Error closing log object (or file) %s: %s",
            filename_.c_str(), status.ToString().c_str());
    }
    delete file_;
    file_ = NULL;
  }
  return status;
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
  char tmp[30];
  snprintf(tmp, sizeof(tmp), "/L-%08x.idx.%02x", rank, partition);
  return parent + tmp;
}

static std::string DataFileName(const std::string& parent, int rank) {
  char tmp[30];
  snprintf(tmp, sizeof(tmp), "/L-%08x.dat", rank);
  return parent + tmp;
}

class WriterImpl : public Writer {
 public:
  WriterImpl(const DirOptions& options);
  virtual ~WriterImpl();

  virtual const CompactionStats* stats() const { return &stats_; }
  virtual Status Append(const Slice& fname, const Slice& data);
  virtual Status Sync();
  virtual Status MakeEpoch();
  virtual Status Finish();

 private:
  Status EnsureDataPadding(LogSink* sink);
  void MaybeSlowdownCaller();
  friend class Writer;

  CompactionStats stats_;
  const DirOptions options_;
  port::Mutex mutex_;
  port::CondVar cond_var_;
  size_t num_parts_;
  uint32_t part_mask_;
  Status finish_status_;
  bool finished_;  // If Finish() has been called
  PlfsIoLogger** io_;
  LogSink* data_;
};

WriterImpl::WriterImpl(const DirOptions& options)
    : options_(options),
      cond_var_(&mutex_),
      num_parts_(0),
      part_mask_(~static_cast<uint32_t>(0)),
      finished_(false),
      io_(NULL),
      data_(NULL) {}

WriterImpl::~WriterImpl() {
  MutexLock l(&mutex_);
  for (size_t i = 0; i < num_parts_; i++) {
    delete io_[i];
  }
  delete[] io_;
  if (data_ != NULL) {
    data_->Unref();
  }
}

void WriterImpl::MaybeSlowdownCaller() {
  Env* const env = options_.env;
  const uint64_t micros = options_.slowdown_micros;
  if (micros != 0) {
    env->SleepForMicroseconds(micros);
  }
}

Status WriterImpl::EnsureDataPadding(LogSink* sink) {
  Status status;
  const uint64_t total_size = sink->Ltell();
  const size_t overflow = total_size % options_.data_buffer;

  if (overflow != 0) {
    const size_t padding = options_.data_buffer - overflow;
    assert(padding < options_.data_buffer);
    status = sink->Lwrite(std::string(padding, 0));
  }

  return status;
}

Status WriterImpl::Finish() {
  Status status;
  if (!finish_status_.ok()) {
    status = finish_status_;
  } else if (!finished_) {
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

    // Wait for compaction
    if (status.ok()) {
      for (size_t i = 0; i < num_parts_; i++) {
        status = io_[i]->Wait();
        if (!status.ok()) {
          break;
        }
      }
    }

    // Padding
    if (status.ok() && data_ != NULL) {
      if (options_.tail_padding) {
        status = EnsureDataPadding(data_);
      }
    }

    // Close logs
    if (status.ok()) {
      for (size_t i = 0; i < num_parts_; i++) {
        status = io_[i]->Close();
        if (!status.ok()) {
          break;
        }
      }
    }

    // Set error status
    if (!status.ok()) {
      if (!status.IsBufferFull()) {
        finish_status_ = status;
      }
    }

    if (status.ok()) {
      finished_ = true;
    }
  }

  if (status.IsBufferFull()) {
    MaybeSlowdownCaller();
  }

  return status;
}

Status WriterImpl::MakeEpoch() {
  Status status;
  if (finished_) {
    status = Status::AssertionFailed("finished");
  } else {
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
  if (finished_) {
    status = Status::AssertionFailed("finished");
  } else {
    uint32_t hash = Hash(fname.data(), fname.size(), 0);
    uint32_t part = hash & part_mask_;
    if (part < num_parts_) {
      MutexLock ml(&mutex_);
      status = io_[part]->Add(fname, data);
    }
  }

  if (status.IsBufferFull()) {
    MaybeSlowdownCaller();
  }
  return status;
}

Status WriterImpl::Sync() {
  return Status::NotSupported(Slice());  // TODO
}

Writer::~Writer() {}

static DirOptions SanitizeWriteOptions(const DirOptions& options) {
  DirOptions result = options;
  if (result.env == NULL) result.env = Env::Default();
  if (result.lg_parts < 0) result.lg_parts = 0;
  if (result.lg_parts > 8) result.lg_parts = 8;
  return result;
}

static void PrintLogInfo(const std::string& name, size_t mem_size) {
#if VERBOSE >= 3
  Verbose(__LOG_ARGS__, 3, "Accessing %s, mem reserved: %s", name.c_str(),
          PrettySize(mem_size).c_str());
#endif
}

// If bytes is not NULL, also create a MeasuredWritableFile to
// count the total number of bytes written to the log.
static Status NewLogSink(const std::string& name, Env* env, size_t buf_size,
                         LogSink** ptr, uint64_t* bytes = NULL) {
  WritableFile* file;
  Status status = env->NewWritableFile(name, &file);
  if (status.ok()) {
    assert(file != NULL);
    if (bytes != NULL) file = new MeasuredWritableFile(file, bytes);
    if (buf_size != 0) file = new UnsafeBufferedWritableFile(file, buf_size);
    PrintLogInfo(name, buf_size);
    LogSink* sink = new LogSink(name, file);
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
  const size_t num_parts = static_cast<size_t>(1 << options.lg_parts);
  const int my_rank = options.rank;
  Env* const env = options.env;
#if VERBOSE >= 2
  Verbose(__LOG_ARGS__, 2, "plfsdir.name -> %s", name.c_str());
  Verbose(__LOG_ARGS__, 2, "plfsdir.memtable_size -> %s",
          PrettySize(options.memtable_buffer).c_str());
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
  Verbose(__LOG_ARGS__, 2, "plfsdir.tail_padding -> %d",
          int(options.tail_padding));
  Verbose(__LOG_ARGS__, 2, "plfsdir.unique_keys -> %d",
          int(options.unique_keys));
  Verbose(__LOG_ARGS__, 2, "plfsdir.num_parts_per_rank -> %u",
          static_cast<unsigned>(num_parts));
  Verbose(__LOG_ARGS__, 2, "plfsdir.my_rank -> %d", my_rank);
#endif
  Status status;
  if (options.is_env_pfs) {
    // Ignore error since it may already exist
    env->CreateDir(name);
  }

  WriterImpl* impl = new WriterImpl(options);
  std::vector<LogSink*> index(num_parts, NULL);
  std::vector<LogSink*> data(1, NULL);  // Shared among all directory partitions
  status = NewLogSink(DataFileName(name, my_rank), env, options.data_buffer,
                      &data[0], &impl->stats_.data_written);
  for (size_t part = 0; part < num_parts; part++) {
    if (status.ok()) {
      status = NewLogSink(PartitionIndexFileName(name, my_rank, part), env,
                          options.index_buffer, &index[part],
                          &impl->stats_.index_written);
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
    impl->data_ = data[0];
    impl->data_->Ref();
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
    const size_t io_size = 8 << 20;  // Less physical I/O ops
    WholeFileBufferedRandomAccessFile* buffered_file =
        new WholeFileBufferedRandomAccessFile(file, size, io_size);
    status = buffered_file->Load();
    if (!status.ok()) {
      delete buffered_file;
    } else {
      PrintLogInfo(fname, size);
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
    PrintLogInfo(fname, 0);
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
