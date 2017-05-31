/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio.h"
#include "deltafs_plfsio_batch.h"
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
      memtable_util(1.0),
      key_size(8),
      value_size(32),
      bf_bits_per_key(8),
      block_size(128 << 10),
      block_util(0.999),
      block_padding(true),
      block_buffer(2 << 20),
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
  mu_ = NULL;
  if (file_ != NULL) {
    Lclose();
  }
}

Status LogSink::Lclose(bool sync) {
  Status status;
  if (file_ != NULL) {
    if (mu_ != NULL) mu_->AssertHeld();
    if (sync) status = file_->Sync();
    if (status.ok()) {
      status = Close();
    }
  }
  return status;
}

Status LogSink::Close() {
  assert(file_ != NULL);
  Status status = file_->Close();
  delete file_;
  file_ = NULL;
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

class DirWriterImpl : public DirWriter {
 public:
  DirWriterImpl(const DirOptions& options);
  virtual ~DirWriterImpl();

  virtual size_t total_memory_usage() const;
  virtual CompactionStats stats() const {
    MutexLock ml(&mutex_);
    return stats_;
  }

  virtual Status Wait();
  virtual Status Write(BatchCursor* cursor, int epoch);
  virtual Status Append(const Slice& fid, const Slice& data, int epoch);
  virtual Status Flush(int epoch);
  virtual Status EpochFlush(int epoch);
  virtual Status Finish();

 private:
  Status WaitForCompaction();
  Status TryFlush(bool epoch_flush = false, bool finalize = false);
  Status TryBatchWrites(BatchCursor* cursor);
  Status TryAppend(const Slice& fid, const Slice& data);
  Status EnsureDataPadding(LogSink* sink, char p = 0);
  Status CloseLogs();
  void MaybeSlowdownCaller();
  friend class DirWriter;

  CompactionStats stats_;
  std::vector<std::string*> write_bufs_;
  const DirOptions options_;
  mutable port::Mutex io_mutex_;  // Protecting the shared data log
  mutable port::Mutex mutex_;
  port::CondVar cond_var_;
  int num_epochs_;
  uint32_t num_parts_;
  uint32_t part_mask_;
  Status finish_status_;
  bool has_pending_flush_;
  bool finished_;  // If Finish() has been called
  DirLogger** dpts_;
  LogSink* data_;
};

DirWriterImpl::DirWriterImpl(const DirOptions& options)
    : options_(options),
      cond_var_(&mutex_),
      num_epochs_(0),
      num_parts_(0),
      part_mask_(~static_cast<uint32_t>(0)),
      has_pending_flush_(false),
      finished_(false),
      dpts_(NULL),
      data_(NULL) {}

DirWriterImpl::~DirWriterImpl() {
  MutexLock l(&mutex_);
  for (size_t i = 0; i < num_parts_; i++) {
    delete dpts_[i];
  }
  delete[] dpts_;
  if (data_ != NULL) {
    data_->Unref();
  }
}

void DirWriterImpl::MaybeSlowdownCaller() {
  mutex_.AssertHeld();
  Env* const env = options_.env;
  const uint64_t micros = options_.slowdown_micros;
  if (micros != 0) {
    mutex_.Unlock();
    env->SleepForMicroseconds(micros);
    mutex_.Lock();
  }
}

Status DirWriterImpl::EnsureDataPadding(LogSink* sink, char padding) {
  Status status;
  sink->Lock();
  const uint64_t total_size = sink->Ltell();
  const size_t overflow = total_size % options_.data_buffer;
  if (overflow != 0) {
    const size_t n = options_.data_buffer - overflow;
    status = sink->Lwrite(std::string(n, padding));
  }
  sink->Unlock();
  return status;
}

Status DirWriterImpl::CloseLogs() {
  mutex_.AssertHeld();
  Status status;
  if (options_.tail_padding) {
    status = EnsureDataPadding(data_);
  }

  if (status.ok()) {
    for (size_t i = 0; i < num_parts_; i++) {
      status = dpts_[i]->PreClose();
      if (!status.ok()) {
        break;
      }
    }
  }

  return status;
}

Status DirWriterImpl::WaitForCompaction() {
  mutex_.AssertHeld();
  Status status;
  for (size_t i = 0; i < num_parts_; i++) {
    status = dpts_[i]->Wait();
    if (!status.ok()) {
      break;
    }
  }
  return status;
}

Status DirWriterImpl::TryBatchWrites(BatchCursor* cursor) {
  mutex_.AssertHeld();
  Status status;
  // TODO
  return status;
}

// Attempt to force a minor compaction on all directory partitions
// simultaneously. If a partition cannot be compacted immediately due to a lack
// of buffer space, skip it and add it to a waiting list so it can be
// reattempted later. Return immediately as soon as all partitions have a minor
// compaction scheduled. Will not wait for all compactions to finish.
// Return OK on success, or a non-OK status on errors.
Status DirWriterImpl::TryFlush(bool epoch_flush, bool finalize) {
  mutex_.AssertHeld();
  assert(has_pending_flush_);
  Status status;
  std::vector<DirLogger*> remaining;
  for (size_t i = 0; i < num_parts_; i++) remaining.push_back(dpts_[i]);
  std::vector<DirLogger*> waiting_list;

  DirLogger::FlushOptions flush_options(epoch_flush, finalize);
  while (!remaining.empty()) {
    waiting_list.clear();
    for (size_t i = 0; i < remaining.size(); i++) {
      flush_options.dry_run =
          true;  // Avoid being blocked waiting for buffer space to reappear
      status = remaining[i]->Flush(flush_options);
      flush_options.dry_run = false;

      if (status.IsBufferFull()) {
        waiting_list.push_back(remaining[i]);  // Try again later
        status = Status::OK();
      } else if (status.ok()) {
        remaining[i]->Flush(flush_options);
      } else {
        break;
      }
    }

    if (status.ok()) {
      if (!waiting_list.empty()) {
        cond_var_.Wait();  // Waiting for buffer space
      }
      waiting_list.swap(remaining);
    } else {
      break;
    }
  }

  return status;
}

// Force a minor compaction, wait for it to complete, and finalize all log
// objects. If there happens to be another pending minor compaction currently
// waiting to be scheduled, wait until it is scheduled (but not necessarily
// completed) before processing this one. After this call, either success or
// error, no further write operation will be allowed in future.
// Return OK on success, or a non-OK status on errors.
Status DirWriterImpl::Finish() {
  Status status;
  MutexLock ml(&mutex_);
  while (true) {
    if (finished_) {
      status = finish_status_;  // Return the cached result
      break;
    } else if (has_pending_flush_) {
      cond_var_.Wait();
    } else {
      has_pending_flush_ = true;
      const bool epoch_flush = true;
      const bool finalize = true;
      status = TryFlush(epoch_flush, finalize);
      if (status.ok()) status = WaitForCompaction();
      if (status.ok()) status = CloseLogs();
      has_pending_flush_ = false;
      cond_var_.SignalAll();
      finish_status_ = status;
      finished_ = true;
      break;
    }
  }
  return status;
}

// Force a minor compaction to start a new epoch, but return immediately without
// waiting for the compaction to complete. If there happens to be another
// pending minor compaction currently waiting to be scheduled, wait until it is
// scheduled (but not necessarily completed) before validating and submitting
// this one. Return OK on success, or a non-OK status on errors.
Status DirWriterImpl::EpochFlush(int epoch) {
  Status status;
  MutexLock ml(&mutex_);
  while (true) {
    if (finished_) {
      status = Status::AssertionFailed("Plfsdir already finished");
      break;
    } else if (has_pending_flush_) {
      cond_var_.Wait();
    } else if (epoch != -1 && epoch < num_epochs_) {
      status = Status::AlreadyExists(Slice());
      break;
    } else if (epoch != -1 && epoch > num_epochs_) {
      status = Status::NotFound(Slice());
      break;
    } else {
      has_pending_flush_ = true;
      const bool epoch_flush = true;
      status = TryFlush(epoch_flush);
      if (status.ok()) num_epochs_++;
      has_pending_flush_ = false;
      cond_var_.SignalAll();
      break;
    }
  }
  return status;
}

// Force a minor compaction but return immediately without waiting for it
// to complete. If there happens to be another pending minor compaction
// currently waiting to be scheduled, wait until it is scheduled (but not
// necessarily completed) before validating and submitting this one.
// Return OK on success, or a non-OK status on errors.
Status DirWriterImpl::Flush(int epoch) {
  Status status;
  MutexLock ml(&mutex_);
  while (true) {
    if (finished_) {
      status = Status::AssertionFailed("Plfsdir already finished");
      break;
    } else if (has_pending_flush_) {
      cond_var_.Wait();
    } else if (epoch != -1 && epoch != num_epochs_) {
      status = Status::AssertionFailed("Bad epoch num");
      break;
    } else {
      has_pending_flush_ = true;
      status = TryFlush();
      has_pending_flush_ = false;
      cond_var_.SignalAll();
      break;
    }
  }
  return status;
}

Status DirWriterImpl::Write(BatchCursor* cursor, int epoch) {
  Status status;
  MutexLock ml(&mutex_);
  while (true) {
    if (finished_) {
      status = Status::AssertionFailed("Plfsdir already finished");
      break;
    } else if (has_pending_flush_) {
      cond_var_.Wait();
    } else if (epoch != -1 && epoch != num_epochs_) {
      status = Status::AssertionFailed("Bad epoch num");
      break;
    } else {
      // Batch writes may trigger one or more flushes
      has_pending_flush_ = true;
      status = TryBatchWrites(cursor);
      has_pending_flush_ = false;
      cond_var_.SignalAll();
      break;
    }
  }
  return status;
}

Status DirWriterImpl::Append(const Slice& fid, const Slice& data, int epoch) {
  Status status;
  MutexLock ml(&mutex_);
  while (true) {
    if (finished_) {
      status = Status::AssertionFailed("Plfsdir already finished");
      break;
    } else if (has_pending_flush_) {
      cond_var_.Wait();
    } else if (epoch != -1 && epoch != num_epochs_) {
      status = Status::AssertionFailed("Bad epoch num");
      break;
    } else {
      // Appends may also trigger flushes
      has_pending_flush_ = true;
      status = TryAppend(fid, data);
      has_pending_flush_ = false;
      cond_var_.SignalAll();
      if (status.IsBufferFull()) {
        MaybeSlowdownCaller();
      }
      break;
    }
  }
  return status;
}

Status DirWriterImpl::TryAppend(const Slice& fid, const Slice& data) {
  mutex_.AssertHeld();
  Status status;
  const uint32_t hash = Hash(fid.data(), fid.size(), 0);
  const uint32_t part = hash & part_mask_;
  assert(part < num_parts_);
  status = dpts_[part]->Add(fid, data);
  return status;
}

// Wait for all on-going compactions to finish.
// Return OK on success, or a non-OK status on errors.
Status DirWriterImpl::Wait() {
  Status status;
  MutexLock ml(&mutex_);
  if (!finished_) {
    status = WaitForCompaction();
  } else {
    status = finish_status_;
  }
  return status;
}

size_t DirWriterImpl::total_memory_usage() const {
  MutexLock ml(&mutex_);
  size_t result = 0;
  for (size_t i = 0; i < num_parts_; i++) result += dpts_[i]->memory_usage();
  for (size_t i = 0; i < write_bufs_.size(); i++) {
    result += write_bufs_[i]->capacity();
  }
  return result;
}

DirWriter::~DirWriter() {}

template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) {
    *ptr = maxvalue;
  }
  if (static_cast<V>(*ptr) < minvalue) {
    *ptr = minvalue;
  }
}

// Fix user-supplied options to be reasonable
static DirOptions SanitizeWriteOptions(const DirOptions& options) {
  DirOptions result = options;
  if (result.env == NULL) result.env = Env::Default();
  ClipToRange(&result.memtable_buffer, 1 << 20, 1 << 30);
  ClipToRange(&result.memtable_util, 0.5, 1.0);
  ClipToRange(&result.block_size, 4 << 10, 4 << 20);
  ClipToRange(&result.block_util, 0.5, 1.0);
  ClipToRange(&result.lg_parts, 0, 8);
  return result;
}

static void PrintLogInfo(const std::string& name, const size_t mem_size) {
#if VERBOSE >= 3
  Verbose(__LOG_ARGS__, 3, "Reading or writing %s, mem reserved: %s",
          name.c_str(), PrettySize(mem_size).c_str());
#endif
}

// Try opening a writable log sink and store its handle in *result.
// If mu is not NULL, return a LogSink associated with the mutex.
// If buf_size is not zero, create a stacked UnsafeBufferedWritableFile to
// ensure the write size. If bytes is not NULL, also create a stacked
// MeasuredWritableFile to track the size of data written to the log.
static Status OpenLogSink(LogSink** result, const std::string& name,
                          Env* env = Env::Default(), const size_t buf_size = 0,
                          port::Mutex* io_mutex = NULL,
                          std::vector<std::string*>* write_bufs = NULL,
                          uint64_t* bytes = NULL) {
  *result = NULL;

  WritableFile* file = NULL;
  Status status = env->NewWritableFile(name, &file);
  if (status.ok()) {
    assert(file != NULL);
    if (bytes != NULL) file = new MeasuredWritableFile(file, bytes);
    if (buf_size != 0) {
      UnsafeBufferedWritableFile* buffered =
          new UnsafeBufferedWritableFile(file, buf_size);
      write_bufs->push_back(buffered->buffer_store());
      file = buffered;
    }
    PrintLogInfo(name, buf_size);
    LogSink* sink = new LogSink(name, file, io_mutex);
    sink->Ref();

    *result = sink;
  }

  return status;
}

Status DirWriter::Open(const DirOptions& opts, const std::string& name,
                       DirWriter** result) {
  *result = NULL;
  DirOptions options = SanitizeWriteOptions(opts);
  const uint32_t num_parts = static_cast<uint32_t>(1 << options.lg_parts);
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

  DirWriterImpl* impl = new DirWriterImpl(options);
  std::vector<LogSink*> index(num_parts, NULL);
  std::vector<LogSink*> data(1, NULL);  // Shared among all directory partitions
  std::vector<std::string*> write_bufs;
  status = OpenLogSink(&data[0], DataFileName(name, my_rank), env,
                       options.data_buffer, &impl->io_mutex_, &write_bufs,
                       &impl->stats_.data_written);
  if (status.ok()) {
    for (size_t part = 0; part < num_parts; part++) {
      status = OpenLogSink(
          &index[part], PartitionIndexFileName(name, my_rank, int(part)), env,
          options.index_buffer, NULL, &write_bufs, &impl->stats_.index_written);
      if (!status.ok()) {
        break;
      }
    }
  }

  if (status.ok()) {
    DirLogger** dpts = new DirLogger*[num_parts];
    for (size_t i = 0; i < num_parts; i++) {
      dpts[i] = new DirLogger(impl->options_, &impl->mutex_, &impl->cond_var_,
                              index[i],  // Partitioned index log object
                              data[0],   // Shared data log object
                              &impl->stats_);
    }
    impl->data_ = data[0];
    impl->data_->Ref();
    // Weak references to log buffers that must not be deleted
    // during the lifetime of this directory.
    impl->write_bufs_.swap(write_bufs);
    impl->part_mask_ = num_parts - 1;
    impl->num_parts_ = num_parts;
    impl->dpts_ = dpts;
    *result = impl;
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
  Verbose(__LOG_ARGS__, 3, "Removing %s ...", fname.c_str());
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
