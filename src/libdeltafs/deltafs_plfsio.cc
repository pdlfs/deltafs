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

#include "pdlfs-common/env_files.h"
#include "pdlfs-common/hash.h"
#include "pdlfs-common/logging.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/strutil.h"

#include <stdio.h>
#include <string>
#include <vector>

namespace pdlfs {
namespace plfsio {

IoStats::IoStats() : index_bytes(0), index_ops(0), data_bytes(0), data_ops(0) {}

DirOptions::DirOptions()
    : total_memtable_budget(32 << 20),
      memtable_util(1.0),
      skip_sort(false),
      key_size(8),
      value_size(32),
      bf_bits_per_key(8),
      block_size(128 << 10),
      block_util(0.999),
      block_padding(true),
      block_batch_size(2 << 20),
      data_buffer(4 << 20),
      index_buffer(4 << 20),
      tail_padding(false),
      compaction_pool(NULL),
      reader_pool(NULL),
      read_size(8 << 20),
      parallel_reads(false),
      non_blocking(false),
      slowdown_micros(0),
      paranoid_checks(false),
      unique_keys(true),
      ignore_filters(false),
      compression(kNoCompression),
      force_compression(false),
      verify_checksums(false),
      skip_checksums(false),
      measure_reads(true),
      measure_writes(true),
      lg_parts(0),
      listener(NULL),
      env(NULL),
      allow_env_threads(false),
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
        options.total_memtable_budget = num;
      }
    } else if (conf_key == "memtable_buffer") {
      if (ParsePrettyNumber(conf_value, &num)) {
        options.total_memtable_budget = num;
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

static std::string IndexFileName(const std::string& parent, int rank,
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

  virtual IoStats GetIoStats() const;

  virtual uint64_t TEST_estimated_sstable_size() const;
  virtual uint32_t TEST_num_sstables() const;
  virtual uint64_t TEST_total_memory_usage() const;

  virtual uint64_t TEST_index_size() const;
  virtual uint64_t TEST_filter_size() const;
  virtual uint64_t TEST_data_size() const;

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
  Status EnsureDataPadding(LogSink* sink, size_t footer_size);
  Status Finalize();
  void MaybeSlowdownCaller();
  friend class DirWriter;

  const DirOptions options_;
  mutable port::Mutex io_mutex_;  // Protecting the shared data log
  mutable port::Mutex mutex_;
  port::CondVar cond_var_;
  int num_epochs_;
  uint32_t num_parts_;
  uint32_t part_mask_;
  Status finish_status_;
  // XXX: rather than using a global barrier, using a reference
  // counted epoch object might slightly increase implementation
  // concurrency, though largely not needed so far
  bool has_pending_flush_;
  bool finished_;  // If Finish() has been called
  WritableFileStats io_stats_;
  std::vector<const OutputStats*> compaction_stats_;
  std::vector<std::string*> write_bufs_;
  DirLogger** dirs_;
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
      dirs_(NULL),
      data_(NULL) {}

DirWriterImpl::~DirWriterImpl() {
  MutexLock l(&mutex_);
  for (size_t i = 0; i < num_parts_; i++) {
    if (dirs_[i] != NULL) {
      dirs_[i]->Unref();
    }
  }
  delete[] dirs_;
  if (data_ != NULL) {
    data_->Unref();
  }
}

void DirWriterImpl::MaybeSlowdownCaller() {
  mutex_.AssertHeld();
  Env* const env = options_.env;
  const int micros = static_cast<int>(options_.slowdown_micros);
  if (micros != 0) {
    mutex_.Unlock();
    env->SleepForMicroseconds(micros);
    mutex_.Lock();
  }
}

Status DirWriterImpl::EnsureDataPadding(LogSink* sink, size_t footer_size) {
  Status status;
  sink->Lock();
  // Add enough padding to ensure the final size of the index log
  // is some multiple of the physical write size.
  const uint64_t total_size = sink->Ltell() + footer_size;
  const size_t overflow = total_size % options_.data_buffer;
  if (overflow != 0) {
    const size_t n = options_.data_buffer - overflow;
    status = sink->Lwrite(std::string(n, 0));
  } else {
    // No need to pad
  }
  sink->Unlock();
  return status;
}

Status DirWriterImpl::Finalize() {
  mutex_.AssertHeld();
  BlockHandle dummy_handle;
  Footer footer;

  dummy_handle.set_offset(0);
  dummy_handle.set_size(0);
  footer.set_epoch_index_handle(dummy_handle);

  footer.set_num_epoches(0);
  footer.set_lg_parts(static_cast<uint32_t>(options_.lg_parts));
  footer.set_unique_keys(static_cast<unsigned char>(options_.unique_keys));
  footer.set_skip_checksums(
      static_cast<unsigned char>(options_.skip_checksums));

  Status status;
  std::string footer_buf;
  footer.EncodeTo(&footer_buf);
  if (options_.tail_padding) {
    status = EnsureDataPadding(data_, footer_buf.size());
  }

  if (status.ok()) {
    data_->Lock();
    status = data_->Lwrite(footer_buf);
    data_->Unlock();
  }

  if (status.ok()) {
    for (size_t i = 0; i < num_parts_; i++) {
      status = dirs_[i]->SyncAndClose();
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
    status = dirs_[i]->Wait();
    if (!status.ok()) {
      break;
    }
  }
  return status;
}

Status DirWriterImpl::TryBatchWrites(BatchCursor* cursor) {
  mutex_.AssertHeld();
  assert(has_pending_flush_);
  Status status;
  std::vector<std::vector<uint32_t> > waiting_list(num_parts_);
  std::vector<std::vector<uint32_t> > queue(num_parts_);
  cursor->Seek(0);
  for (; cursor->Valid(); cursor->Next()) {
    Slice fid = cursor->fid();
    const uint32_t hash = Hash(fid.data(), fid.size(), 0);
    const uint32_t part = hash & part_mask_;
    assert(part < num_parts_);
    status = dirs_[part]->Add(fid, cursor->data());

    if (status.IsBufferFull()) {
      // Try again later
      waiting_list[part].push_back(cursor->offset());
      status = Status::OK();
    } else if (!status.ok()) {
      break;
    } else {
      // OK
    }
  }

  while (status.ok()) {
    for (size_t i = 0; i < num_parts_; i++) {
      waiting_list[i].swap(queue[i]);
      waiting_list[i].clear();
    }
    bool has_more = false;  // If the entire batch is done
    for (size_t i = 0; i < num_parts_; i++) {
      if (!queue[i].empty()) {
        std::vector<uint32_t>::iterator it = queue[i].begin();
        for (; it != queue[i].end(); ++it) {
          cursor->Seek(*it);
          if (cursor->Valid()) {
            status = dirs_[i]->Add(cursor->fid(), cursor->data());
          } else {
            status = cursor->status();
          }

          if (status.IsBufferFull()) {
            // Try again later
            waiting_list[i].assign(it, queue[i].end());
            status = Status::OK();
            has_more = true;
            break;
          } else if (!status.ok()) {
            break;
          } else {
            // OK
          }
        }

        if (!status.ok()) {
          break;
        }
      }
    }

    if (!status.ok()) {
      break;
    } else if (has_more) {
      cond_var_.Wait();
    } else {
      break;
    }
  }

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
  for (size_t i = 0; i < num_parts_; i++) remaining.push_back(dirs_[i]);
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
      if (status.ok()) status = Finalize();
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
  status = dirs_[part]->Add(fid, data);
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

IoStats DirWriterImpl::GetIoStats() const {
  MutexLock ml(&mutex_);
  IoStats result;
  for (size_t i = 0; i < num_parts_; i++) {
    result.index_bytes += dirs_[i]->io_stats_.TotalBytes();
    result.index_ops += dirs_[i]->io_stats_.TotalOps();
  }
  result.data_bytes = io_stats_.TotalBytes();
  result.data_ops = io_stats_.TotalOps();
  return result;
}

uint64_t DirWriterImpl::TEST_estimated_sstable_size() const {
  MutexLock ml(&mutex_);
  uint64_t result = 0;
  if (num_parts_ != 0) {
    result = dirs_[0]->estimated_table_size();
  }
  return result;
}

uint32_t DirWriterImpl::TEST_num_sstables() const {
  MutexLock ml(&mutex_);
  uint32_t result = 0;
  for (size_t i = 0; i < num_parts_; i++) {
    result += dirs_[i]->num_tables();
  }
  return result;
}

uint64_t DirWriterImpl::TEST_total_memory_usage() const {
  MutexLock ml(&mutex_);
  uint64_t result = 0;
  for (size_t i = 0; i < num_parts_; i++) result += dirs_[i]->memory_usage();
  for (size_t i = 0; i < write_bufs_.size(); i++) {
    result += write_bufs_[i]->capacity();
  }
  return result;
}

uint64_t DirWriterImpl::TEST_index_size() const {
  MutexLock ml(&mutex_);
  uint64_t result = 0;
  for (size_t i = 0; i < compaction_stats_.size(); i++) {
    result += compaction_stats_[i]->index_size;
  }
  return result;
}

uint64_t DirWriterImpl::TEST_filter_size() const {
  MutexLock ml(&mutex_);
  uint64_t result = 0;
  for (size_t i = 0; i < compaction_stats_.size(); i++) {
    result += compaction_stats_[i]->filter_size;
  }
  return result;
}

uint64_t DirWriterImpl::TEST_data_size() const {
  MutexLock ml(&mutex_);
  uint64_t result = 0;
  for (size_t i = 0; i < compaction_stats_.size(); i++) {
    result += compaction_stats_[i]->data_size;
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
  ClipToRange(&result.total_memtable_budget, 1 << 20, 1 << 30);
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

// Try opening a log sink and store its handle in *result.
static Status OpenSink(
    LogSink** result, const std::string& fname, Env* env,
    size_t buf_size = 0,  // Enable write buffering
    port::Mutex* io_mutex =
        NULL,  // Forces external synchronization among multiple threads
    std::vector<std::string*>* write_bufs =
        NULL,  // Facilitate the measurement of memory usage,
    WritableFileStats* io_stats = NULL  // Enable I/O monitoring
    ) {
  *result = NULL;
  WritableFile* base = NULL;
  Status status = env->NewWritableFile(fname, &base);
  if (status.ok()) {
    assert(base != NULL);
  } else {
    return status;
  }

  WritableFile* file;  // Bind to an external stats for monitoring
  if (io_stats != NULL) {
    file = new MeasuredWritableFile(io_stats, base);
  } else {
    file = base;
  }
  if (buf_size != 0) {
    UnsafeBufferedWritableFile* buffered =
        new UnsafeBufferedWritableFile(file, buf_size);
    write_bufs->push_back(buffered->buffer_store());
    file = buffered;
  } else {
    // No writer buffer?
  }

  PrintLogInfo(fname, buf_size);
  LogSink* sink = new LogSink(fname, file, io_mutex);
  sink->Ref();

  *result = sink;
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
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.name -> %s (mode=write)", name.c_str());
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.memtable_budget -> %s",
          PrettySize(options.total_memtable_budget).c_str());
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.key_size -> %s",
          PrettySize(options.key_size).c_str());
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.value_size -> %s",
          PrettySize(options.value_size).c_str());
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.bf_bits_per_key -> %d",
          int(options.bf_bits_per_key));
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.block_size -> %s",
          PrettySize(options.block_size).c_str());
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.block_util -> %.2f%%",
          100 * options.block_util);
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.data_buffer -> %s",
          PrettySize(options.data_buffer).c_str());
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.index_buffer -> %s",
          PrettySize(options.index_buffer).c_str());
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.tail_padding -> %d",
          int(options.tail_padding));
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.unique_keys -> %d",
          int(options.unique_keys));
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.num_parts_per_rank -> %u",
          static_cast<unsigned>(num_parts));
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.my_rank -> %d", my_rank);
#endif
  Status status;
  if (options.is_env_pfs) {
    // Ignore error since it may already exist
    env->CreateDir(name);
  }

  DirWriterImpl* impl = new DirWriterImpl(options);
  std::vector<DirLogger*> tmp_dirs(num_parts, NULL);
  std::vector<LogSink*> index(num_parts, NULL);
  std::vector<LogSink*> data(1, NULL);  // Shared among all partitions
  std::vector<const OutputStats*> compaction_stats;
  std::vector<std::string*> write_bufs;
  WritableFileStats* io_stats =
      options.measure_writes ? &impl->io_stats_ : NULL;
  status =
      OpenSink(&data[0], DataFileName(name, my_rank), env, options.data_buffer,
               &impl->io_mutex_, &write_bufs, io_stats);
  if (status.ok()) {
    port::Mutex* const mtx = NULL;  // No synchronization needed for index files
    for (size_t i = 0; i < num_parts; i++) {
      tmp_dirs[i] =
          new DirLogger(impl->options_, i, &impl->mutex_, &impl->cond_var_);
      WritableFileStats* internal_io_stats =
          options.measure_writes ? &tmp_dirs[i]->io_stats_ : NULL;
      status =
          OpenSink(&index[i], IndexFileName(name, my_rank, int(i)), env,
                   options.index_buffer, mtx, &write_bufs, internal_io_stats);
      tmp_dirs[i]->Ref();
      if (status.ok()) {
        tmp_dirs[i]->Open(data[0], index[i]);
        const OutputStats* os = tmp_dirs[i]->output_stats();
        compaction_stats.push_back(os);
      } else {
        break;
      }
    }
  }

  if (status.ok()) {
    DirLogger** dirs = new DirLogger*[num_parts];
    for (size_t i = 0; i < num_parts; i++) {
      assert(tmp_dirs[i] != NULL);
      dirs[i] = tmp_dirs[i];
      dirs[i]->Ref();
    }
    impl->data_ = data[0];
    impl->data_->Ref();
    impl->compaction_stats_.swap(compaction_stats);
    // Weak references to log buffers that must not be deleted
    // during the lifetime of this directory.
    impl->write_bufs_.swap(write_bufs);
    impl->part_mask_ = num_parts - 1;
    impl->num_parts_ = num_parts;
    impl->dirs_ = dirs;
    *result = impl;
  } else {
    delete impl;
  }

  for (size_t i = 0; i < num_parts; i++) {
    if (tmp_dirs[i] != NULL) tmp_dirs[i]->Unref();
    if (index[i] != NULL) index[i]->Unref();
    if (i < data.size()) {
      if (data[i] != NULL) {
        data[i]->Unref();
      }
    }
  }

  return status;
}

class DirReaderImpl : public DirReader {
 public:
  DirReaderImpl(const DirOptions& opts, const std::string& name);
  virtual ~DirReaderImpl();

  virtual Status ReadAll(const Slice& fid, std::string* dst, char* tmp,
                         size_t tmp_length);

  virtual IoStats GetIoStats() const;

 private:
  RandomAccessFileStats io_stats_;
  friend class DirReader;

  DirOptions options_;
  const std::string name_;
  uint32_t num_parts_;
  uint32_t part_mask_;

  mutable port::Mutex mutex_;
  port::CondVar cond_cv_;
  Dir** dirs_;  // Lazily initialized directory partitions
  LogSource* data_;
};

DirReaderImpl::DirReaderImpl(const DirOptions& opts, const std::string& name)
    : options_(opts),
      name_(name),
      num_parts_(0),
      part_mask_(~static_cast<uint32_t>(0)),
      cond_cv_(&mutex_),
      dirs_(NULL),
      data_(NULL) {}

DirReaderImpl::~DirReaderImpl() {
  MutexLock ml(&mutex_);
  for (size_t i = 0; i < num_parts_; i++) {
    if (dirs_[i] != NULL) {
      dirs_[i]->Unref();
    }
  }
  delete[] dirs_;
  if (data_ != NULL) {
    data_->Unref();
  }
}

static Status LoadSource(LogSource** result, const std::string& fname, Env* env,
                         size_t read_size = 8 << 20,
                         SequentialFileStats* stats = NULL) {
  *result = NULL;
  SequentialFile* base = NULL;
  uint64_t size = 0;
  Status status = env->NewSequentialFile(fname, &base);
  if (status.ok()) {
    status = env->GetFileSize(fname, &size);
    if (!status.ok()) {
      delete base;
    }
  }
  if (status.ok()) {
    SequentialFile* file;  // Bind to an external stats for monitoring
    if (stats != NULL) {
      file = new MeasuredSequentialFile(stats, base);
    } else {
      file = base;
    }
    WholeFileBufferedRandomAccessFile* buffered_file =
        new WholeFileBufferedRandomAccessFile(file, size, read_size);
    status = buffered_file->Load();
    if (status.ok()) {
      PrintLogInfo(fname, size);
      LogSource* src = new LogSource(buffered_file, size);
      src->Ref();

      *result = src;
    } else {
      delete buffered_file;
    }
  }

  return status;
}

static Status OpenSource(LogSource** result, const std::string& fname, Env* env,
                         RandomAccessFileStats* stats = NULL) {
  *result = NULL;
  RandomAccessFile* base = NULL;
  uint64_t size = 0;
  Status status = env->NewRandomAccessFile(fname, &base);
  if (status.ok()) {
    status = env->GetFileSize(fname, &size);
    if (!status.ok()) {
      delete base;
    }
  }
  if (status.ok()) {
    RandomAccessFile* file;  // Bind to an external stats for monitoring
    if (stats != NULL) {
      file = new MeasuredRandomAccessFile(stats, base);
    } else {
      file = base;
    }
    PrintLogInfo(fname, 0);
    LogSource* src = new LogSource(file, size);
    src->Ref();

    *result = src;
  }

  return status;
}

Status DirReaderImpl::ReadAll(const Slice& fid, std::string* dst, char* tmp,
                              size_t tmp_length) {
  Status status;
  uint32_t hash = Hash(fid.data(), fid.size(), 0);
  uint32_t part = hash & part_mask_;
  assert(part < num_parts_);
  MutexLock ml(&mutex_);
  if (dirs_[part] == NULL) {
    mutex_.Unlock();  // Unlock when load dir indexes
    LogSource* indx = NULL;
    Dir* dir = new Dir(options_, &mutex_, &cond_cv_);
    dir->Ref();
    SequentialFileStats* io_stats =
        options_.measure_reads ? &dir->io_stats_ : NULL;
    const size_t io_size = options_.read_size;
    status = LoadSource(&indx, IndexFileName(name_, options_.rank, part),
                        options_.env, io_size, io_stats);
    if (status.ok()) {
      status = dir->Open(indx);
    }
    mutex_.Lock();
    if (status.ok()) {
      dir->RebindDataSource(data_);
      if (dirs_[part] != NULL) {
        dirs_[part]->Unref();
      }
      dirs_[part] = dir;
      dirs_[part]->Ref();
    }
    dir->Unref();
    if (indx != NULL) {
      indx->Unref();
    }
  }

  if (status.ok()) {
    assert(dirs_[part] != NULL);
    Dir* const dir = dirs_[part];
    dir->Ref();
    status = dirs_[part]->Read(fid, dst, tmp, tmp_length);
    dir->Unref();
    return status;
  } else {
    return status;
  }
}

IoStats DirReaderImpl::GetIoStats() const {
  MutexLock ml(&mutex_);
  IoStats result;
  for (size_t i = 0; i < num_parts_; i++) {
    if (dirs_[i] != NULL) {
      result.index_bytes += dirs_[i]->io_stats_.TotalBytes();
      result.index_ops += dirs_[i]->io_stats_.TotalOps();
    }
  }
  result.data_bytes = io_stats_.TotalBytes();
  result.data_ops = io_stats_.TotalOps();
  return result;
}

DirReader::~DirReader() {}

static DirOptions SanitizeReadOptions(const DirOptions& options) {
  DirOptions result = options;
  if (result.env == NULL) {
    result.env = Env::Default();
  }
  return result;
}

Status DirReader::Open(const DirOptions& opts, const std::string& name,
                       DirReader** result) {
  *result = NULL;
  DirOptions options = SanitizeReadOptions(opts);
  const uint32_t num_parts = 1u << options.lg_parts;
  const int my_rank = options.rank;
  Env* const env = options.env;
  LogSource* data = NULL;
  Status status;
#if VERBOSE >= 2
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.name -> %s (mode=read)", name.c_str());
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.read_size -> %s",
          PrettySize(options.read_size).c_str());
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.paranoid_checks -> %d",
          int(options.paranoid_checks));
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.verify_checksums -> %d",
          int(options.verify_checksums));
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.measure_reads -> %d",
          int(options.measure_reads));
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.ignore_filters -> %d",
          int(options.ignore_filters));
  Verbose(__LOG_ARGS__, 2, "FS: plfsdir.my_rank -> %d", my_rank);
#endif
  DirReaderImpl* impl = new DirReaderImpl(options, name);
  if (options.measure_reads) {
    status =
        OpenSource(&data, DataFileName(name, my_rank), env, &impl->io_stats_);
  } else {
    status = OpenSource(&data, DataFileName(name, my_rank), env);
  }
  char space[Footer::kEncodedLength];
  Footer footer;
  Slice input;
  if (!status.ok()) {
    // Error
  } else if (data->Size() < sizeof(space)) {
    status = Status::Corruption("Dir data too short to be valid");
  } else if (options.paranoid_checks) {
    status =
        data->Read(data->Size() - sizeof(space), sizeof(space), &input, space);
    if (status.ok()) {
      status = footer.DecodeFrom(&input);
    }
  }

  // Update options
  if (options.paranoid_checks) {
    if (status.ok()) {
      impl->options_.lg_parts = static_cast<int>(footer.lg_parts());
      impl->options_.skip_checksums =
          static_cast<bool>(footer.skip_checksums());
      impl->options_.unique_keys = static_cast<bool>(footer.unique_keys());
    }
  }

  if (status.ok()) {
    impl->dirs_ = new Dir*[num_parts]();
    impl->part_mask_ = num_parts - 1;
    impl->num_parts_ = num_parts;
    impl->data_ = data;
    impl->data_->Ref();

    *result = impl;
  } else {
    delete impl;
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
  if (options.is_env_pfs) {
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
  } else {
    const size_t num_parts = 1u << options.lg_parts;
    const int my_rank = options.rank;
    std::vector<std::string> names;
    names.push_back(DataFileName(dirname, my_rank));
    for (size_t part = 0; part < num_parts; part++) {
      names.push_back(IndexFileName(dirname, my_rank, part));
    }
    if (status.ok()) {
      for (size_t i = 0; i < names.size(); i++) {
        status = DeleteLogStream(names[i], env);
        if (!status.ok()) {
          break;
        }
      }
    }
  }
  return status;
}

}  // namespace plfsio
}  // namespace pdlfs
