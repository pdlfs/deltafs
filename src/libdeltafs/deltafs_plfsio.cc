/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio.h"
#include "deltafs_plfsio_batch.h"
#include "deltafs_plfsio_filter.h"
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
    : total_memtable_budget(4 << 20),
      memtable_util(0.97),
      memtable_reserv(1.00),
      leveldb_compatible(true),
      skip_sort(false),
      fixed_kv_length(false),
      key_size(8),
      value_size(32),
      filter(kFtBloomFilter),
      filter_bits_per_key(0),
      bf_bits_per_key(8),
      bm_fmt(kFmtUncompressed),
      bm_key_bits(24),
      cuckoo_seed(301),
      cuckoo_max_moves(500),
      cuckoo_frac(0.95),
      block_size(32 << 10),
      block_util(0.996),
      block_padding(true),
      block_batch_size(2 << 20),
      data_buffer(4 << 20),
      min_data_buffer(4 << 20),
      index_buffer(4 << 20),
      min_index_buffer(4 << 20),
      epoch_log_rotation(false),
      tail_padding(false),
      compaction_pool(NULL),
      reader_pool(NULL),
      read_size(8 << 20),
      parallel_reads(false),
      paranoid_checks(false),
      ignore_filters(false),
      compression(kNoCompression),
      index_compression(kNoCompression),
      force_compression(false),
      verify_checksums(false),
      skip_checksums(false),
      measure_reads(true),
      measure_writes(true),
      num_epochs(-1),
      lg_parts(-1),
      listener(NULL),
      mode(kDmUniqueKey),
      env(NULL),
      allow_env_threads(false),
      is_env_pfs(true),
      rank(0) {}

namespace {

bool ParseBool(const Slice& key, const Slice& value, bool* result) {
  if (!ParsePrettyBool(value, result)) {
    Warn(__LOG_ARGS__, "Unknown dir option: %s=%s, option ignored", key.c_str(),
         value.c_str());
    return false;
  } else {
    return true;
  }
}

bool ParseInteger(const Slice& key, const Slice& value, uint64_t* result) {
  if (!ParsePrettyNumber(value, result)) {
    Warn(__LOG_ARGS__, "Unknown dir option: %s=%s, option ignored", key.c_str(),
         value.c_str());
    return false;
  } else {
    return true;
  }
}

bool ParseBitmapFormat(const Slice& key, const Slice& value,
                       BitmapFormat* result) {
  if (value == "uncompressed") {
    *result = kFmtUncompressed;
    return true;
  } else if (value == "roar") {
    *result = kFmtRoaring;
    return true;
  } else if (value == "fast-vb+") {
    *result = kFmtFastVarintPlus;
    return true;
  } else if (value == "vb+") {
    *result = kFmtVarintPlus;
    return true;
  } else if (value == "vb") {
    *result = kFmtVarint;
    return true;
  } else if (value == "fast-p-f-delta") {
    *result = kFmtFastPfDelta;
    return true;
  } else if (value == "p-f-delta") {
    *result = kFmtPfDelta;
    return true;
  } else {
    Warn(__LOG_ARGS__, "Unknown bitmap format: %s=%s, option ignored",
         key.c_str(), value.c_str());
    return false;
  }
}

bool ParseFilterType(const Slice& key, const Slice& value, FilterType* result) {
  if (value.starts_with("bloom")) {
    *result = kFtBloomFilter;
    return true;
  } else if (value.starts_with("bitmap")) {
    *result = kFtBitmap;
    return true;
  } else {
    Warn(__LOG_ARGS__, "Unknown filter type: %s=%s, option ignored",
         key.c_str(), value.c_str());
    return false;
  }
}

bool ParseCompressionType(const Slice& key, const Slice& value,
                          CompressionType* result) {
  if (value.starts_with("snappy")) {
    *result = kSnappyCompression;
    return true;
  } else if (value.starts_with("no")) {
    *result = kNoCompression;
    return true;
  } else {
    Warn(__LOG_ARGS__, "Unknown compression type: %s=%s, option ignored",
         key.c_str(), value.c_str());
    return false;
  }
}

}  // namespace

DirOptions ParseDirOptions(const char* input) {
  DirOptions result;
  std::vector<std::string> conf_segments;
  size_t n = SplitString(&conf_segments, input, '&');  // k1=v1 & k2=v2 & k3=v3
  std::vector<std::string> conf_pair;
  for (size_t i = 0; i < n; i++) {
    conf_pair.resize(0);
    SplitString(&conf_pair, conf_segments[i].c_str(), '=', 1);
    if (conf_pair.size() != 2) {
      continue;
    }
    FilterType filter_type;
    BitmapFormat bm_fmt;
    CompressionType compression_type;
    Slice conf_key = conf_pair[0];
    Slice conf_value = conf_pair[1];
    uint64_t num;
    bool flag;
    if (conf_key == "lg_parts") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.lg_parts = int(num);
      }
    } else if (conf_key == "num_epochs") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.num_epochs = int(num);
      }
    } else if (conf_key == "rank") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.rank = int(num);
      }
    } else if (conf_key == "memtable_size") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.total_memtable_budget = num;
      }
    } else if (conf_key == "total_memtable_budget") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.total_memtable_budget = num;
      }
    } else if (conf_key == "compaction_buffer") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.block_batch_size = num;
      }
    } else if (conf_key == "data_buffer") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.data_buffer = num;
      }
    } else if (conf_key == "min_data_buffer") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.min_data_buffer = num;
      }
    } else if (conf_key == "index_buffer") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.index_buffer = num;
      }
    } else if (conf_key == "min_index_buffer") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.min_index_buffer = num;
      }
    } else if (conf_key == "block_size") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.block_size = num;
      }
    } else if (conf_key == "block_padding") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.block_padding = flag;
      }
    } else if (conf_key == "tail_padding") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.tail_padding = flag;
      }
    } else if (conf_key == "verify_checksums") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.verify_checksums = flag;
      }
    } else if (conf_key == "skip_checksums") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.skip_checksums = flag;
      }
    } else if (conf_key == "skip_sort") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.skip_sort = flag;
      }
    } else if (conf_key == "parallel_reads") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.parallel_reads = flag;
      }
    } else if (conf_key == "paranoid_checks") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.paranoid_checks = flag;
      }
    } else if (conf_key == "epoch_log_rotation") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.epoch_log_rotation = flag;
      }
    } else if (conf_key == "ignore_filters") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.ignore_filters = flag;
      }
    } else if (conf_key == "fixed_kv") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.fixed_kv_length = flag;
      }
    } else if (conf_key == "leveldb_compatible") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.leveldb_compatible = flag;
      }
    } else if (conf_key == "filter") {
      if (ParseFilterType(conf_key, conf_value, &filter_type)) {
        result.filter = filter_type;
      }
    } else if (conf_key == "filter_bits_per_key") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.filter_bits_per_key = num;
      }
    } else if (conf_key == "bf_bits_per_key") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.bf_bits_per_key = num;
      }
    } else if (conf_key == "bm_fmt") {
      if (ParseBitmapFormat(conf_key, conf_value, &bm_fmt)) {
        result.bm_fmt = bm_fmt;
      }
    } else if (conf_key == "bm_key_bits") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.bm_key_bits = num;
      }
    } else if (conf_key == "compression") {
      if (ParseCompressionType(conf_key, conf_value, &compression_type)) {
        result.compression = compression_type;
      }
    } else if (conf_key == "index_compression") {
      if (ParseCompressionType(conf_key, conf_value, &compression_type)) {
        result.index_compression = compression_type;
      }
    } else if (conf_key == "force_compression") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.force_compression = flag;
      }
    } else if (conf_key == "value_size") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.value_size = num;
      }
    } else if (conf_key == "key_size") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.key_size = num;
      }
    } else {
      Warn(__LOG_ARGS__, "Unknown option key: %s, option ignored",
           conf_key.c_str());
    }
  }

  return result;
}

struct DirWriter::Rep {
 public:
  Rep(const DirOptions& opts, const std::string& dirname);
  ~Rep();

  bool HasCompaction();
  Status ObtainCompactionStatus();
  Status WaitForCompaction();
  Status MaybeRotateLogs(Epoch*);
  Status TryFlush(Epoch*, bool ef = false, bool fi = false);
  Status TryBatchWrites(Epoch*, BatchCursor* cursor);
  Status TryAdd(Epoch*, const Slice& fid, const Slice& data);
  Status EnsureDataPadding(LogSink* sink, size_t footer_size);
  Status InstallDirInfo(const std::string& footer);
  Status Finalize();

  const DirOptions options_;
  mutable port::Mutex io_mutex_;  // Protecting the shared data log
  mutable port::Mutex mutex_;
  port::CondVar bg_cv_;
  port::CondVar cv_;
  const std::string dirname_;
  uint32_t num_parts_;
  uint32_t part_mask_;
  Status finish_status_;
  Epoch* epoch_;   // Current epoch
  bool finished_;  // If Finish() has been called
  WritableFileStats io_stats_;
  const DirOutputStats** compac_stats_;
  DirIndexer** idxers_;
  LogSink* data_;
  Env* env_;
};

DirWriter::Rep::Rep(const DirOptions& o, const std::string& d)
    : options_(o),
      bg_cv_(&mutex_),
      cv_(&mutex_),
      dirname_(d),
      num_parts_(0),
      part_mask_(~static_cast<uint32_t>(0)),
      epoch_(NULL),
      finished_(false),
      compac_stats_(NULL),
      idxers_(NULL),
      data_(NULL),
      env_(options_.env) {
  epoch_ = new Epoch(0, &mutex_);
  epoch_->Ref();
}

DirWriter::Rep::~Rep() {
  MutexLock l(&mutex_);
  for (size_t i = 0; i < num_parts_; i++) {
    if (idxers_[i] != NULL) {
      idxers_[i]->Unref();
    }
  }
  if (epoch_ != NULL) epoch_->Unref();
  delete[] compac_stats_;
  delete[] idxers_;
  if (data_ != NULL) {
    data_->Unref();
  }
}

Status DirWriter::Rep::EnsureDataPadding(LogSink* sink, size_t footer_size) {
  Status status;
  sink->Lock();  // Ltell() and Lwrite() must go as an atomic operation
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

Status DirWriter::Rep::MaybeRotateLogs(Epoch* ep) {
  Status status;
  mutex_.AssertHeld();
  if (!options_.epoch_log_rotation) {
    return status;
  }
  // TODO: Rotate logs without waiting for compactions
  status = WaitForCompaction();
  if (status.ok()) {
    mutex_.Unlock();  // Unlock when rotating logs
    data_->Lock();
    // Prepare for the next epoch
    status = data_->Lrotate(1 + ep->seq_);
    data_->Unlock();
    mutex_.Lock();
  }

  return status;
}

Status DirWriter::Rep::InstallDirInfo(const std::string& footer) {
  WritableFile* file;
  const std::string fname = DirInfoFileName(dirname_);
  Status status = env_->NewWritableFile(fname.c_str(), &file);
  if (!status.ok()) {
    return status;
  }

  Slice contents = footer;
  std::string buf;
  // Add enough padding to ensure the final size of the footer file
  // is some multiple of the physical write size.
  if (options_.tail_padding) {
    const size_t footer_size = footer.size();
    const size_t overflow = footer_size % options_.data_buffer;
    if (overflow != 0) {
      buf.resize(options_.data_buffer - overflow, 0);
      buf += footer;
      contents = buf;
    } else {
      // No need to pad
    }
  }
  status = file->Append(contents);
  if (status.ok()) {
    status = file->Sync();
  }
  if (status.ok()) {
    status = file->Close();
  }

  // Will auto-close if we did not close above
  delete file;
  if (!status.ok()) {
    env_->DeleteFile(fname.c_str());
  }
  return status;
}

// REQUIRES: mutex_ has been locked and no on-going compactions.
Status DirWriter::Rep::Finalize() {
  mutex_.AssertHeld();
  assert(!HasCompaction());
  uint32_t max_epochs = 0;
  for (uint32_t i = 0; i < num_parts_; i++)
    max_epochs = std::max(max_epochs, idxers_[i]->num_epochs());
  Footer footer = Mkfoot(options_);
  BlockHandle dummy_handle;

  dummy_handle.set_offset(0);
  dummy_handle.set_size(0);
  footer.set_epoch_index_handle(dummy_handle);
  footer.set_num_epochs(max_epochs);

  Status status;
  std::string ftdata;
  footer.EncodeTo(&ftdata);
  if (options_.tail_padding) {
    status = EnsureDataPadding(data_, ftdata.size());
  }

  if (status.ok()) {
    data_->Lock();
    status = data_->Lwrite(ftdata);
    data_->Unlock();
  }

  if (status.ok()) {
    for (uint32_t i = 0; i < num_parts_; i++) {
      status = idxers_[i]->SyncAndClose();
      if (!status.ok()) {
        break;
      }
    }
  }

  // Write out our primary footer copy
  if (status.ok()) {
    if (options_.rank == 0) {  // Rank 0 does the writing
      status = InstallDirInfo(ftdata);
    }
  }

  return status;
}

Status DirWriter::Rep::ObtainCompactionStatus() {
  mutex_.AssertHeld();
  Status status;
  for (size_t i = 0; i < num_parts_; i++) {
    status = idxers_[i]->bg_status();
    if (!status.ok()) {
      break;
    }
  }
  return status;
}

bool DirWriter::Rep::HasCompaction() {
  mutex_.AssertHeld();
  for (size_t i = 0; i < num_parts_; i++) {
    if (idxers_[i]->has_bg_compaction()) {
      return true;
    }
  }
  return false;
}

Status DirWriter::Rep::WaitForCompaction() {
  mutex_.AssertHeld();
  Status status;
  while (true) {
    status = ObtainCompactionStatus();
    if (status.ok() && HasCompaction()) {
      bg_cv_.Wait();
    } else {
      break;
    }
  }
  return status;
}

Status DirWriter::Rep::TryBatchWrites(Epoch* ep, BatchCursor* cursor) {
  mutex_.AssertHeld();
  assert(ep->num_ongoing_ops_ != 0);
  Status status;
  std::vector<std::vector<uint32_t> > waiting_list(num_parts_);
  std::vector<std::vector<uint32_t> > queue(num_parts_);
  bool has_more = false;  // If the entire batch is done
  cursor->Seek(0);
  for (; cursor->Valid(); cursor->Next()) {
    Slice fid = cursor->fid();
    const uint32_t hash = Hash(fid.data(), fid.size(), 0);
    const uint32_t part = hash & part_mask_;
    assert(part < num_parts_);
    status = idxers_[part]->Add(ep, fid, cursor->data());

    if (status.IsBufferFull()) {
      // Try again later
      waiting_list[part].push_back(cursor->offset());
      status = Status::OK();
      has_more = true;
    } else if (!status.ok()) {
      break;
    } else {
      // OK
    }
  }

  while (status.ok()) {
    if (has_more) {
      has_more = false;
      bg_cv_.Wait();
    } else {
      break;
    }
    for (size_t i = 0; i < num_parts_; i++) {
      waiting_list[i].swap(queue[i]);
      waiting_list[i].clear();
    }
    for (size_t i = 0; i < num_parts_; i++) {
      if (!queue[i].empty()) {
        std::vector<uint32_t>::iterator it = queue[i].begin();
        for (; it != queue[i].end(); ++it) {
          cursor->Seek(*it);
          if (cursor->Valid()) {
            status = idxers_[i]->Add(ep, cursor->fid(), cursor->data());
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
  }

  return status;
}

// Attempt to schedule a minor compaction on all directory partitions
// simultaneously. If a compaction cannot be scheduled immediately due to a lack
// of buffer space, it will be added to a waiting list so it can be reattempted
// later. Return immediately as soon as all partitions have a minor compaction
// scheduled. Will not wait for all compactions to finish.
// Return OK on success, or a non-OK status on errors.
Status DirWriter::Rep::TryFlush(Epoch* ep, bool ef, bool fi) {
  mutex_.AssertHeld();
  if (ef || fi) {
    assert(ep->committing_ && ep->num_ongoing_ops_ == 0);
  } else {
    assert(ep->num_ongoing_ops_ != 0);
  }
  Status status;
  std::vector<DirIndexer*> remaining;
  for (size_t i = 0; i < num_parts_; i++) remaining.push_back(idxers_[i]);
  std::vector<DirIndexer*> waiting_list;

  DirIndexer::FlushOptions flush_options(ef, fi);
  while (!remaining.empty()) {
    waiting_list.clear();
    for (size_t i = 0; i < remaining.size(); i++) {
      flush_options.dry_run =
          true;  // Avoid being blocked waiting for buffer space to reappear
      status = remaining[i]->Flush(flush_options, ep);
      flush_options.dry_run = false;

      if (status.IsBufferFull()) {
        waiting_list.push_back(remaining[i]);  // Try again later
        status = Status::OK();
      } else if (status.ok()) {
        remaining[i]->Flush(flush_options, ep);
      } else {
        break;
      }
    }

    if (status.ok()) {
      if (!waiting_list.empty()) {
        bg_cv_.Wait();  // Waiting for buffer space
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
Status DirWriter::Finish() {
  Status status;
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  while (true) {
    if (r->finished_) {
      status = r->finish_status_;  // Return the stored result
      break;
    }
    Epoch* const cur = r->epoch_;
    assert(cur != NULL);
    if (cur->committing_) {
      r->cv_.Wait();
    } else {
      cur->committing_ = true;
      while (cur->num_ongoing_ops_ != 0) {
        cur->cv_.Wait();
      }
      status = r->TryFlush(cur, true /*epoch flush*/, true /*finalize*/);
      if (status.ok()) status = r->WaitForCompaction();
      if (status.ok()) status = r->Finalize();
      r->finish_status_ = status;
      r->finished_ = true;
      r->epoch_ = NULL;
      r->cv_.SignalAll();
      cur->Unref();
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
Status DirWriter::EpochFlush(int epoch) {
  Status status;
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  while (true) {
    if (r->finished_) {
      status = Status::AssertionFailed("Plfsdir already finished");
      break;
    }
    Epoch* const cur = r->epoch_;
    assert(cur != NULL);
    if (epoch == -1 && cur->committing_) {
      r->cv_.Wait();  // Miss the boat. Gotta wait
    } else if (epoch != -1 && epoch != int(cur->seq_)) {
      status = Status::AssertionFailed("Bad epoch num");
      break;
    } else if (cur->committing_) {
      status = Status::AssertionFailed("Epoch is being flushed");
      break;
    } else {
      cur->committing_ = true;  // No more writing
      while (cur->num_ongoing_ops_ != 0) {
        cur->cv_.Wait();
      }
      status = r->TryFlush(cur, true /*epoch flush*/);
      if (status.ok())
        status = r->MaybeRotateLogs(cur);  // May temporarily unlock
      Epoch* const nxt = new Epoch(1 + cur->seq_, &r->mutex_);
      assert(r->epoch_ == cur);
      r->epoch_ = nxt;
      r->cv_.SignalAll();
      cur->Unref();
      nxt->Ref();
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
Status DirWriter::Flush(int epoch) {
  Status status;
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  while (true) {
    if (r->finished_) {
      status = Status::AssertionFailed("Plfsdir already finished");
      break;
    }
    Epoch* const cur = r->epoch_;
    assert(cur != NULL);
    if (epoch == -1 && cur->committing_) {
      r->cv_.Wait();
    } else if (epoch != -1 && epoch != int(cur->seq_)) {
      status = Status::AssertionFailed("Bad epoch num");
      break;
    } else if (cur->committing_) {
      status = Status::AssertionFailed("Epoch is being flushed");
      break;
    } else {
      cur->num_ongoing_ops_++;
      status = r->TryFlush(cur);
      assert(cur->num_ongoing_ops_ != 0);
      cur->num_ongoing_ops_--;
      if (cur->committing_ && cur->num_ongoing_ops_ == 0) {
        cur->cv_.SignalAll();
      }
      break;
    }
  }
  return status;
}

Status DirWriter::Write(BatchCursor* cursor, int epoch) {
  Status status;
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  while (true) {
    if (r->finished_) {
      status = Status::AssertionFailed("Plfsdir already finished");
      break;
    }
    Epoch* const cur = r->epoch_;
    assert(cur != NULL);
    if (epoch == -1 && cur->committing_) {
      r->cv_.Wait();
    } else if (epoch != -1 && epoch != int(cur->seq_)) {
      status = Status::AssertionFailed("Bad epoch num");
      break;
    } else if (cur->committing_) {
      status = Status::AssertionFailed("Epoch is being flushed");
      break;
    } else {
      cur->num_ongoing_ops_++;
      status = r->TryBatchWrites(cur, cursor);
      assert(cur->num_ongoing_ops_ != 0);
      cur->num_ongoing_ops_--;
      if (cur->committing_ && cur->num_ongoing_ops_ == 0) {
        cur->cv_.SignalAll();
      }
      break;
    }
  }
  return status;
}

Status DirWriter::Add(const Slice& fid, const Slice& data, int epoch) {
  Status status;
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  while (true) {
    if (r->finished_) {
      status = Status::AssertionFailed("Plfsdir already finished");
      break;
    }
    Epoch* const cur = r->epoch_;
    assert(cur != NULL);
    if (epoch == -1 && cur->committing_) {
      r->cv_.Wait();
    } else if (epoch != -1 && epoch != int(cur->seq_)) {
      status = Status::AssertionFailed("Bad epoch num");
      break;
    } else if (cur->committing_) {
      status = Status::AssertionFailed("Epoch is being flushed");
      break;
    } else {
      cur->num_ongoing_ops_++;
      status = r->TryAdd(cur, fid, data);
      assert(cur->num_ongoing_ops_ != 0);
      cur->num_ongoing_ops_--;
      if (cur->committing_ && cur->num_ongoing_ops_ == 0) {
        cur->cv_.SignalAll();
      }
      break;
    }
  }
  return status;
}

Status DirWriter::Rep::TryAdd(Epoch* ep, const Slice& fid, const Slice& data) {
  mutex_.AssertHeld();
  Status status;
  const uint32_t hash = Hash(fid.data(), fid.size(), 0);
  const uint32_t part = hash & part_mask_;
  assert(part < num_parts_);
  status = idxers_[part]->Add(ep, fid, data);
  return status;
}

// Wait for all on-going compactions to finish.
// Return OK on success, or a non-OK status on errors.
Status DirWriter::Wait() {
  Status status;
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  if (!r->finished_) return r->WaitForCompaction();
  return r->finish_status_;
}

// Sync storage I/O.
// Return OK on success, or a non-OK status on errors.
Status DirWriter::Sync() {
  Status status;
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  if (r->finished_) return r->finish_status_;
  status = r->WaitForCompaction();
  if (!status.ok()) return status;
  LogSink* const sink = r->data_;
  sink->Lock();
  status = sink->Lsync();
  sink->Unlock();
  if (status.ok()) {
    for (uint32_t part = 0; part < r->num_parts_; part++) {
      status = r->idxers_[part]->indx_->Lsync();
      if (!status.ok()) {
        break;
      }
    }
  }
  return status;
}

IoStats DirWriter::GetIoStats() const {
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  IoStats result;
  for (size_t i = 0; i < r->num_parts_; i++) {
    result.index_bytes += r->idxers_[i]->io_stats_.TotalBytes();
    result.index_ops += r->idxers_[i]->io_stats_.TotalOps();
  }
  result.data_bytes = r->io_stats_.TotalBytes();
  result.data_ops = r->io_stats_.TotalOps();
  return result;
}

uint64_t DirWriter::TEST_estimated_sstable_size() const {
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  if (r->num_parts_ != 0) {
    return r->idxers_[0]->estimated_sstable_size();
  } else {
    return 0;
  }
}

uint64_t DirWriter::TEST_planned_filter_size() const {
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  if (r->num_parts_ != 0) {
    return r->idxers_[0]->planned_filter_size();
  } else {
    return 0;
  }
}

uint32_t DirWriter::TEST_num_keys() const {
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  uint32_t result = 0;
  for (size_t i = 0; i < r->num_parts_; i++) {
    result += r->compac_stats_[i]->total_num_keys_;
  }
  return result;
}

uint32_t DirWriter::TEST_num_dropped_keys() const {
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  uint32_t result = 0;
  for (size_t i = 0; i < r->num_parts_; i++) {
    result += r->compac_stats_[i]->total_num_dropped_keys_;
  }
  return result;
}

uint32_t DirWriter::TEST_num_data_blocks() const {
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  uint32_t result = 0;
  for (size_t i = 0; i < r->num_parts_; i++) {
    result += r->compac_stats_[i]->total_num_blocks_;
  }
  return result;
}

uint32_t DirWriter::TEST_num_sstables() const {
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  uint32_t result = 0;
  for (size_t i = 0; i < r->num_parts_; i++) {
    result += r->compac_stats_[i]->total_num_tables_;
  }
  return result;
}

uint64_t DirWriter::TEST_total_memory_usage() const {
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  uint64_t result = 0;
  result += r->data_->memory_usage();
  for (size_t i = 0; i < r->num_parts_; i++)
    result += r->idxers_[i]->indx_->memory_usage();
  for (size_t i = 0; i < r->num_parts_; i++)
    result += r->idxers_[i]->memory_usage();
  return result;
}

uint64_t DirWriter::TEST_raw_index_contents() const {
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  uint64_t result = 0;
  for (size_t i = 0; i < r->num_parts_; i++) {
    result += r->compac_stats_[i]->index_size;
  }
  return result;
}

uint64_t DirWriter::TEST_raw_filter_contents() const {
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  uint64_t result = 0;
  for (size_t i = 0; i < r->num_parts_; i++) {
    result += r->compac_stats_[i]->filter_size;
  }
  return result;
}

uint64_t DirWriter::TEST_raw_data_contents() const {
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  uint64_t result = 0;
  for (size_t i = 0; i < r->num_parts_; i++) {
    result += r->compac_stats_[i]->data_size;
  }
  return result;
}

uint64_t DirWriter::TEST_value_bytes() const {
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  uint64_t result = 0;
  for (size_t i = 0; i < r->num_parts_; i++) {
    result += r->compac_stats_[i]->value_size;
  }
  return result;
}

uint64_t DirWriter::TEST_key_bytes() const {
  Rep* const r = rep_;
  MutexLock ml(&r->mutex_);
  uint64_t result = 0;
  for (size_t i = 0; i < r->num_parts_; i++) {
    result += r->compac_stats_[i]->key_size;
  }
  return result;
}

DirWriter::DirWriter(Rep* rep) : rep_(rep) {}

DirWriter::~DirWriter() { delete rep_; }

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
  ClipToRange(&result.total_memtable_budget, 1 << 20, 1 << 30);
  ClipToRange(&result.memtable_util, 0.5, 1.0);
  ClipToRange(&result.block_size, 1 << 10, 1 << 20);
  ClipToRange(&result.block_util, 0.5, 1.0);
  ClipToRange(&result.lg_parts, 0, 8);
  if (result.index_buffer < result.min_index_buffer) {
    result.index_buffer = result.min_index_buffer;
  }
  if (result.data_buffer < result.min_data_buffer) {
    result.data_buffer = result.min_data_buffer;
  }
  if (result.env == NULL) {
    result.env = Env::Default();
  }
  return result;
}

#if VERBOSE >= 3
namespace {
// Return a brief summary of the configuration of a bitmap filter
std::string BitmapFilterOptions(const DirOptions& options) {
  assert(options.filter == kFtBitmap);
  char tmp[50];
  switch (options.bm_fmt) {
    case kFmtUncompressed:
      snprintf(tmp, sizeof(tmp), "BMP (uncompressed, key_bits=%d)",
               int(options.bm_key_bits));
      return tmp;
    case kFmtRoaring:
      snprintf(tmp, sizeof(tmp), "BMP (roar, key_bits=%d)",
               int(options.bm_key_bits));
      return tmp;
    case kFmtFastVarintPlus:
      snprintf(tmp, sizeof(tmp), "BMP (fast vb+, key_bits=%d)",
               int(options.bm_key_bits));
      return tmp;
    case kFmtVarintPlus:
      snprintf(tmp, sizeof(tmp), "BMP (vb+, key_bits=%d)",
               int(options.bm_key_bits));
      return tmp;
    case kFmtVarint:
      snprintf(tmp, sizeof(tmp), "BMP (vb, key_bits=%d)",
               int(options.bm_key_bits));
      return tmp;
    case kFmtFastPfDelta:
      snprintf(tmp, sizeof(tmp), "BMP (fast p-f-delta, key_bits=%d)",
               int(options.bm_key_bits));
      return tmp;
    case kFmtPfDelta:
      snprintf(tmp, sizeof(tmp), "BMP (p-f-delta, key_bits=%d)",
               int(options.bm_key_bits));
      return tmp;
    default:
      snprintf(tmp, sizeof(tmp), "BMP (others)");
      return tmp;
  }
}

// Return a brief summary of filter configuration.
std::string FilterOptions(const DirOptions& options) {
  char tmp[50];
  switch (options.filter) {
    case kFtBitmap:
      return BitmapFilterOptions(options);
    case kFtBloomFilter:
      snprintf(tmp, sizeof(tmp), "BF (bits_per_key=%d)",
               int(options.bf_bits_per_key));
      return tmp;
    case kFtNoFilter:
      return "Dis";
    default:
      return "Unk";
  }
}
}  // namespace
#endif

Status DirWriter::TryOpen(Rep* rep) {
  Status status;
  assert(rep != NULL);
  const DirOptions* const options = &rep->options_;
  const uint32_t num_parts = static_cast<uint32_t>(1 << options->lg_parts);
  const int my_rank = options->rank;
  Env* const env = options->env;
  if (options->is_env_pfs) {
    // Ignore error since it may already exist
    env->CreateDir(rep->dirname_.c_str());
  }

  std::vector<DirIndexer*> diridxers(num_parts, NULL);
  std::vector<LogSink*> index(num_parts, NULL);
  std::vector<LogSink*> data(1, NULL);  // Shared among all partitions
  std::vector<const DirOutputStats*> output_stats;
  LogSink::LogOptions io_opts;
  io_opts.rank = my_rank;
  io_opts.type = kDefIoType;
  if (options->epoch_log_rotation) io_opts.rotation = kRotationExtCtrl;
  if (options->measure_writes) io_opts.stats = &rep->io_stats_;
  io_opts.mu = &rep->io_mutex_;
  io_opts.min_buf = options->min_data_buffer;
  io_opts.max_buf = options->data_buffer;
  io_opts.env = env;
  status = LogSink::Open(io_opts, rep->dirname_, &data[0]);
  if (status.ok()) {
    for (size_t i = 0; i < num_parts; i++) {
      diridxers[i] =
          new DirIndexer(rep->options_, i, &rep->mutex_, &rep->bg_cv_);
      LogSink::LogOptions idx_opts;
      idx_opts.rank = my_rank;
      idx_opts.sub_partition = static_cast<int>(i);
      idx_opts.type = kIdxIoType;
      if (options->measure_writes) idx_opts.stats = &diridxers[i]->io_stats_;
      idx_opts.mu = NULL;
      idx_opts.min_buf = options->min_index_buffer;
      idx_opts.max_buf = options->index_buffer;
      idx_opts.env = env;
      status = LogSink::Open(idx_opts, rep->dirname_, &index[i]);
      diridxers[i]->Ref();
      if (status.ok()) {
        diridxers[i]->Open(data[0], index[i]);
        const DirOutputStats* os = &diridxers[i]->compac_stats_;
        output_stats.push_back(os);
      } else {
        break;
      }
    }
  }

  if (status.ok()) {
    const DirOutputStats** compac_stats = new const DirOutputStats*[num_parts];
    DirIndexer** idxers = new DirIndexer*[num_parts];
    for (size_t i = 0; i < num_parts; i++) {
      assert(diridxers[i] != NULL);
      compac_stats[i] = output_stats[i];
      idxers[i] = diridxers[i];
      idxers[i]->Ref();
    }
    rep->data_ = data[0];
    rep->data_->Ref();
    rep->compac_stats_ = compac_stats;
    rep->part_mask_ = num_parts - 1;
    rep->num_parts_ = num_parts;
    rep->idxers_ = idxers;
  }

  for (size_t i = 0; i < num_parts; i++) {
    if (diridxers[i] != NULL) diridxers[i]->Unref();
    if (index[i] != NULL) index[i]->Unref();
    if (i < data.size()) {
      if (data[i] != NULL) {
        data[i]->Unref();
      }
    }
  }

  return status;
}

Status DirWriter::Open(const DirOptions& _opts, const std::string& dirname,
                       DirWriter** result) {
  *result = NULL;
  DirOptions options = SanitizeWriteOptions(_opts);
#if VERBOSE >= 2
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.name -> %s (mode=write)",
          dirname.c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.memtable_budget -> %s",
          PrettySize(options.total_memtable_budget).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.memtable_util -> %.2f%%",
          100 * options.memtable_util);
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.memtable_reserv -> %.2f%%",
          100 * options.memtable_reserv);
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.leveldb_compatible -> %s",
          int(options.leveldb_compatible) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.skip_sort -> %s",
          int(options.skip_sort) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.fixed_kv_length -> %s",
          int(options.fixed_kv_length) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.key_size -> %s",
          PrettySize(options.key_size).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.value_size -> %s",
          PrettySize(options.value_size).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.filter -> %s",
          FilterOptions(options).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.filter_bits_per_key -> %d",
          int(options.filter_bits_per_key));
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.block_size -> %s",
          PrettySize(options.block_size).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.block_util -> %.2f%%",
          100 * options.block_util);
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.block_padding -> %s",
          int(options.block_padding) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.block_batch_size -> %s",
          PrettySize(options.block_batch_size).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.data_buffer -> %s",
          PrettySize(options.data_buffer).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.min_data_buffer -> %s",
          PrettySize(options.min_data_buffer).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.index_buffer -> %s",
          PrettySize(options.index_buffer).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.min_index_buffer -> %s",
          PrettySize(options.min_index_buffer).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.tail_padding -> %s",
          int(options.tail_padding) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.compaction_pool -> %s",
          options.compaction_pool != NULL
              ? options.compaction_pool->ToDebugString().c_str()
              : "None");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.compression -> %s",
          options.compression == kSnappyCompression ? "Snappy" : "None");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.index_compression -> %s",
          options.index_compression == kSnappyCompression ? "Snappy" : "None");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.force_compression -> %s",
          int(options.force_compression) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.skip_checksums -> %s",
          int(options.skip_checksums) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.measure_writes -> %s",
          int(options.measure_writes) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.epoch_log_rotation -> %s",
          int(options.epoch_log_rotation) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.allow_env_threads -> %s",
          int(options.allow_env_threads) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.is_env_pfs -> %s",
          int(options.is_env_pfs) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.mode -> %s",
          DirModeName(options.mode).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.memtable_parts -> %d (lg_parts=%d)",
          1 << options.lg_parts, options.lg_parts);
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.my_rank -> %d", options.rank);
#endif

  Rep* rep = new Rep(options, dirname);
  Status status = TryOpen(rep);
  if (status.ok()) {
    *result = new DirWriter(rep);
  } else {
    delete rep;
  }

  return status;
}

class DirReaderImpl : public DirReader {
 public:
  DirReaderImpl(const DirOptions& opts, const std::string& name);
  virtual ~DirReaderImpl();

  virtual Status Count(const CountOp& op, size_t* result);
  virtual Status Read(const ReadOp& op, const Slice& fid, std::string* dst);
  virtual Status Scan(const ScanOp& op, ScanSaver, void*);

  virtual IoStats GetIoStats() const;

 private:
  Status OpenDir(size_t part);
  RandomAccessFileStats io_stats_;
  friend class DirReader;

  DirOptions options_;
  const std::string name_;
  uint32_t num_parts_;
  uint32_t part_mask_;

  mutable port::Mutex mutex_;
  port::CondVar cond_cv_;
  // Lazily initialized directory partitions
  Dir** dirs_;
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

// Open a directory partition if it has not been opened before.
// Return OK on success, or a non-OK status on errors.
// REQUIRES: mutex_ is locked.
Status DirReaderImpl::OpenDir(size_t part) {
  mutex_.AssertHeld();
  Status status;
  assert(part < num_parts_);
  if (dirs_[part] == NULL) {
    mutex_.Unlock();  // Unlock when reading dir indexes
    LogSource* indx = NULL;
    Dir* dir = new Dir(options_, &mutex_, &cond_cv_);
    dir->Ref();
    LogSource::LogOptions idx_opts;
    idx_opts.type = kIdxIoType;
    idx_opts.sub_partition = static_cast<int>(part);
    idx_opts.rank = options_.rank;
    if (options_.measure_reads) idx_opts.seq_stats = &dir->io_stats_;
    idx_opts.io_size = options_.read_size;
    idx_opts.env = options_.env;
    status = LogSource::Open(idx_opts, name_, &indx);
    if (status.ok()) {
      status = dir->Open(indx);
    }
    mutex_.Lock();
    if (status.ok()) {
      dir->InstallDataSource(data_);
      if (dirs_[part] != NULL) dirs_[part]->Unref();
      dirs_[part] = dir;
      dirs_[part]->Ref();
    }
    dir->Unref();
    if (indx != NULL) {
      indx->Unref();
    }
  }
  return status;
}

// Perform a count operation on all partitions.
// Return OK on success, or a non-OK status on errors.
Status DirReaderImpl::Count(const CountOp& op, size_t* result) {
  Status status;
  MutexLock ml(&mutex_);
  size_t subtotal;

  *result = 0;
  for (uint32_t part = 0; part < num_parts_; part++) {
    status = OpenDir(part);
    if (status.ok()) {
      assert(dirs_[part] != NULL);
      Dir* const dir = dirs_[part];
      dir->Ref();
      Dir::CountOptions opts;
      opts.epoch_start = op.epoch_start;
      opts.epoch_end = op.epoch_end;

      status = dirs_[part]->Count(opts, &subtotal);
      dir->Unref();
      if (status.ok()) {
        *result += subtotal;
      }
    }

    if (!status.ok()) {
      break;
    }
  }

  return status;
}

// Perform a scan operation on all partitions.
// Return OK on success, or a non-OK status on errors.
Status DirReaderImpl::Scan(const ScanOp& op, ScanSaver saver, void* arg) {
  Status status;
  MutexLock ml(&mutex_);
  Dir::ScanStats stats;
  stats.total_table_seeks = 0;
  stats.total_seeks = 0;
  stats.n = 0;

  for (uint32_t part = 0; part < num_parts_; part++) {
    status = OpenDir(part);
    if (status.ok()) {
      assert(dirs_[part] != NULL);
      Dir* const dir = dirs_[part];
      dir->Ref();
      Dir::ScanOptions opts;
      opts.epoch_start = op.epoch_start;
      opts.epoch_end = op.epoch_end;
      opts.force_serial_reads = op.no_parallel_reads;
      Dir::Saver dir_saver = static_cast<Dir::Saver>(saver);
      opts.usr_cb = reinterpret_cast<void*>(dir_saver);
      opts.arg_cb = arg;
      char tmp[256];  // Temporary buffer space for the read operation
      opts.tmp_length = sizeof(tmp);
      opts.tmp = tmp;

      status = dirs_[part]->Scan(opts, &stats);
      dir->Unref();
    }

    if (!status.ok()) {
      break;
    }
  }

  if (status.ok()) {
    if (op.table_seeks != NULL) {
      *op.table_seeks = stats.total_table_seeks;
    }
    if (op.seeks != NULL) {
      *op.seeks = stats.total_seeks;
    }
    if (op.n != NULL) {
      *op.n = stats.n;
    }
  }

  return status;
}

// Perform a read operation for a key.
// Return OK on success, or a non-OK status on errors.
Status DirReaderImpl::Read(const ReadOp& op, const Slice& fid,
                           std::string* dst) {
  Status status;
  uint32_t hash = Hash(fid.data(), fid.size(), 0);
  uint32_t part = hash & part_mask_;
  MutexLock ml(&mutex_);
  Dir::ReadStats stats;
  stats.total_table_seeks = 0;
  stats.total_seeks = 0;

  status = OpenDir(part);
  if (status.ok()) {
    assert(dirs_[part] != NULL);
    Dir* const dir = dirs_[part];
    dir->Ref();
    Dir::ReadOptions opts;
    opts.epoch_start = op.epoch_start;
    opts.epoch_end = op.epoch_end;
    opts.force_serial_reads = op.no_parallel_reads;
    char tmp[256];  // Temporary buffer space for the read operation
    opts.tmp_length = sizeof(tmp);
    opts.tmp = tmp;

    status = dirs_[part]->Read(opts, fid, dst, &stats);
    dir->Unref();
  }

  if (status.ok()) {
    if (op.table_seeks != NULL) {
      *op.table_seeks = stats.total_table_seeks;
    }
    if (op.seeks != NULL) {
      *op.seeks = stats.total_seeks;
    }
  }

  return status;
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

DirReader::CountOp::CountOp()
    : epoch_start(0), epoch_end(~static_cast<uint32_t>(0)) {}

void DirReader::CountOp::SetEpoch(int epoch) {
  assert(epoch >= -1);
  if (epoch != -1) {
    epoch_start = static_cast<uint32_t>(epoch);
    epoch_end = epoch_start + 1;
  }
}

DirReader::ReadOp::ReadOp()
    : epoch_start(0),
      epoch_end(~static_cast<uint32_t>(0)),
      no_parallel_reads(false),
      table_seeks(NULL),
      seeks(NULL) {}

void DirReader::ReadOp::SetEpoch(int epoch) {
  assert(epoch >= -1);
  if (epoch != -1) {
    epoch_start = static_cast<uint32_t>(epoch);
    epoch_end = epoch_start + 1;
  }
}

DirReader::ScanOp::ScanOp()
    : epoch_start(0),
      epoch_end(~static_cast<uint32_t>(0)),
      no_parallel_reads(false),
      table_seeks(NULL),
      seeks(NULL),
      n(NULL) {}

void DirReader::ScanOp::SetEpoch(int epoch) {
  assert(epoch >= -1);
  if (epoch != -1) {
    epoch_start = static_cast<uint32_t>(epoch);
    epoch_end = epoch_start + 1;
  }
}

DirReader::~DirReader() {}

// Return the name of the filter for printing.
static std::string FilterName(FilterType type) {
  switch (type) {
    case kFtNoFilter:
      return "Dis";
    case kFtBloomFilter:
      return "Bloom filter";
    case kFtBitmap:
      return "Bitmap";
    default:
      return "Unk";
  }
}

// Override options in accordance with the given footer.
static DirOptions MaybeRewriteOptions(  // Only a subset of options are checked
    const DirOptions& origin, const Footer& footer) {
  DirOptions result = ApplyFooter(origin, footer);
  if (result.value_size != origin.value_size)
    Warn(__LOG_ARGS__, "Dfs.plfsdir.value_size -> %d (was %d)",
         int(result.value_size), int(origin.value_size));
  if (result.key_size != origin.key_size)
    Warn(__LOG_ARGS__, "Dfs.plfsdir.key_size -> %d (was %d)",
         int(result.key_size), int(origin.key_size));
  if (result.fixed_kv_length != origin.fixed_kv_length)
    Warn(__LOG_ARGS__, "Dfs.plfsdir.fixed_kv_length -> %s (was %s)",
         result.fixed_kv_length ? "Yes" : "No",
         origin.fixed_kv_length ? "Yes" : "No");
  if (result.leveldb_compatible != origin.leveldb_compatible)
    Warn(__LOG_ARGS__, "Dfs.plfsdir.leveldb_compatible -> %s (was %s)",
         result.leveldb_compatible ? "Yes" : "No",
         origin.leveldb_compatible ? "Yes" : "No");
  if (result.epoch_log_rotation != origin.epoch_log_rotation)
    Warn(__LOG_ARGS__, "Dfs.plfsdir.epoch_log_rotation -> %s (was %s)",
         result.epoch_log_rotation ? "Yes" : "No",
         origin.epoch_log_rotation ? "Yes" : "No");
  if (result.skip_checksums != origin.skip_checksums)
    Warn(__LOG_ARGS__, "Dfs.plfsdir.skip_checksums -> %s (was %s)",
         result.skip_checksums ? "Yes" : "No",
         origin.skip_checksums ? "Yes" : "No");
  if (result.filter != origin.filter)
    Warn(__LOG_ARGS__, "Dfs.plfsdir.filter -> %s (was %s)",
         FilterName(result.filter).c_str(), FilterName(origin.filter).c_str());
  if (result.mode != origin.mode)
    Warn(__LOG_ARGS__, "Dfs.plfsdir.mode -> %s (was %s)",
         DirModeName(result.mode).c_str(), DirModeName(origin.mode).c_str());
  if (result.num_epochs != origin.num_epochs)
    Warn(__LOG_ARGS__, "Dfs.plfsdir.num_epochs -> %d (was %d)",
         result.num_epochs, origin.num_epochs);
  if (result.lg_parts != origin.lg_parts)
    Warn(__LOG_ARGS__, "Dfs.plfsdir.memtable_parts -> %d (was %d)",
         1 << result.lg_parts, 1 << origin.lg_parts);
  return result;
}

static DirOptions SanitizeReadOptions(const DirOptions& options) {
  DirOptions result = options;
  if (result.num_epochs < 0) result.num_epochs = -1;
  if (result.lg_parts < 0) result.lg_parts = -1;
  if (result.env == NULL) {
    result.env = Env::Default();
  }
  return result;
}

Status DirReader::Open(const DirOptions& _opts, const std::string& dirname,
                       DirReader** result) {
  *result = NULL;
  DirOptions options = SanitizeReadOptions(_opts);
  uint32_t num_parts =  // May have to be lazy initialized from the footer
      options.lg_parts == -1 ? 0 : 1u << options.lg_parts;
  const int my_rank = options.rank;
  Env* const env = options.env;
  Status status;
#if VERBOSE >= 2
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.name -> %s (mode=read)",
          dirname.c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.filter -> %s",
          FilterName(options.filter).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.reader_pool -> %s",
          options.reader_pool != NULL
              ? options.reader_pool->ToDebugString().c_str()
              : "None");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.read_size -> %s",
          PrettySize(options.read_size).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.parallel_reads -> %s",
          int(options.parallel_reads) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.paranoid_checks -> %s",
          int(options.paranoid_checks) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.ignore_filters -> %s",
          int(options.ignore_filters) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.verify_checksums -> %s",
          int(options.verify_checksums) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.skip_checksums -> %s",
          int(options.skip_checksums) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.measure_reads -> %s",
          int(options.measure_reads) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.epoch_log_rotation -> %s",
          int(options.epoch_log_rotation) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.allow_env_threads -> %s",
          int(options.allow_env_threads) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.is_env_pfs -> %s",
          int(options.is_env_pfs) ? "Yes" : "No");
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.mode -> %s",
          DirModeName(options.mode).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.num_epochs -> %d", options.num_epochs);
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.memtable_parts -> %d (lg_parts=%d)",
          int(num_parts), options.lg_parts);
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.my_rank -> %d", my_rank);
#endif
  // We have three copies of the footer stored for each dir.
  // The primary copy is stored in a dedicated per-dir manifest file.
  // The 2nd copy is attached to the end of the data log file (data log may be
  // rotated). The last copy is appended to the end of each index log file.
  Footer footer;
  std::string dir_info;  // Stores the primary footer copy
  if (options.lg_parts == -1 || options.num_epochs == -1 ||
      options.paranoid_checks) {  // Skip the footer unless we need more info
    status = ReadFileToString(env, DirInfoFileName(dirname).c_str(), &dir_info);
    if (!status.ok()) {
      return status;
    } else if (dir_info.size() < Footer::kEncodedLength) {
      return Status::Corruption("Truncated dir info");
    }
    // Get rid of the padding
    Slice input = dir_info;
    if (input.size() > Footer::kEncodedLength) {
      input.remove_prefix(input.size() - Footer::kEncodedLength);
    }
    status = footer.DecodeFrom(&input);
    if (!status.ok()) {
      return status;
    }

    // Rewrite potentially ill-specified options
    options = MaybeRewriteOptions(options, footer);
    num_parts = 1u << options.lg_parts;
  }

  LogSource* data = NULL;
  DirReaderImpl* impl = new DirReaderImpl(options, dirname);
  LogSource::LogOptions io_opts;
  io_opts.rank = my_rank;
  io_opts.type = kDefIoType;
  io_opts.sub_partition = -1;  // The data file does not have any sub-partitions
  if (options.epoch_log_rotation) io_opts.num_rotas = options.num_epochs + 1;
  if (options.measure_reads) io_opts.stats = &impl->io_stats_;
  io_opts.env = env;
  status = LogSource::Open(io_opts, dirname, &data);
  if (!status.ok()) {
    // Error
  } else if (data->Size(data->LastFileIndex()) < Footer::kEncodedLength) {
    status = Status::Corruption("Data log too short to be valid");
  } else if (options.paranoid_checks) {
    // Also verify the replicated footer if requested
    Slice contents;
    char tmp[Footer::kEncodedLength];
    uint64_t off = data->Size(data->LastFileIndex()) - Footer::kEncodedLength;
    status = data->Read(off, Footer::kEncodedLength, &contents, tmp,
                        data->LastFileIndex());
    if (status.ok()) {
      if (!Slice(dir_info).ends_with(contents)) {
        status = Status::Corruption("Footer replica corrupted");
      }
    }
  }

  if (status.ok()) {
    // Dir indexes to be fetched later
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

}  // namespace plfsio
}  // namespace pdlfs
