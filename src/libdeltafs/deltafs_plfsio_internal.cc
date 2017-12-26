/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_internal.h"
#include "deltafs_plfsio_events.h"
#include "deltafs_plfsio_filter.h"

#include "pdlfs-common/logging.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/strutil.h"

#include <assert.h>
#include <math.h>
#include <algorithm>

namespace pdlfs {
extern const char* GetLengthPrefixedSlice(const char* p, const char* limit,
                                          Slice* result);
namespace plfsio {

// Return current time in microseconds.
static inline uint64_t GetCurrentTimeMicros() {
  return Env::Default()->NowMicros();
}

// Create a new write buffer and determine an estimated memory usage per
// entry. For small entries, there is a 2-byte overhead per entry.
// This overhead is necessary for supporting variable length
// key-value pairs.
WriteBuffer::WriteBuffer(const DirOptions& options)
    : num_entries_(0), finished_(false) {
  const size_t entry_size =  // Estimated, actual entry sizes may differ
      options.key_size + options.value_size;
  bytes_per_entry_ =  // Memory usage per entry
      static_cast<size_t>(
          VarintLength(options.key_size)) +  // Varint encoding for key lengths
      static_cast<size_t>(
          VarintLength(options.value_size)) +  // For value lengths
      entry_size;
}

class WriteBuffer::Iter : public Iterator {
 public:
  explicit Iter(const WriteBuffer* write_buffer)
      : cursor_(-1),
        offsets_(&write_buffer->offsets_[0]),
        num_entries_(write_buffer->num_entries_),
        buffer_(write_buffer->buffer_) {}

  virtual ~Iter() {}
  virtual void Next() { cursor_++; }
  virtual void Prev() { cursor_--; }
  virtual Status status() const { return Status::OK(); }
  virtual bool Valid() const { return cursor_ >= 0 && cursor_ < num_entries_; }
  virtual void SeekToFirst() { cursor_ = 0; }
  virtual void SeekToLast() { cursor_ = num_entries_ - 1; }
  virtual void Seek(const Slice& target) {
    // Not supported
  }

  virtual Slice key() const {
    assert(Valid());
    Slice result;
    const char* p = &buffer_[offsets_[cursor_]];
    Slice input = buffer_;
    assert(p - buffer_.data() >= 0);
    input.remove_prefix(p - buffer_.data());
    if (GetLengthPrefixedSlice(&input, &result)) {
      return result;
    } else {
      assert(false);
      result = Slice();
      return result;
    }
  }

  virtual Slice value() const {
    assert(Valid());
    Slice result;
    const char* p = &buffer_[offsets_[cursor_]];
    Slice input = buffer_;
    assert(p - buffer_.data() >= 0);
    input.remove_prefix(p - buffer_.data());
    if (GetLengthPrefixedSlice(&input, &result) &&
        GetLengthPrefixedSlice(&input, &result)) {
      return result;
    } else {
      assert(false);
      result = Slice();
      return result;
    }
  }

 private:
  int cursor_;
  const uint32_t* offsets_;
  int num_entries_;
  Slice buffer_;
};

Iterator* WriteBuffer::NewIterator() const {
  assert(finished_);
  return new Iter(this);
}

struct WriteBuffer::STLLessThan {
  Slice buffer_;

  explicit STLLessThan(const Slice& buffer) : buffer_(buffer) {}

  bool operator()(uint32_t a, uint32_t b) {
    Slice key_a = GetKey(a);
    Slice key_b = GetKey(b);
    assert(!key_a.empty() && !key_b.empty());
    return key_a < key_b;
  }

  Slice GetKey(uint32_t offset) {
    Slice result;
    const char* p = GetLengthPrefixedSlice(
        buffer_.data() + offset, buffer_.data() + buffer_.size(), &result);
    if (p != NULL) {
      return result;
    } else {
      assert(false);
      return result;
    }
  }
};

void WriteBuffer::Finish(bool skip_sort) {
  assert(!finished_);
  finished_ = true;
  // Sort entries if not skipped
  if (!skip_sort) {
    std::vector<uint32_t>::iterator begin = offsets_.begin();
    std::vector<uint32_t>::iterator end = offsets_.end();
    std::sort(begin, end, STLLessThan(buffer_));
  }
}

void WriteBuffer::Reset() {
  num_entries_ = 0;
  finished_ = false;
  offsets_.clear();
  buffer_.clear();
}

void WriteBuffer::Reserve(size_t bytes_to_reserve) {
  // Reserve memory for the write buffer
  buffer_.reserve(bytes_to_reserve);
  const uint32_t num_entries =  // Estimated, actual counts may differ
      static_cast<uint32_t>(ceil(double(bytes_to_reserve) / bytes_per_entry_));
  // Also reserve memory for the offset array
  offsets_.reserve(num_entries);
}

bool WriteBuffer::Add(const Slice& key, const Slice& value) {
  assert(!finished_);       // Finish() has not been called
  assert(key.size() != 0);  // Key cannot be empty
  const size_t offset = buffer_.size();
  PutLengthPrefixedSlice(&buffer_, key);
  PutLengthPrefixedSlice(&buffer_, value);
  offsets_.push_back(static_cast<uint32_t>(offset));
  num_entries_++;
  return true;
}

size_t WriteBuffer::bytes_per_entry() const {
  return bytes_per_entry_;  // Not including the 4-byte memory for the offset
}

size_t WriteBuffer::memory_usage() const {
  size_t result = 0;
  result += sizeof(uint32_t) * offsets_.capacity();
  result += buffer_.capacity();
  return result;
}

OutputStats::OutputStats()
    : final_data_size(0),
      data_size(0),
      final_meta_index_size(0),
      meta_index_size(0),
      final_index_size(0),
      index_size(0),
      final_filter_size(0),
      filter_size(0),
      value_size(0),
      key_size(0) {}

TableLogger::TableLogger(const DirOptions& options, LogSink* data,
                         LogSink* indx)
    : options_(options),
      num_uncommitted_indx_(0),
      num_uncommitted_data_(0),
      pending_restart_(false),
      pending_commit_(false),
      data_block_(16),
      indx_block_(1),
      meta_block_(1),
      root_block_(1),
      pending_indx_entry_(false),
      pending_meta_entry_(false),
      pending_root_entry_(false),
      total_num_keys_(0),
      total_num_dropped_keys_(0),
      total_num_blocks_(0),
      total_num_tables_(0),
      num_tables_(0),
      num_epochs_(0),
      pending_data_flush_(0),
      pending_indx_flush_(0),
      data_sink_(data),
      data_offset_(0),
      indx_logger_(options, indx),
      indx_sink_(indx),
      finished_(false) {
  // Sanity checks
  assert(indx_sink_ != NULL && data_sink_ != NULL);

  indx_sink_->Ref();
  data_sink_->Ref();

  // Allocate memory
  const size_t estimated_index_size_per_table = 4 << 10;
  indx_block_.Reserve(estimated_index_size_per_table);
  const size_t estimated_meta_index_size_per_epoch = 4 << 10;
  meta_block_.Reserve(estimated_meta_index_size_per_epoch);
  const size_t estimated_root_index = 4 << 10;
  root_block_.Reserve(estimated_root_index);

  uncommitted_indexes_.reserve(1 << 10);
  data_block_.buffer_store()->reserve(options_.block_batch_size);
  data_block_.buffer_store()->clear();
  pending_restart_ = true;
}

TableLogger::~TableLogger() {
  indx_sink_->Unref();
  data_sink_->Unref();
}

void TableLogger::MakeEpoch() {
  assert(!finished_);  // Finish() has not been called
  EndTable(static_cast<EmptyFilterBlock*>(NULL),
           static_cast<ChunkType>(EmptyFilterBlock::chunk_type()));
  if (!ok()) {
    return;  // Abort
  } else if (num_tables_ == 0) {
    return;  // Empty epoch
  } else if (num_epochs_ >= kMaxEpochNo) {
    status_ = Status::AssertionFailed("Too many epochs");
    return;
  }
  EpochStone stone;

  BlockHandle meta_index_handle;
  Slice meta_index_contents = meta_block_.Finish();
  status_ =
      indx_logger_.Write(kMetaChunk, meta_index_contents, &meta_index_handle);

  if (ok()) {
    const uint64_t meta_index_size = meta_index_contents.size();
    const uint64_t final_meta_index_size =
        meta_index_handle.size() + kBlockTrailerSize;
    output_stats_.final_meta_index_size += final_meta_index_size;
    output_stats_.meta_index_size += meta_index_size;
  } else {
    return;  // Abort
  }

  if (ok()) {
    meta_block_.Reset();
    pending_root_handle_.set_offset(meta_index_handle.offset());
    pending_root_handle_.set_size(meta_index_handle.size());
    assert(!pending_root_entry_);
    pending_root_entry_ = true;
  } else {
    return;  // Abort
  }

  if (num_epochs_ > kMaxEpochNo) {
    status_ = Status::AssertionFailed("Too many epochs");
  } else if (pending_root_entry_) {
    std::string handle_encoding;
    pending_root_handle_.EncodeTo(&handle_encoding);
    root_block_.Add(EpochKey(num_epochs_), handle_encoding);
    pending_root_entry_ = false;
  }

  // Insert an epoch seal
  if (ok()) {
    stone.set_handle(meta_index_handle);
    stone.set_id(num_epochs_);
    std::string epoch_stone;
    stone.EncodeTo(&epoch_stone);
    status_ = indx_logger_.SealEpoch(epoch_stone);
  } else {
    return;  // Abort
  }

  if (ok()) {
    pending_indx_flush_ = indx_sink_->Ltell();
    pending_data_flush_ = data_offset_;
  }

  if (ok()) {
#ifndef NDEBUG
    // Keys are only required to be unique within an epoch
    keys_.clear();
#endif

    num_tables_ = 0;
    num_epochs_++;
  }
}

template <typename T>
void TableLogger::EndTable(T* filter_block, ChunkType filter_type) {
  assert(!finished_);  // Finish() has not been called

  EndBlock();
  if (!ok()) {
    return;
  } else if (pending_indx_entry_) {
    BytewiseComparator()->FindShortSuccessor(&last_key_);
    PutLengthPrefixedSlice(&uncommitted_indexes_, last_key_);
    pending_indx_handle_.EncodeTo(&uncommitted_indexes_);
    pending_indx_entry_ = false;
    num_uncommitted_indx_++;
  }

  Commit();
  if (!ok()) {
    return;
  } else if (indx_block_.empty()) {
    return;  // Empty table
  }

  BlockHandle index_handle;
  Slice index_contents = indx_block_.Finish();
  status_ = indx_logger_.Write(kIdxChunk, index_contents, &index_handle);

  if (ok()) {
    const uint64_t index_size = index_contents.size();
    const uint64_t final_index_size = index_handle.size() + kBlockTrailerSize;
    output_stats_.final_index_size += final_index_size;
    output_stats_.index_size += index_size;
  } else {
    return;  // Abort
  }

  BlockHandle filter_handle;
  if (filter_block != NULL) {
    Slice filer_contents = filter_block->Finish();
    status_ = indx_logger_.Write(filter_type, filer_contents, &filter_handle);
    if (ok()) {
      const uint64_t filter_size = filer_contents.size();
      const uint64_t final_filter_size =
          filter_handle.size() + kBlockTrailerSize;
      output_stats_.final_filter_size += final_filter_size;
      output_stats_.filter_size += filter_size;
    } else {
      return;  // Abort
    }
  } else {
    filter_handle.set_offset(0);  // No filter configured
    filter_handle.set_size(0);
  }

  if (ok()) {
    indx_block_.Reset();
    pending_meta_handle_.set_filter_offset(filter_handle.offset());
    pending_meta_handle_.set_filter_size(filter_handle.size());
    pending_meta_handle_.set_index_offset(index_handle.offset());
    pending_meta_handle_.set_index_size(index_handle.size());
    assert(!pending_meta_entry_);
    pending_meta_entry_ = true;
  } else {
    return;  // Abort
  }

  if (num_tables_ > kMaxTableNo) {
    status_ = Status::AssertionFailed("Too many tables");
  } else if (pending_meta_entry_) {
    pending_meta_handle_.set_smallest_key(smallest_key_);
    BytewiseComparator()->FindShortSuccessor(&largest_key_);
    pending_meta_handle_.set_largest_key(largest_key_);
    std::string handle_encoding;
    pending_meta_handle_.EncodeTo(&handle_encoding);
    meta_block_.Add(EpochTableKey(num_epochs_, num_tables_), handle_encoding);
    pending_meta_entry_ = false;
  }

  if (ok()) {
    smallest_key_.clear();
    largest_key_.clear();
    last_key_.clear();
    total_num_tables_++;
    num_tables_++;
  }
}

void TableLogger::Commit() {
  assert(!finished_);  // Finish() has not been called
  // Skip empty commit
  if (data_block_.buffer_store()->empty()) return;
  if (!ok()) return;  // Abort

  assert(num_uncommitted_data_ == num_uncommitted_indx_);
  std::string* const buffer = data_block_.buffer_store();

  Slice key;
  data_sink_->Lock();
  if (options_.block_padding) {
    assert(buffer->size() % options_.block_size ==
           0);  // Verify block alignment
  }
  // A data log file may be rotated so we must index against the
  // physical offset
  const size_t base = data_sink_->Ptell();
  int num_index_committed = 0;
  Slice input = uncommitted_indexes_;
  std::string handle_encoding;
  BlockHandle handle;
  while (!input.empty()) {
    if (GetLengthPrefixedSlice(&input, &key)) {
      handle.DecodeFrom(&input);
      const uint64_t offset = handle.offset();
      handle.set_offset(base + offset);  // Finalize the block offset
      handle_encoding.clear();
      handle.EncodeTo(&handle_encoding);
      assert(offset >= BlockHandle::kMaxEncodedLength);
      assert(
          memcmp(&buffer->at(offset - BlockHandle::kMaxEncodedLength),
                 std::string(size_t(BlockHandle::kMaxEncodedLength), 0).c_str(),
                 BlockHandle::kMaxEncodedLength) == 0);
      // Finalize the leading block handle
      memcpy(&buffer->at(offset - BlockHandle::kMaxEncodedLength),
             handle_encoding.data(), handle_encoding.size());
      if (options_.block_padding) {
        assert((base + offset - BlockHandle::kMaxEncodedLength) %
                   options_.block_size ==
               0);  // Verify block alignment
      }
      indx_block_.Add(key, handle_encoding);
      num_index_committed++;
    } else {
      break;
    }
  }

  assert(num_index_committed == num_uncommitted_indx_);
  status_ = data_sink_->Lwrite(*buffer);
  data_offset_ = base + buffer->size();
  data_sink_->Unlock();
  if (!ok()) return;  // Abort

  pending_commit_ = false;
  num_uncommitted_data_ = num_uncommitted_indx_ = 0;
  uncommitted_indexes_.clear();
  data_block_.buffer_store()->clear();
  pending_restart_ = true;
}

void TableLogger::EndBlock() {
  assert(!finished_);               // Finish() has not been called
  if (pending_restart_) return;     // Empty block
  if (data_block_.empty()) return;  // Empty block
  if (!ok()) return;                // Abort

  // | <------------ options_.block_size (e.g. 32KB) ------------> |
  //   block handle   block contents  block trailer  block padding
  //                | <---------- final block contents ----------> |
  //                          (LevelDb compatible format)
  Slice block_contents = data_block_.Finish();
  const size_t block_size = block_contents.size();
  Slice final_block_contents;  // With the trailer and any inserted padding
  if (options_.block_padding) {
    // Target size for the final block contents
    const size_t padding_target =
        options_.block_size - BlockHandle::kMaxEncodedLength;
    assert(block_size + kBlockTrailerSize <=
           padding_target);  // Must fit in the space
    final_block_contents = data_block_.Finalize(
        !options_.skip_checksums, static_cast<uint32_t>(padding_target),
        static_cast<char>(0xff));
  } else {
    final_block_contents = data_block_.Finalize(!options_.skip_checksums);
  }

  const size_t final_block_size = final_block_contents.size();
  const uint64_t block_offset =
      data_block_.buffer_store()->size() - final_block_size;
  output_stats_.final_data_size += final_block_size;
  output_stats_.data_size += block_size;

  if (ok()) {
    pending_restart_ = true;
    pending_indx_handle_.set_size(block_size);
    pending_indx_handle_.set_offset(block_offset);
    assert(!pending_indx_entry_);
    pending_indx_entry_ = true;
    num_uncommitted_data_++;
    total_num_blocks_++;
  }
}

void TableLogger::Add(const Slice& key, const Slice& value) {
  assert(!finished_);       // Finish() has not been called
  assert(key.size() != 0);  // Keys cannot be empty
  if (!ok()) return;        // Abort

  if (!last_key_.empty()) {
    // Keys within a single table are inserted in a weakly sorted order
    assert(key >= last_key_);
    if (options_.mode == kDirUniqueDrop) {  // Auto deduplicate
      if (key == last_key_) {
        total_num_dropped_keys_++;
        return;  // Drop
      }
    } else if (options_.mode != kDirMultiMap) {
      assert(key != last_key_);  // Keys are strongly ordered, no duplicates
    }
  }
  if (smallest_key_.empty()) {
    smallest_key_ = key.ToString();
  }
  largest_key_ = key.ToString();

  // Add an index entry if there is one pending insertion
  if (pending_indx_entry_) {
    BytewiseComparator()->FindShortestSeparator(&last_key_, key);
    PutLengthPrefixedSlice(&uncommitted_indexes_, last_key_);
    pending_indx_handle_.EncodeTo(&uncommitted_indexes_);
    pending_indx_entry_ = false;
    num_uncommitted_indx_++;
  }

  // Commit buffered data and indexes
  if (pending_commit_) {
    Commit();
    if (!ok()) {
      return;
    }
  }

  // Restart the block buffer
  if (pending_restart_) {
    pending_restart_ = false;
    data_block_.SwitchBuffer(
        NULL);  // Continue appending to the same underlying buffer
    // Pre-reserve enough space for the leading block handle
    data_block_.Pad(BlockHandle::kMaxEncodedLength);
    data_block_.Reset();
  }

  last_key_ = key.ToString();
  output_stats_.value_size += value.size();
  output_stats_.key_size += key.size();
#ifndef NDEBUG
  if (options_.mode == kDirUnique) {
    assert(keys_.count(last_key_) == 0);
    keys_.insert(last_key_);
  }
#endif

  data_block_.Add(key, value);
  total_num_keys_++;
  if (data_block_.CurrentSizeEstimate() + kBlockTrailerSize +
          BlockHandle::kMaxEncodedLength >=
      static_cast<size_t>(options_.block_size * options_.block_util)) {
    EndBlock();
    // Schedule buffer commit if it is about to full
    if (data_block_.buffer_store()->size() + options_.block_size >
        options_.block_batch_size) {
      pending_commit_ = true;
    }
  }
}

Status TableLogger::Finish() {
  assert(!finished_);  // Finish() has not been called
  MakeEpoch();
  finished_ = true;
  if (!ok()) return status_;
  std::string footer_buf;
  Footer footer = Mkfoot(options_);

  assert(!pending_indx_entry_);
  assert(!pending_meta_entry_);
  assert(!pending_root_entry_);

  BlockHandle root_index_handle;
  Slice root_index_contents = root_block_.Finish();
  status_ =
      indx_logger_.Write(kRtChunk, root_index_contents, &root_index_handle);

  if (ok()) {
    const uint64_t root_index_size = root_index_contents.size();
    const uint64_t final_root_index_size =
        root_index_handle.size() + kBlockTrailerSize;
    output_stats_.final_meta_index_size += final_root_index_size;
    output_stats_.meta_index_size += root_index_size;
  } else {
    return status_;
  }

  // Write the final footer
  footer.set_epoch_index_handle(root_index_handle);
  footer.set_num_epochs(num_epochs_);
  footer.EncodeTo(&footer_buf);
  status_ = indx_logger_.Finish(footer_buf);

  return status_;
}

template <typename T>
DirLogger<T>::DirLogger(const DirOptions& options, size_t part, port::Mutex* mu,
                        port::CondVar* cv)
    : options_(options),
      bg_cv_(cv),
      mu_(mu),
      part_(part),
      num_flush_requested_(0),
      num_flush_completed_(0),
      has_bg_compaction_(false),
      filter_(NULL),
      mem_buf_(NULL),
      imm_buf_(NULL),
      imm_buf_is_epoch_flush_(false),
      imm_buf_is_final_(false),
      buf0_(options),
      buf1_(options),
      tb_(NULL),
      data_(NULL),
      indx_(NULL),
      opened_(false),
      refs_(0) {
  // Determine the right table size and bloom filter size.
  // Works best when the key and value sizes are fixed.
  //
  // Otherwise, if the estimated key or value sizes are greater
  // than the real average, filter will be allocated with less bytes
  // and there will be higher false positive rate.
  //
  // On the other hand, if the estimated sizes are less than
  // the real, filter will waste memory and each
  // write buffer will be allocated with
  // less memory.
  assert(buf0_.bytes_per_entry() == buf1_.bytes_per_entry());
  size_t bytes_per_entry = buf0_.bytes_per_entry();  // Estimated memory usage

  size_t total_bits_per_entry =  // Due to double buffering and filtering
      2 * (8 * bytes_per_entry) + options_.filter_bits_per_key;

  size_t memory =  // Total write buffer for each memtable partition
      options_.total_memtable_budget /
          static_cast<uint32_t>(1 << options_.lg_parts) -
      options_.block_batch_size;  // Reserved for compaction

  // Estimated number of entries per table according to configured key size,
  // value size, and filter size.
  entries_per_tb_ = static_cast<uint32_t>(
      ceil(8.0 * double(memory) / double(total_bits_per_entry)));

  // Memory reserved for each table.
  // This portion of memory is part of the entire memtable budget stated in
  // user options. The actual memory, and storage, used may differ.
  tb_bytes_ = entries_per_tb_ * bytes_per_entry;

  // Memory reserved for the filter associated with each table.
  // This portion of memory is part of the entire memtable budget stated in
  // user options. The actual memory, and storage, used for filters may differ.
  ft_bits_ = entries_per_tb_ * options_.filter_bits_per_key;

  ft_bytes_ = (ft_bits_ + 7) / 8;
  ft_bits_ = ft_bytes_ * 8;

#if VERBOSE >= 2
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.memtable.tb_size -> %d x %s",
          2 * (1 << options_.lg_parts), PrettySize(tb_bytes_).c_str());
  Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.memtable.ft_size -> %d x %s",
          2 * (1 << options_.lg_parts), PrettySize(ft_bytes_).c_str());
#endif

  // Allocate memory
  buf0_.Reserve(tb_bytes_);
  buf1_.Reserve(tb_bytes_);

  if (options_.filter == kDirNoFilter) {
    // Skip filter
  } else if (options_.filter != kDirBloomFilter ||
             options_.bf_bits_per_key != 0) {
    filter_ = new T(options_, ft_bytes_);
  } else {
    // Skip filter
  }

  mem_buf_ = &buf0_;
}

template <typename T>
DirLogger<T>::~DirLogger() {
  mu_->AssertHeld();
  while (has_bg_compaction_) {
    bg_cv_->Wait();
  }
  delete tb_;
  if (data_ != NULL) data_->Unref();
  if (indx_ != NULL) indx_->Unref();
  delete filter_;
}

template <typename T>
Status DirLogger<T>::Open(LogSink* data, LogSink* indx) {
  // Open() has not been called before
  assert(!opened_);
  opened_ = true;
  data_ = data;
  indx_ = indx;

  data_->Ref();
  indx_->Ref();
  tb_ = new TableLogger(options_, data_, indx_);

  return Status::OK();
}

// True iff there is an on-going background compaction.
template <typename T>
bool DirLogger<T>::has_bg_compaction() {
  mu_->AssertHeld();
  return has_bg_compaction_;
}

// Report background compaction status.
template <typename T>
Status DirLogger<T>::bg_status() {
  mu_->AssertHeld();
  return bg_status_;
}

// Sync and pre-close all linked log files.
// By default, log files are reference-counted and are implicitly closed when
// de-referenced by the last opener. Optionally, a caller may force data
// sync and pre-closing all log files.
template <typename T>
Status DirLogger<T>::SyncAndClose() {
  Status status;
  if (!opened_) return status;
  assert(data_ != NULL);
  data_->Lock();
  status = data_->Lclose(true);
  data_->Unlock();
  assert(indx_ != NULL);
  if (status.ok()) status = indx_->Lclose(true);
  return status;
}

// If dry_run has been set, simply perform status checks and no compaction
// jobs will be scheduled or waited for. Return immediately, and return OK if
// compaction may be scheduled immediately without waiting, or return a special
// status if compaction cannot be scheduled immediately due to lack of buffer
// space, or directly return a status that indicates an I/O error.
// Otherwise, **wait** until a compaction is scheduled unless
// options_.non_blocking is set. After a compaction has been scheduled,
// **wait** until it finishes unless no_wait has been set.
template <typename T>
Status DirLogger<T>::Flush(const FlushOptions& flush_options) {
  mu_->AssertHeld();
  assert(opened_);
  // Wait for buffer space
  while (imm_buf_ != NULL) {
    if (flush_options.dry_run || options_.non_blocking) {
      return Status::BufferFull(Slice());
    } else {
      bg_cv_->Wait();
    }
  }

  Status status;
  if (flush_options.dry_run) {
    status = bg_status_;  // Status check only
  } else {
    num_flush_requested_++;
    const uint32_t thres = num_flush_requested_;
    const bool force = true;
    status = Prepare(force, flush_options.epoch_flush, flush_options.finalize);
    if (status.ok()) {
      if (!flush_options.no_wait) {
        while (num_flush_completed_ < thres) {
          bg_cv_->Wait();
        }
      }
    }
  }

  return status;
}

template <typename T>
Status DirLogger<T>::Add(const Slice& key, const Slice& value) {
  mu_->AssertHeld();
  assert(opened_);
  Status status = Prepare();
  while (status.ok()) {
    // Implementation may reject a key-value insertion
    if (!mem_buf_->Add(key, value)) {
      status = Prepare();
    } else {
      break;
    }
  }
  return status;
}

template <typename T>
Status DirLogger<T>::Prepare(bool force /* force minor memtable flush */,
                             bool epoch_flush /* force epoch flush */,
                             bool finalize) {
  mu_->AssertHeld();
  Status status;
  assert(mem_buf_ != NULL);
  while (true) {
    if (!bg_status_.ok()) {
      status = bg_status_;
      break;
    } else if (!force &&
               mem_buf_->CurrentBufferSize() <
                   static_cast<size_t>(tb_bytes_ * options_.memtable_util)) {
      // There is room in current write buffer
      break;
    } else if (imm_buf_ != NULL) {
      if (options_.non_blocking) {
        status = Status::BufferFull(Slice());
        break;
      } else {
        bg_cv_->Wait();
      }
    } else {
      // Attempt to switch to a new write buffer
      force = false;
      assert(imm_buf_ == NULL);
      imm_buf_ = mem_buf_;
      if (epoch_flush) imm_buf_is_epoch_flush_ = true;
      epoch_flush = false;
      if (finalize) imm_buf_is_final_ = true;
      finalize = false;
      WriteBuffer* const current_buf = mem_buf_;
      MaybeScheduleCompaction();
      if (current_buf == &buf0_) {
        mem_buf_ = &buf1_;
      } else {
        mem_buf_ = &buf0_;
      }
    }
  }

  return status;
}

template <typename T>
void DirLogger<T>::MaybeScheduleCompaction() {
  mu_->AssertHeld();

  // Do not schedule more if we are in error status
  if (!bg_status_.ok()) {
    return;
  }
  // Skip if there is one already scheduled
  if (has_bg_compaction_) {
    return;
  }
  // Nothing to be scheduled
  if (imm_buf_ == NULL) {
    return;
  }

  has_bg_compaction_ = true;

  if (options_.compaction_pool != NULL) {
    options_.compaction_pool->Schedule(DirLogger::BGWork, this);
  } else if (options_.allow_env_threads) {
    Env::Default()->Schedule(DirLogger::BGWork, this);
  } else {
    DoCompaction();
  }
}

template <typename T>
void DirLogger<T>::BGWork(void* arg) {
  DirLogger* ins = reinterpret_cast<DirLogger*>(arg);
  MutexLock ml(ins->mu_);
  ins->DoCompaction();
}

template <typename T>
void DirLogger<T>::DoCompaction() {
  mu_->AssertHeld();
  assert(has_bg_compaction_);
  assert(imm_buf_ != NULL);
  CompactMemtable();
  imm_buf_->Reset();
  imm_buf_is_epoch_flush_ = false;
  imm_buf_is_final_ = false;
  imm_buf_ = NULL;
  has_bg_compaction_ = false;
  MaybeScheduleCompaction();
  bg_cv_->SignalAll();
}

template <typename T>
void DirLogger<T>::CompactMemtable() {
  mu_->AssertHeld();
  WriteBuffer* const buffer = imm_buf_;
  assert(buffer != NULL);
  const bool is_final = imm_buf_is_final_;
  const bool is_epoch_flush = imm_buf_is_epoch_flush_;
  TableLogger* const tb = tb_;
  T* const ft = filter_;
  mu_->Unlock();
  const uint64_t start = GetCurrentTimeMicros();
  if (options_.listener != NULL) {
    CompactionEvent event;
    event.type = kCompactionStart;
    event.micros = start;
    event.part = part_;
    options_.listener->OnEvent(kCompactionStart, &event);
  }
#if VERBOSE >= 3
  Verbose(__LOG_ARGS__, 3, "Compacting memtable: (%d/%d Bytes) ...",
          static_cast<int>(buffer->CurrentBufferSize()),
          static_cast<int>(tb_bytes_));
#ifndef NDEBUG
  const OutputStats stats = tb->output_stats_;
#endif
#endif  // VERBOSE
#ifndef NDEBUG
  uint32_t num_keys = 0;
#endif
  buffer->Finish(options_.skip_sort);
  Iterator* const iter = buffer->NewIterator();
  iter->SeekToFirst();
  if (ft != NULL) {
    ft->Reset(buffer->NumEntries());
  }
  for (; iter->Valid(); iter->Next()) {
#ifndef NDEBUG
    num_keys++;
#endif
    if (ft != NULL) {
      ft->AddKey(iter->key());
    }
    tb->Add(iter->key(), iter->value());
    if (!tb->ok()) {
      break;
    }
  }

  if (tb->ok()) {
    // Double checks
    assert(num_keys == buffer->NumEntries());
    // Inject the filter into the table
    tb->EndTable(ft, static_cast<ChunkType>(T::chunk_type()));
#if VERBOSE >= 3
#ifndef NDEBUG
    Verbose(
        __LOG_ARGS__, 3, "\t+ D: %s, I: %s, F: %s",
        PrettySize(tb->output_stats_.final_data_size - stats.final_data_size)
            .c_str(),
        PrettySize(tb->output_stats_.final_index_size - stats.final_index_size)
            .c_str(),
        PrettySize(tb->output_stats_.final_filter_size -
                   stats.final_filter_size)
            .c_str());
#endif
#endif  // VERBOSE
    if (is_epoch_flush) {
      tb->MakeEpoch();
    }
    if (is_final) {
      tb->Finish();
    }
  }

  const uint64_t end = GetCurrentTimeMicros();
  if (options_.listener != NULL) {
    CompactionEvent event;
    event.type = kCompactionEnd;
    event.micros = end;
    event.part = part_;
    options_.listener->OnEvent(kCompactionEnd, &event);
  }
#if VERBOSE >= 3
  Verbose(__LOG_ARGS__, 3, "Compaction done: %d kv pairs (%d us)",
          static_cast<int>(buffer->NumEntries()),
          static_cast<int>(end - start));
#endif

  Status status = tb->status();
  delete iter;
  mu_->Lock();
  num_flush_completed_++;
  bg_status_ = status;
  return;
}

template <typename T>
size_t DirLogger<T>::memory_usage() const {
  mu_->AssertHeld();
  if (opened_) {
    size_t result = 0;
    result += buf0_.memory_usage();
    result += buf1_.memory_usage();
    std::vector<std::string*> stores;
    stores.push_back(tb_->root_block_.buffer_store());
    stores.push_back(tb_->meta_block_.buffer_store());
    stores.push_back(tb_->indx_block_.buffer_store());
    stores.push_back(tb_->data_block_.buffer_store());
    if (filter_ != NULL) stores.push_back(filter_->buffer_store());
    stores.push_back(tb_->indx_logger_.buffer_store());
    for (size_t i = 0; i < stores.size(); i++) {
      result += stores[i]->capacity();
    }
    return result;
  } else {
    return 0;
  }
}

template class DirLogger<BloomBlock>;

template class DirLogger<BitmapBlock<UncompressedFormat> >;

template class DirLogger<BitmapBlock<VarintFormat> >;

template class DirLogger<BitmapBlock<VarintPlusFormat> >;

template class DirLogger<BitmapBlock<PVarintPlusFormat> >;

template class DirLogger<BitmapBlock<PForDeltaFormat> >;

template class DirLogger<BitmapBlock<PpForDeltaFormat> >;

template class DirLogger<BitmapBlock<RoaringFormat> >;

template class DirLogger<BitmapBlock<PRoaringFormat> >;

template class DirLogger<EmptyFilterBlock>;

static Status ReadBlock(LogSource* source, const DirOptions& options,
                        const BlockHandle& handle, BlockContents* result,
                        bool cached = false, uint32_t file_index = 0,
                        char* tmp = NULL, size_t tmp_length = 0) {
  result->data = Slice();
  result->heap_allocated = false;
  result->cachable = false;

  assert(source != NULL);
  size_t n = static_cast<size_t>(handle.size());
  size_t m = n + kBlockTrailerSize;
  char* buf = tmp;
  if (cached) {
    buf = NULL;
  } else if (tmp == NULL || tmp_length < m) {
    buf = new char[m];
  }
  Slice contents;
  Status status = source->Read(handle.offset(), m, &contents, buf, file_index);
  if (status.ok()) {
    if (contents.size() != m) {
      status = Status::Corruption("Truncated block read");
    }
  }
  if (!status.ok()) {
    if (buf != tmp) delete[] buf;
    return status;
  }

  // CRC checks
  const char* data = contents.data();  // Pointer to where read put the data
  if (!options.skip_checksums && options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      if (buf != tmp) delete[] buf;
      status = Status::Corruption("Block checksum mismatch");
      return status;
    }
  }

  if (data[n] == kSnappyCompression) {
    size_t ulen = 0;
    if (!port::Snappy_GetUncompressedLength(data, n, &ulen)) {
      if (buf != tmp) delete[] buf;
      status = Status::Corruption("Cannot compress");
      return status;
    }
    char* ubuf = new char[ulen];
    if (!port::Snappy_Uncompress(data, n, ubuf)) {
      if (buf != tmp) delete[] buf;
      delete[] ubuf;
      status = Status::Corruption("Cannot compress");
      return status;
    }
    if (buf != tmp) {
      delete[] buf;
    }
    result->data = Slice(ubuf, ulen);
    result->heap_allocated = true;
    result->cachable = true;
  } else if (data != buf) {
    // File implementation has given us pointer to some other data.
    // Use it directly under the assumption that it will be live
    // while the file is open.
    if (buf != tmp) {
      delete[] buf;
    }
    result->data = Slice(data, n);
    result->heap_allocated = false;
    result->cachable = false;  // Avoid double cache
  } else {
    result->data = Slice(buf, n);
    result->heap_allocated = (buf != tmp);
    result->cachable = true;
  }

  return status;
}

// Retrieve value to a specific key from a given block and call "opts.saver"
// using the value found. In addition, set *exhausted to true if a larger key
// has been observed so there is no need to check further.
// Return OK on success and a non-OK status on errors.
Status Dir::Fetch(const FetchOptions& opts, const Slice& key,
                  const BlockHandle& h, bool* exhausted) {
  *exhausted = false;
  Status status;
  BlockContents contents;
  status = ReadBlock(data_, options_, h, &contents, false, opts.file_index,
                     opts.tmp, opts.tmp_length);
  if (!status.ok()) {
    return status;
  } else {
    opts.stats->seeks++;
  }

  Block* block = new Block(contents);
  Iterator* const iter = block->NewIterator(BytewiseComparator());
  if (options_.mode != kDirMultiMap) {
    iter->Seek(key);  // Binary search
  } else {
    iter->SeekToFirst();
    while (iter->Valid() && key > iter->key()) {
      iter->Next();
    }
  }

  // The target key is strictly larger than all keys in the
  // current block, and is also known to be strictly smaller than
  // all keys in the next block.
  if (!iter->Valid()) {
    // Special case for some non-existent keys
    // that happen to locate in the gap between two
    // adjacent data blocks
    *exhausted = true;
  }

  for (; iter->Valid(); iter->Next()) {
    if (iter->key() == key) {
      opts.saver(opts.arg, key, iter->value());
      if (options_.mode != kDirMultiMap) {
        break;  // If keys are unique, we are done
      }
    } else {
      assert(iter->key() > key);
      *exhausted = true;
      break;
    }
  }

  status = iter->status();

  delete iter;
  delete block;
  return status;
}

// Check if a specific key may or must not exist in one or more blocks
// indexed by the given filter.
bool Dir::KeyMayMatch(const Slice& key, const BlockHandle& h) {
  Status status;
  BlockContents contents;
  // We always prefetch and cache all filter blocks in memory
  // so there is no need to allocate an additional
  // buffer to store the block contents
  const bool cached = true;
  status = ReadBlock(indx_, options_, h, &contents, cached);
  if (status.ok()) {
    bool r;  // False if key must not match so no need for further access
    if (options_.filter == kDirBloomFilter) {
      r = BloomKeyMayMatch(key, contents.data);
    } else if (options_.filter == kDirBitmap) {
      r = BitmapKeyMustMatch(key, contents.data);
    } else {  // Unknown filter type
      r = true;
    }
    if (contents.heap_allocated) {
      delete[] contents.data.data();
    }
    return r;
  } else {
    return true;
  }
}

// Retrieve value to a specific key from a given table and call "opts.saver"
// using the value found. Filter will be consulted if available to avoid
// unnecessary reads. Return OK on success and a non-OK status on errors.
Status Dir::Fetch(const FetchOptions& opts, const Slice& key,
                  const TableHandle& h) {
  Status status;
  // Check table key range and the paired filter
  if (key < h.smallest_key() || key > h.largest_key()) {
    return status;
  } else if (!options_.ignore_filters) {
    BlockHandle filter_handle;
    filter_handle.set_offset(h.filter_offset());
    filter_handle.set_size(h.filter_size());
    if (filter_handle.size() != 0) {  // Filter detected
      if (!KeyMayMatch(key, filter_handle)) {
        // Assuming no false negatives
        return status;
      }
    }
  }

  // Load the index block
  BlockContents index_contents;
  BlockHandle index_handle;
  index_handle.set_offset(h.index_offset());
  index_handle.set_size(h.index_size());
  // We always prefetch and cache all index blocks in memory
  // so there is no need to allocate an additional
  // buffer to store the block contents
  const bool cached = true;
  status = ReadBlock(indx_, options_, index_handle, &index_contents, cached);
  if (!status.ok()) {
    return status;
  } else {
    opts.stats->table_seeks++;
  }

  Block* index_block = new Block(index_contents);
  Iterator* const iter = index_block->NewIterator(BytewiseComparator());
  if (options_.mode != kDirMultiMap) {
    iter->Seek(key);  // Binary search
  } else {
    iter->SeekToFirst();
    while (iter->Valid() && key > iter->key()) {
      iter->Next();
    }
  }

  bool exhausted = false;
  for (; iter->Valid(); iter->Next()) {
    BlockHandle block_handle;
    Slice input = iter->value();
    status = block_handle.DecodeFrom(&input);
    if (status.ok()) {
      status = Fetch(opts, key, block_handle, &exhausted);
    }

    if (!status.ok()) {
      break;
    } else if (options_.mode != kDirMultiMap) {
      break;
    } else if (exhausted) {
      break;
    }
  }

  if (status.ok()) {
    status = iter->status();
  }

  delete iter;
  delete index_block;
  return status;
}

namespace {
struct SaverState {
  std::string* dst;
  bool found;
};

static void SaveValue(void* arg, const Slice& key, const Slice& value) {
  SaverState* state = reinterpret_cast<SaverState*>(arg);
  state->dst->append(value.data(), value.size());
  state->found = true;
}

struct ParaSaverState : public SaverState {
  uint32_t epoch;
  std::vector<uint32_t>* offsets;
  std::string* buffer;
  port::Mutex* mu;
};

static void ParaSaveValue(void* arg, const Slice& key, const Slice& value) {
  ParaSaverState* state = reinterpret_cast<ParaSaverState*>(arg);
  MutexLock ml(state->mu);
  state->offsets->push_back(static_cast<uint32_t>(state->buffer->size()));
  PutVarint32(state->buffer, state->epoch);
  PutLengthPrefixedSlice(state->buffer, value);
  state->found = true;
}

static inline Iterator* NewRtIterator(Block* block) {
  Iterator* iter = block->NewIterator(BytewiseComparator());
  iter->SeekToFirst();
  return iter;
}

}  // namespace

Status Dir::DoGet(const Slice& key, const BlockHandle& h, uint32_t epoch,
                  GetContext* ctx, GetStats* stats) {
  Status status;
  // Load the meta index for the epoch
  BlockContents meta_index_contents;
  // We always prefetch and cache all index blocks in memory
  // so there is no need to allocate an additional
  // buffer to store the block contents
  const bool cached = true;
  status = ReadBlock(indx_, options_, h, &meta_index_contents, cached);
  if (!status.ok()) {
    return status;
  }
  Block* meta_index_block = new Block(meta_index_contents);
  Iterator* const iter = meta_index_block->NewIterator(BytewiseComparator());
  iter->SeekToFirst();
  std::string epoch_table_key;
  uint32_t table = 0;
  for (; status.ok(); table++) {
    epoch_table_key = EpochTableKey(epoch, table);
    // Try reusing current iterator position if possible
    if (!iter->Valid() || iter->key() != epoch_table_key) {
      iter->Seek(epoch_table_key);
      if (!iter->Valid()) {
        break;  // EOF
      } else if (iter->key() != epoch_table_key) {
        break;  // No such table
      }
    }
    ParaSaverState arg;
    arg.epoch = epoch;
    arg.offsets = ctx->offsets;
    arg.buffer = ctx->buffer;
    arg.mu = mu_;
    arg.dst = ctx->dst;
    arg.found = false;
    TableHandle table_handle;
    Slice input = iter->value();
    status = table_handle.DecodeFrom(&input);
    iter->Next();
    if (status.ok()) {
      FetchOptions opts;
      if (options_.epoch_log_rotation) {
        opts.file_index = epoch;
      } else {
        opts.file_index = 0;
      }
      opts.stats = stats;
      opts.tmp_length = ctx->tmp_length;
      opts.tmp = ctx->tmp;
      if (options_.parallel_reads) {
        opts.saver = ParaSaveValue;
        opts.arg = &arg;
        status = Fetch(opts, key, table_handle);
      } else {
        opts.saver = SaveValue;
        opts.arg = &arg;
        status = Fetch(opts, key, table_handle);
      }
      if (status.ok() && arg.found) {
        if (options_.mode != kDirMultiMap) {
          break;
        }
      }
    }
  }

  if (status.ok()) {
    status = iter->status();
  }

  delete iter;
  delete meta_index_block;
  return status;
}

void Dir::Get(const Slice& key, uint32_t epoch, GetContext* ctx) {
  mu_->AssertHeld();
  if (!ctx->status->ok()) {
    return;
  }
  Iterator* rt_iter = ctx->rt_iter;
  if (rt_iter == NULL) {
    rt_iter = NewRtIterator(rt_);
  }
  mu_->Unlock();
  GetStats stats;
  stats.table_seeks = 0;  // Number of tables touched
  // Number of data blocks fetched
  stats.seeks = 0;
  Status status;
  for (uint32_t dummy = epoch; dummy == epoch; dummy++) {
    std::string epoch_key = EpochKey(epoch);
    // Try reusing current iterator position if possible
    if (!rt_iter->Valid() || rt_iter->key() != epoch_key) {
      rt_iter->Seek(epoch_key);
      if (!rt_iter->Valid()) {
        break;  // EOF
      } else if (rt_iter->key() != epoch_key) {
        break;  // No such epoch
      }
    }
    BlockHandle h;
    Slice input = rt_iter->value();
    status = h.DecodeFrom(&input);
    rt_iter->Next();
    if (status.ok()) {
      status = DoGet(key, h, epoch, ctx, &stats);
    } else {
      // Skip the epoch
    }
    break;
  }

  if (status.ok()) {
    status = rt_iter->status();
  }

  mu_->Lock();
  if (rt_iter != ctx->rt_iter) {
    delete rt_iter;
  }
  // Increase the total seek count
  ctx->num_table_seeks += stats.table_seeks;
  ctx->num_seeks += stats.seeks;
  assert(ctx->num_open_reads > 0);
  ctx->num_open_reads--;
  bg_cv_->SignalAll();
  if (ctx->status->ok()) {
    *ctx->status = status;
  }
}

struct Dir::STLLessThan {
  Slice buffer_;

  explicit STLLessThan(const Slice& buffer) : buffer_(buffer) {}

  bool operator()(uint32_t a, uint32_t b) {
    const uint32_t epoch_a = GetEpoch(a);
    const uint32_t epoch_b = GetEpoch(b);
    return epoch_a < epoch_b;
  }

  uint32_t GetEpoch(uint32_t off) {
    uint32_t e = 0;
    const char* p = GetVarint32Ptr(  // Decode epoch number
        buffer_.data() + off, buffer_.data() + buffer_.size(), &e);
    if (p != NULL) {
      return e;
    } else {
      assert(false);
      return e;
    }
  }
};

void Dir::Merge(GetContext* ctx) {
  std::vector<uint32_t>::iterator begin;
  begin = ctx->offsets->begin();
  std::vector<uint32_t>::iterator end;
  end = ctx->offsets->end();
  // A key might appear multiple times within each
  // epoch so the sort must be stable.
  std::stable_sort(begin, end, STLLessThan(*ctx->buffer));

  uint32_t ignored;
  Slice value;
  std::vector<uint32_t>::const_iterator it;
  for (it = ctx->offsets->begin(); it != ctx->offsets->end(); ++it) {
    Slice input = *ctx->buffer;
    input.remove_prefix(*it);
    bool r1 = GetVarint32(&input, &ignored);
    bool r2 = GetLengthPrefixedSlice(&input, &value);
    if (r1 && r2) {
      ctx->dst->append(value.data(), value.size());
    } else {
      assert(false);
    }
  }
}

Status Dir::Read(const Slice& key, std::string* dst, char* tmp,
                 size_t tmp_length, ReadStats* stats) {
  mu_->AssertHeld();
  Status status;
  assert(rt_ != NULL);
  std::vector<uint32_t> offsets;
  std::string buffer;

  GetContext ctx;
  ctx.tmp = tmp;  // User-supplied buffer space
  ctx.tmp_length = tmp_length;
  ctx.num_open_reads = 0;  // Number of outstanding epoch read operations
  ctx.status = &status;
  ctx.offsets = &offsets;
  ctx.buffer = &buffer;
  ctx.num_table_seeks = 0;  // Total number of tables touched
  // Total number of data blocks fetched
  ctx.num_seeks = 0;
  if (!options_.parallel_reads) {
    // Pre-create the root iterator for serial reads
    ctx.rt_iter = NewRtIterator(rt_);
  } else {
    ctx.rt_iter = NULL;
  }
  ctx.dst = dst;
  if (num_epoches_ != 0) {
    uint32_t epoch = 0;
    for (; epoch < num_epoches_; epoch++) {
      ctx.num_open_reads++;
      BGItem item;
      item.epoch = epoch;
      item.dir = this;
      item.ctx = &ctx;
      item.key = key;
      if (!options_.parallel_reads) {
        Get(item.key, item.epoch, item.ctx);
      } else if (options_.reader_pool != NULL) {
        options_.reader_pool->Schedule(Dir::BGWork, &item);
      } else if (options_.allow_env_threads) {
        Env::Default()->Schedule(Dir::BGWork, &item);
      } else {
        Get(item.key, item.epoch, item.ctx);
      }
      if (!status.ok()) {
        break;
      }
    }
  }

  // Wait for all outstanding read operations to conclude
  while (ctx.num_open_reads > 0) {
    bg_cv_->Wait();
  }

  delete ctx.rt_iter;
  // Merge sort read results
  if (status.ok()) {
    if (stats != NULL) {
      stats->total_table_seeks = ctx.num_table_seeks;
      stats->total_seeks = ctx.num_seeks;
    }
    if (options_.parallel_reads) {
      Merge(&ctx);
    }
  }

  return status;
}

void Dir::BGWork(void* arg) {
  BGItem* item = reinterpret_cast<BGItem*>(arg);
  MutexLock ml(item->dir->mu_);
  item->dir->Get(item->key, item->epoch, item->ctx);
}

Dir::Dir(const DirOptions& options, port::Mutex* mu, port::CondVar* bg_cv)
    : options_(options),
      num_epoches_(0),
      data_(NULL),
      indx_(NULL),
      mu_(mu),
      bg_cv_(bg_cv),
      rt_(NULL),
      refs_(0) {}

Dir::~Dir() {
  mu_->AssertHeld();
  if (data_ != NULL) data_->Unref();
  if (indx_ != NULL) indx_->Unref();
  delete rt_;
}

void Dir::InstallDataSource(LogSource* data) {
  if (data != data_) {
    if (data_ != NULL) data_->Unref();
    data_ = data;
    if (data_ != NULL) {
      data_->Ref();
    }
  }
}

template <typename U, typename V>
static inline bool UnMatch(U a, V b) {
  return a != static_cast<U>(b);
}

// Double-check if the options supplied by user code match the directory footer
// fetched from the storage. Return OK if verification passes.
static Status VerifyOptions(const DirOptions& options, const Footer& footer) {
  if (UnMatch(options.lg_parts, footer.lg_parts()) ||
      UnMatch(options.key_size, footer.key_size()) ||
      UnMatch(options.value_size, footer.value_size()) ||
      UnMatch(options.epoch_log_rotation, footer.epoch_log_rotation()) ||
      UnMatch(options.skip_checksums, footer.skip_checksums()) ||
      UnMatch(options.filter, footer.filter_type()) ||
      UnMatch(options.mode, footer.mode())) {
    return Status::AssertionFailed("Options does not match footer");
  } else {
    return Status::OK();
  }
}

Status Dir::Open(LogSource* indx) {
  Status status;
  char space[Footer::kEncodedLength];
  Slice input;
  if (indx->Size() >= sizeof(space)) {
    status =
        indx->Read(indx->Size() - sizeof(space), sizeof(space), &input, space);
  } else {
    status = Status::Corruption("Dir index too short to be valid");
  }

  if (!status.ok()) {
    return status;
  }

  Footer footer;
  status = footer.DecodeFrom(&input);
  if (!status.ok()) {
    return status;
  } else if (options_.paranoid_checks) {
    status = VerifyOptions(options_, footer);
    if (!status.ok()) {
      return status;
    }
  }

  BlockContents contents;
  const BlockHandle& handle = footer.epoch_index_handle();
  status = ReadBlock(indx, options_, handle, &contents, true);
  if (!status.ok()) {
    return status;
  }

  num_epoches_ = footer.num_epochs();
  rt_ = new Block(contents);
  indx_ = indx;
  indx_->Ref();

  return status;
}

}  // namespace plfsio
}  // namespace pdlfs
