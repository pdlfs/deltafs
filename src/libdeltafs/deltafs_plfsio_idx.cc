/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_idx.h"
#include "deltafs_plfsio_filter.h"

#include <set>

namespace pdlfs {
namespace plfsio {

DirOutputStats::DirOutputStats()
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

DirIndexer::DirIndexer(const DirOptions& options)
    : options_(options),
      total_num_keys_(0),
      total_num_dropped_keys_(0),
      total_num_blocks_(0),
      total_num_tables_(0),
      num_tables_(0),
      num_epochs_(0) {}

DirIndexer::~DirIndexer() {}

// Write sorted directory contents into a pair of log files.
class DirIndexerImpl : public DirIndexer {
 public:
  DirIndexerImpl(const DirOptions& options, LogSink* data, LogSink* indx);
  virtual ~DirIndexerImpl();

  virtual void Add(const Slice& key, const Slice& value);

  // Force the start of a new table.
  // REQUIRES: Finish() has not been called.
  void EndTable(const Slice& filter_contents, ChunkType filter_type);

  // Force the start of a new epoch.
  // REQUIRES: Finish() has not been called.
  virtual void MakeEpoch();

  // Finalize table contents.
  // No further writes.
  virtual Status Finish();

 protected:
  virtual size_t memory_usage() const;

 private:
  // End the current block and force the start of a new data block.
  // REQUIRES: Finish() has not been called.
  void EndBlock();

  // Flush buffered data blocks and finalize their indexes.
  // REQUIRES: Finish() has not been called.
  void Commit();
#ifndef NDEBUG
  // Used to verify the uniqueness of all input keys
  std::set<std::string> keys_;
#endif

  std::string smallest_key_;
  std::string largest_key_;
  std::string last_key_;
  uint32_t num_uncommitted_indx_;  // Number of uncommitted index entries
  uint32_t num_uncommitted_data_;  // Number of uncommitted data blocks
  bool pending_restart_;           // Request to restart the data block buffer
  bool pending_commit_;  // Request to commit buffered data and indexes
  BlockBuilder data_block_;
  BlockBuilder indx_block_;  // Locate the data blocks within a table
  BlockBuilder meta_block_;  // Locate the tables within an epoch
  BlockBuilder root_block_;  // Locate each epoch
  bool pending_indx_entry_;
  BlockHandle pending_indx_handle_;
  bool pending_meta_entry_;
  TableHandle pending_meta_handle_;
  bool pending_root_entry_;
  BlockHandle pending_root_handle_;
  std::string uncommitted_indexes_;
  uint64_t pending_data_flush_;  // Offset of the data pending flush
  uint64_t pending_indx_flush_;  // Offset of the index pending flush
  LogSink* data_sink_;
  uint64_t data_offset_;  // Latest data offset
  LogWriter indx_logger_;
  LogSink* indx_sink_;
  bool finished_;
};

DirIndexerImpl::DirIndexerImpl(const DirOptions& options, LogSink* data,
                               LogSink* indx)
    : DirIndexer(options),
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
  if (options_.block_batch_size != 0)
    data_block_.buffer_store()->reserve(options_.block_batch_size);
  data_block_.buffer_store()->clear();
  pending_restart_ = true;
}

DirIndexerImpl::~DirIndexerImpl() {
  indx_sink_->Unref();
  data_sink_->Unref();
}

void DirIndexerImpl::MakeEpoch() {
  assert(!finished_);  // Finish() has not been called
  EndTable(Slice(), static_cast<ChunkType>(0));
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

void DirIndexerImpl::EndTable(const Slice& filter_contents,
                              ChunkType filter_type) {
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
  if (!filter_contents.empty()) {
    status_ = indx_logger_.Write(filter_type, filter_contents, &filter_handle);
    if (ok()) {
      const uint64_t filter_size = filter_contents.size();
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

void DirIndexerImpl::Commit() {
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

void DirIndexerImpl::EndBlock() {
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

void DirIndexerImpl::Add(const Slice& key, const Slice& value) {
  assert(!finished_);       // Finish() has not been called
  assert(key.size() != 0);  // Keys cannot be empty
  if (!ok()) return;        // Abort

  if (!last_key_.empty()) {
    // Keys within a single table are inserted in a weakly sorted order
    assert(key >= last_key_);
    if (options_.mode == kDmUniqueDrop) {  // Auto deduplicate
      if (key == last_key_) {
        total_num_dropped_keys_++;
        return;  // Drop
      }
    } else if (options_.mode != kDmMultiMap) {
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
    data_block_.ResetBuffer(
        NULL);  // Continue appending to the same underlying buffer
    // Pre-reserve enough space for the leading block handle
    data_block_.Pad(BlockHandle::kMaxEncodedLength);
    data_block_.Reset();
  }

  last_key_ = key.ToString();
  output_stats_.value_size += value.size();
  output_stats_.key_size += key.size();
#ifndef NDEBUG
  if (options_.mode == kDmUniqueKey) {
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

Status DirIndexerImpl::Finish() {
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

size_t DirIndexerImpl::memory_usage() const {
  size_t result = 0;
  result += root_block_.memory_usage();
  result += meta_block_.memory_usage();
  result += indx_block_.memory_usage();
  result += data_block_.memory_usage();
  // XXX: Add index log's LogWriter's memory usage as well
  return result;
}

DirIndexer* DirIndexer::Open(const DirOptions& options, LogSink* data,
                             LogSink* indx) {
  return new DirIndexerImpl(options, data, indx);
}

}  // namespace plfsio
}  // namespace pdlfs
