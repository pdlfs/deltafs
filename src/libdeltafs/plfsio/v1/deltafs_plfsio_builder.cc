/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_builder.h"
#include "deltafs_plfsio_recov.h"

#include <math.h>

namespace pdlfs {
namespace plfsio {

DirOutputStats::DirOutputStats()
    : total_num_keys_(0),
      total_num_dropped_keys_(0),
      total_num_blocks_(0),
      total_num_tables_(0),
      final_data_size(0),
      data_size(0),
      final_meta_index_size(0),
      meta_index_size(0),
      final_index_size(0),
      index_size(0),
      final_filter_size(0),
      filter_size(0),
      value_size(0),
      key_size(0) {}

DirBuilder::DirBuilder(const DirOptions& options, DirOutputStats* stats)
    : options_(options),
      compac_stats_(stats),
      num_entries_(0),  // Within the current epoch
      num_tabls_(0),    // Within the current epoch
      num_eps_(0) {}

DirBuilder::~DirBuilder() {}

void ArrayBlockBuilder::Add(const Slice& key, const Slice& value) {
  assert(key.size() == key_size_);
  buffer_.append(key.data(), key_size_);
  assert(value.size() == value_size_);
  buffer_.append(value.data(), value_size_);
}

Slice ArrayBlockBuilder::Finish(CompressionType compression,
                                bool force_compression) {
  assert(!finished_);
  // Remember key value sizes for later retrieval
  PutFixed32(&buffer_, value_size_);
  PutFixed32(&buffer_, key_size_);
  return AbstractBlockBuilder::Finish(compression, force_compression);
}

size_t ArrayBlockBuilder::CurrentSizeEstimate() const {
  size_t result = buffer_.size() - buffer_start_;
  if (!finished_) {
    return result + 8;
  } else {
    return result;
  }
}

ArrayBlock::ArrayBlock(const BlockContents& contents)
    : data_(contents.data.data()),
      size_(contents.data.size()),
      owned_(contents.heap_allocated),
      value_size_(0),
      key_size_(0),
      limit_(0) {
  if (size_ < 2 * sizeof(uint32_t)) {
    size_ = 0;  // Error marker
  } else {
    value_size_ = DecodeFixed32(data_ + size_ - 2 * sizeof(uint32_t));
    key_size_ = DecodeFixed32(data_ + size_ - sizeof(uint32_t));
    if (key_size_ == 0) {
      // Keys cannot be empty
      size_ = 0;
    }
  }

  if (size_ == 0) {
    return;
  }
  limit_ = size_ - (2 * sizeof(uint32_t));
  uint32_t max_entries = limit_ / (key_size_ + value_size_);
  // Adjust limit to match key value sizes
  limit_ = max_entries * (key_size_ + value_size_);
}

ArrayBlock::~ArrayBlock() {
  if (owned_) {
    delete[] data_;
  }
}

class ArrayBlock::Iter : public Iterator {
 private:
  const Comparator* const comparator_;
  const char* const data_;  // Underlying block contents
  const uint32_t limit_;
  const uint32_t value_size_;
  const uint32_t key_size_;
  // Offset in data_ of current entry.  >= limit_ if !Valid
  uint32_t current_;

  Status status_;

  inline int Compare(const Slice& a, const Slice& b) const {
    assert(comparator_ == BytewiseComparator());
    return a.compare(b);
  }

 public:
  Iter(const Comparator* comparator, const char* data, uint32_t limit,
       uint32_t value_size, uint32_t key_size)
      : comparator_(comparator),
        data_(data),
        limit_(limit),
        value_size_(value_size),
        key_size_(key_size),
        current_(limit) {
    assert(limit_ % (key_size_ + value_size_) == 0);
  }

  virtual ~Iter() {}
  virtual bool Valid() const { return current_ < limit_; }
  virtual Status status() const { return status_; }
  virtual Slice key() const {
    assert(Valid());
    return Slice(data_ + current_, key_size_);
  }

  virtual Slice value() const {
    assert(Valid());
    return Slice(data_ + current_ + key_size_, value_size_);
  }

  virtual void Next() {
    assert(Valid());
    current_ += key_size_ + value_size_;
  }

  virtual void Prev() {
    assert(Valid());
    if (current_ >= key_size_ + value_size_) {
      current_ -= key_size_ + value_size_;
    } else {
      // No more entries
      current_ = limit_;
    }
  }

  virtual void SeekToFirst() { current_ = 0; }

  virtual void SeekToLast() {
    if (limit_ >= key_size_ + value_size_) {
      current_ = limit_ - key_size_ - value_size_;
    } else {
      current_ = 0;
    }
  }

  // If comparator_ is not NULL, keys are considered ordered and we use binary
  // search to find the target. Otherwise, linear search is used.
  virtual void Seek(const Slice& target) {
    const uint32_t entry_size = key_size_ + value_size_;
    uint32_t num_entries = limit_ / entry_size;
    if (comparator_ != NULL && num_entries != 0) {
      uint32_t left = 0;
      uint32_t right = num_entries - 1;
      while (left < right) {
        uint32_t mid = (left + right + 1) / 2;
        Slice mid_key = Slice(data_ + mid * entry_size, key_size_);
        if (Compare(mid_key, target) < 0) {
          // Key at mid is smaller than target.
          left = mid;
        } else {
          // Key at mid is >= target.
          right = mid - 1;
        }
      }
      current_ = left * entry_size;
      for (; Valid(); Next()) {
        if (Compare(key(), target) >= 0) {
          return;
        }
      }
    } else {
      SeekToFirst();
      for (; Valid(); Next()) {
        if (key() == target) {
          return;
        }
      }
    }
  }
};

// Return an iterator to the block contents. The result should be deleted when
// no longer needed.
Iterator* ArrayBlock::NewIterator(const Comparator* comparator) {
  if (size_ < 2 * sizeof(uint32_t)) {
    return NewErrorIterator(
        Status::Corruption("Cannot understand block contents"));
  } else if (limit_ != 0) {
    return new Iter(comparator, data_, limit_, value_size_, key_size_);
  } else {
    return NewEmptyIterator();
  }
}

template <typename T>
SeqDirBuilder<T>::SeqDirBuilder(const DirOptions& options,
                                DirOutputStats* stats, LogSink* data,
                                LogSink* indx)
    : DirBuilder(options, stats),
      num_uncommitted_indx_(0),
      num_uncommitted_data_(0),
      pending_restart_(false),
      pending_commit_(false),
      data_block_(new T(options)),
      indx_block_(1),
      epok_block_(1),
      root_block_(1),
      pending_indx_entry_(false),
      pending_meta_entry_(false),
      pending_root_entry_(false),
      pending_data_flush_(0),
      pending_indx_flush_(0),
      data_sink_(data),
      data_offset_(0),
      indx_writter_(new LogWriter(options, indx)),
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
  epok_block_.Reserve(estimated_meta_index_size_per_epoch);
  const size_t estimated_root_index = 4 << 10;
  root_block_.Reserve(estimated_root_index);

  block_threshold_ =
      static_cast<size_t>(floor(options_.block_size * options_.block_util));
  uncommitted_indexes_.reserve(1 << 10);
  if (options_.block_batch_size != 0)
    data_block_->buffer_store()->reserve(options_.block_batch_size);
  data_block_->buffer_store()->clear();
  pending_restart_ = true;
}

template <typename T>
SeqDirBuilder<T>::~SeqDirBuilder() {
  indx_sink_->Unref();
  data_sink_->Unref();
  delete indx_writter_;
  delete data_block_;
}

template <typename T>
void SeqDirBuilder<T>::FinishEpoch(uint32_t ep_seq) {
  assert(!finished_);  // Finish() has not been called
  // Skip epochs already finished
  if (ep_seq < num_eps_) return;
  EndTable(Slice(), static_cast<ChunkType>(0) /*Invalid*/);
  if (!ok()) return;
  if (num_tabls_ == 0) {  // Empty epoch
    // Empty epochs are skipped. But we need to remember their existence.
    num_eps_ = ep_seq + 1;
    return;
  }
  EpochStone stone;

  BlockHandle epok_block_handle;
  Slice epok_index_contents = epok_block_.Finish();
  status_ =
      indx_writter_->Write(kMetaChunk, epok_index_contents, &epok_block_handle);
  if (!ok()) {
    return;
  }

  const uint64_t meta_index_size = epok_index_contents.size();
  const uint64_t final_meta_index_size =
      epok_block_handle.size() + kBlockTrailerSize;
  compac_stats_->final_meta_index_size += final_meta_index_size;
  compac_stats_->meta_index_size += meta_index_size;

  epok_block_.Reset();
  last_epok_info_.set_index_offset(epok_block_handle.offset());
  last_epok_info_.set_index_size(epok_block_handle.size());
  last_epok_info_.set_num_tables(num_tabls_);
  last_epok_info_.set_num_ents(num_entries_);
  assert(!pending_root_entry_);
  pending_root_entry_ = true;

  std::string handle_encoding;
  last_epok_info_.EncodeTo(&handle_encoding);
  root_block_.Add(EpochKey(num_eps_), handle_encoding);
  pending_root_entry_ = false;

  stone.set_handle(epok_block_handle);
  stone.set_id(num_eps_);
  std::string epoch_stone;
  stone.EncodeTo(&epoch_stone);
  status_ = indx_writter_->SealEpoch(epoch_stone);
  if (!ok()) {
    return;
  }

  pending_indx_flush_ = indx_sink_->Ltell();
  pending_data_flush_ = data_offset_;

  if (ok()) {
    num_eps_ = ep_seq + 1;  // Flush up-to the requested epoch seq
#ifndef NDEBUG
    // Keys are only required to be unique within an epoch
    keys_.clear();
#endif
    num_entries_ = 0;
    num_tabls_ = 0;
  }
}

template <typename T>
void SeqDirBuilder<T>::EndTable(const Slice& filter_contents,
                                ChunkType filter_type) {
  assert(!finished_);  // Finish() has not been called

  EndBlock();
  if (!ok()) {
    return;
  } else if (pending_indx_entry_) {
    BytewiseComparator()->FindShortSuccessor(&last_key_);
    PutLengthPrefixedSlice(&uncommitted_indexes_, last_key_);
    last_data_info_.EncodeTo(&uncommitted_indexes_);
    pending_indx_entry_ = false;
    num_uncommitted_indx_++;
  }

  Commit();
  if (!ok()) {
    return;
  } else if (indx_block_.empty()) {
    return;  // Empty table
  }

  BlockHandle index_block_handle;
  Slice index_contents = indx_block_.Finish();
  status_ =
      indx_writter_->Write(kIdxChunk, index_contents, &index_block_handle);
  if (!ok()) {
    return;
  }

  const uint64_t index_size = index_contents.size();
  const uint64_t final_index_size =
      index_block_handle.size() + kBlockTrailerSize;
  compac_stats_->final_index_size += final_index_size;
  compac_stats_->index_size += index_size;

  BlockHandle filter_handle;
  if (!filter_contents.empty()) {
    status_ =
        indx_writter_->Write(filter_type, filter_contents, &filter_handle);
    if (!ok()) {
      return;
    }

    const uint64_t filter_size = filter_contents.size();
    const uint64_t final_filter_size = filter_handle.size() + kBlockTrailerSize;
    compac_stats_->final_filter_size += final_filter_size;
    compac_stats_->filter_size += filter_size;
  } else {
    filter_handle.set_offset(0);  // No filter installed
    filter_handle.set_size(0);
  }

  indx_block_.Reset();
  last_tabl_info_.set_filter_offset(filter_handle.offset());
  last_tabl_info_.set_filter_size(filter_handle.size());
  last_tabl_info_.set_index_offset(index_block_handle.offset());
  last_tabl_info_.set_index_size(index_block_handle.size());
  assert(!pending_meta_entry_);
  pending_meta_entry_ = true;

  last_tabl_info_.set_smallest_key(smallest_key_);
  BytewiseComparator()->FindShortSuccessor(&largest_key_);
  last_tabl_info_.set_largest_key(largest_key_);
  std::string handle_encoding;
  last_tabl_info_.EncodeTo(&handle_encoding);
  epok_block_.Add(EpochTableKey(num_eps_, num_tabls_), handle_encoding);
  pending_meta_entry_ = false;

  compac_stats_->total_num_tables_++;
  num_tabls_++;  // Num of tables within an epoch
  smallest_key_.clear();
  largest_key_.clear();
  last_key_.clear();
}

template <typename T>
void SeqDirBuilder<T>::Commit() {
  assert(!finished_);  // Finish() has not been called
  // Skip empty commit
  if (data_block_->buffer_store()->empty()) return;
  if (!ok()) return;  // Abort

  assert(num_uncommitted_data_ == num_uncommitted_indx_);
  std::string* const buffer = data_block_->buffer_store();

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
  data_block_->buffer_store()->clear();
  pending_restart_ = true;
}

template <typename T>
void SeqDirBuilder<T>::EndBlock() {
  assert(!finished_);                // Finish() has not been called
  if (pending_restart_) return;      // Empty block
  if (data_block_->empty()) return;  // Empty block
  if (!ok()) return;                 // Abort

  // | <------------ options_.block_size (e.g. 32KB) ------------> |
  //   block handle   block contents  block trailer  block padding
  //                | <---------- final block contents ----------> |
  //                          (LevelDb compatible layout)
  Slice block_contents =
      data_block_->Finish(options_.compression, options_.force_compression);
  const size_t block_size = block_contents.size();
  Slice final_block_contents;  // With the trailer and any inserted padding
  if (options_.block_padding) {
    // Target size for the final block contents after padding
    size_t padding_target =
        options_.block_size - BlockHandle::kMaxEncodedLength;
    while (padding_target < block_size + kBlockTrailerSize)
      padding_target += options_.block_size;
    final_block_contents = data_block_->Finalize(
        !options_.skip_checksums, static_cast<uint32_t>(padding_target),
        static_cast<char>(0xff));
  } else {
    final_block_contents = data_block_->Finalize(!options_.skip_checksums);
  }

  const size_t final_block_size = final_block_contents.size();
  const uint64_t block_offset =
      data_block_->buffer_store()->size() - final_block_size;
  compac_stats_->final_data_size += final_block_size;
  compac_stats_->data_size += block_size;

  if (ok()) {
    compac_stats_->total_num_blocks_++;
    pending_restart_ = true;
    last_data_info_.set_size(block_size);
    last_data_info_.set_offset(block_offset);
    assert(!pending_indx_entry_);
    pending_indx_entry_ = true;
    num_uncommitted_data_++;
  }
}

template <typename T>
void SeqDirBuilder<T>::Add(const Slice& key, const Slice& value) {
  assert(!finished_);       // Finish() has not been called
  assert(key.size() != 0);  // Keys cannot be empty
  if (!ok()) return;        // Abort

  if (IsKeyUnOrdered(options_.mode)) {
    if (smallest_key_.empty() || key < smallest_key_) {
      smallest_key_ = key.ToString();
    }
    if (largest_key_.empty() || key > largest_key_) {
      largest_key_ = key.ToString();
    }
  } else {  // Keys within a single table are inserted in a weakly sorted order
    if (!last_key_.empty()) {
      assert(key >= last_key_);
      if (options_.mode == kDmUniqueDrop) {  // Ignore duplicates
        if (key == last_key_) {
          compac_stats_->total_num_dropped_keys_++;
          return;  // Drop it
        }
      } else if (IsKeyUnique(options_.mode)) {
        assert(key != last_key_);  // Keys are strongly ordered. No duplicates
      }
    }
    if (smallest_key_.empty()) {
      smallest_key_ = key.ToString();
    }
    largest_key_ = key.ToString();
  }

  // Add an index entry if there is one pending insertion
  if (pending_indx_entry_) {
    BytewiseComparator()->FindShortestSeparator(&last_key_, key);
    PutLengthPrefixedSlice(&uncommitted_indexes_, last_key_);
    last_data_info_.EncodeTo(&uncommitted_indexes_);
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
    data_block_->TEST_SwitchBuffer(
        NULL);  // Continue appending to the same underlying buffer
    // Pre-reserve enough space for the leading block handle
    data_block_->TEST_Pad(BlockHandle::kMaxEncodedLength);
    data_block_->Reset();
  }

  last_key_ = key.ToString();
  compac_stats_->value_size += value.size();
  compac_stats_->key_size += key.size();
#ifndef NDEBUG
  if (options_.mode == kDmUniqueKey) {
    assert(keys_.count(last_key_) == 0);
    keys_.insert(last_key_);
  }
#endif

  data_block_->Add(key, value);
  compac_stats_->total_num_keys_++;
  num_entries_++;  // Num key-value entries within an epoch
  if (IsKeyUnOrdered(options_.mode)) {
    return;  // Force one block per table
  }
  if (data_block_->CurrentSizeEstimate() + kBlockTrailerSize +
          BlockHandle::kMaxEncodedLength >=
      block_threshold_) {
    EndBlock();
    // Schedule buffer commit if it is about to full
    if (data_block_->buffer_store()->size() + options_.block_size >
        options_.block_batch_size) {
      pending_commit_ = true;
    }
  }
}

template <typename T>
void SeqDirBuilder<T>::Finish(uint32_t ep_seq) {
  assert(!finished_);  // Finish() has not been called
  if (ep_seq < num_eps_) ep_seq = num_eps_;
  EndTable(Slice(), static_cast<ChunkType>(0) /*Invalid*/);
  // Only establish a new epoch if we have pending epoch contents
  if (ok() && num_tabls_ != 0) FinishEpoch(ep_seq);
  finished_ = true;
  if (!ok()) {
    return;
  }

  std::string footer_buf;
  Footer footer = Mkfoot(options_);
  assert(!pending_indx_entry_);
  assert(!pending_meta_entry_);
  assert(!pending_root_entry_);

  BlockHandle root_block_handle;
  Slice root_block_contents = root_block_.Finish();
  status_ =
      indx_writter_->Write(kRtChunk, root_block_contents, &root_block_handle);
  if (!ok()) {
    return;
  }

  const uint64_t root_block_size = root_block_contents.size();
  const uint64_t final_root_block_size =
      root_block_handle.size() + kBlockTrailerSize;
  compac_stats_->final_meta_index_size += final_root_block_size;
  compac_stats_->meta_index_size += root_block_size;

  footer.set_epoch_index_handle(root_block_handle);
  footer.set_num_epochs(num_eps_);
  footer.EncodeTo(&footer_buf);
  status_ = indx_writter_->Finish(footer_buf);
}

template <typename T>
size_t SeqDirBuilder<T>::memory_usage() const {
  size_t result = data_block_->memory_usage();
  result += root_block_.memory_usage();
  result += epok_block_.memory_usage();
  result += indx_block_.memory_usage();
  // XXX: Add index log's LogWriter's memory usage as well
  return result;
}

// Use options to determine block formats.
// Directly return the builder instance. This call won't fail.
DirBuilder* DirBuilder::Open(const DirOptions& options, DirOutputStats* stats,
                             LogSink* data, LogSink* indx) {
  if (!options.leveldb_compatible) {
    if (options.fixed_kv_length) {
      return new SeqDirBuilder<ArrayBlockBuilder>(options, stats, data, indx);
    }
  }

  return new SeqDirBuilder<>(options, stats, data, indx);
}

namespace {

void CleanupArrayBlock(void* arg1, void* arg2) {
  delete reinterpret_cast<ArrayBlock*>(arg1);
}

Iterator* OpenArrayBlock(const Comparator* cmp, const BlockContents& contents) {
  ArrayBlock* array_block = new ArrayBlock(contents);
  Iterator* iter = array_block->NewIterator(cmp);
  iter->RegisterCleanup(CleanupArrayBlock, array_block, NULL);
  return iter;
}

void CleanupBlock(void* arg1, void* arg2) {
  delete reinterpret_cast<Block*>(arg1);
}

Iterator* OpenBlock(const Comparator* cmp, const BlockContents& contents) {
  Block* block = new Block(contents);
  Iterator* iter = block->NewIterator(cmp);
  iter->RegisterCleanup(CleanupBlock, block, NULL);
  return iter;
}

}  // namespace

Iterator* OpenDirBlock  // Use options to determine the block format to use
    (const DirOptions& options, const BlockContents& contents) {
  const Comparator* comparator = BytewiseComparator();
  if (IsKeyUnOrdered(options.mode)) {
    comparator = NULL;
  }
  if (!options.leveldb_compatible) {
    if (options.fixed_kv_length) return OpenArrayBlock(comparator, contents);
    // XXX: should we provide our own block format
    // to support variable kv?
  }

  return OpenBlock(comparator, contents);
}

}  // namespace plfsio
}  // namespace pdlfs
