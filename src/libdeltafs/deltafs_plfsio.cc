/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio.h"

#include <algorithm>

namespace pdlfs {
extern const char* GetLengthPrefixedSlice(const char* p, const char* limit,
                                          Slice* result);
namespace plfsio {

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

void WriteBuffer::Finish() {
  struct STLLessThan {
    Slice buffer_;
    STLLessThan(const std::string& buffer) : buffer_(buffer) {}
    bool operator()(uint32_t a, uint32_t b) {
      Slice key_a = GetKey(a);
      Slice key_b = GetKey(b);
      assert(!key_a.empty() && !key_b.empty());
      return key_a < key_b;
    }
    Slice GetKey(uint32_t offset) {
      Slice result;
      bool ok = GetLengthPrefixedSlice(
          buffer_.data() + offset,          // Key start
          buffer_.data() + buffer_.size(),  // Space limit
          &result);
      if (ok) {
        return result;
      } else {
        assert(false);
        return result;
      }
    }
  };

  // Sort entries
  assert(!finished_);
  std::vector<uint32_t>::iterator begin = offsets_.begin();
  std::vector<uint32_t>::iterator end = offsets_.end();
  std::sort(begin, end, STLLessThan(buffer_));
  finished_ = true;
}

void WriteBuffer::Reset() {
  num_entries_ = 0;
  finished_ = false;
  offsets_.clear();
  buffer_.clear();
}

void WriteBuffer::Reserve(uint32_t num_entries, size_t size_per_entry) {
  buffer_.reserve(num_entries * (size_per_entry + 2));
  offsets_.reserve(num_entries);
}

void WriteBuffer::Add(const Slice& key, const Slice& value) {
  assert(!finished_);       // Finish() has not been called
  assert(key.size() != 0);  // Key cannot be empty
  size_t offset = buffer_.size();
  PutLengthPrefixedSlice(&buffer_, key);
  PutLengthPrefixedSlice(&buffer_, value);
  offsets_.push_back(offset);
  num_entries_++;
}

TableLogger::TableLogger(const Options& options, LogSink* data, LogSink* index)
    : options_(options),
      data_block_(kDataBlkRestartInt),
      index_block_(kNonDataBlkRestartInt),
      epoch_block_(kNonDataBlkRestartInt),
      pending_index_entry_(false),
      pending_epoch_entry_(false),
      num_tables_(0),
      num_epoches_(0),
      data_log_(data),
      index_log_(index),
      finished_(false) {}

void TableLogger::EndEpoch() {
  assert(!finished_);  // Finish() has not been called
  EndTable();
  if (ok() && num_tables_ != 0) {
    num_tables_ = 0;
    assert(num_epoches_ < kMaxEpoches);
    num_epoches_++;
  }
}

void TableLogger::EndTable() {
  assert(!finished_);  // Finish() has not been called
  EndBlock();
  if (!ok()) return;  // Abort
  if (pending_index_entry_) {
    assert(data_block_.empty());
    BytewiseComparator()->FindShortSuccessor(&last_key_);
    std::string handle_encoding;
    pending_index_handle_.EncodeTo(&handle_encoding);
    index_block_.Add(last_key_, handle_encoding);
    pending_index_entry_ = false;
  } else if (index_block_.empty()) {
    return;  // No more work
  }

  assert(!pending_epoch_entry_);
  Slice contents = index_block_.Finish();
  uint64_t index_offset = index_log_->Ltell();
  status_ = index_log_->Lwrite(contents);

  if (ok()) {
    index_block_.Reset();
    pending_epoch_handle_.set_size(contents.size());
    pending_epoch_handle_.set_offset(index_offset);
    pending_epoch_entry_ = true;
  }

  if (pending_epoch_entry_) {
    assert(index_block_.empty());
    pending_epoch_handle_.set_smallest_key(smallest_key_);
    BytewiseComparator()->FindShortSuccessor(&largest_key_);
    pending_epoch_handle_.set_largest_key(largest_key_);
    std::string handle_encoding;
    pending_epoch_handle_.EncodeTo(&handle_encoding);
    epoch_block_.Add(EpochKey(num_epoches_, num_tables_), handle_encoding);
    pending_epoch_entry_ = false;
  }

  if (ok()) {
    smallest_key_.clear();
    largest_key_.clear();
    last_key_.clear();
    assert(num_tables_ < kMaxTablesPerEpoch);
    num_tables_++;
  }
}

void TableLogger::EndBlock() {
  assert(!finished_);               // Finish() has not been called
  if (data_block_.empty()) return;  // No more work
  if (!ok()) return;                // Abort
  assert(!pending_index_entry_);
  Slice contents = data_block_.Finish();
  uint64_t data_offset = data_log_->Ltell();
  status_ = data_log_->Lwrite(contents);
  if (ok()) {
    data_block_.Reset();
    pending_index_handle_.set_size(contents.size());
    pending_index_handle_.set_offset(data_offset);
    pending_index_entry_ = true;
  }
}

void TableLogger::Add(const Slice& key, const Slice& value) {
  assert(!finished_);       // Finish() has not been called
  assert(key.size() != 0);  // Key cannot be empty
  if (!ok()) return;        // Abort
  largest_key_ = key.ToString();
  if (!smallest_key_.empty()) {
    smallest_key_ = key.ToString();
  }

  if (!last_key_.empty()) {
    // Keys within a single table are expected to be added in a sorted order
    // and we don't allow duplicated keys
    assert(key.compare(last_key_) > 0);
  }

  // Add an index entry if there is one pending insertion
  if (pending_index_entry_) {
    assert(data_block_.empty());
    BytewiseComparator()->FindShortestSeparator(&last_key_, key);
    std::string handle_encoding;
    pending_index_handle_.EncodeTo(&handle_encoding);
    index_block_.Add(last_key_, handle_encoding);
    pending_index_entry_ = false;
  }

  last_key_ = key.ToString();
  data_block_.Add(key, value);
  if (data_block_.CurrentSizeEstimate() >= options_.block_size) {
    EndBlock();
  }
}

Status TableLogger::Finish() {
  assert(!finished_);
  EndEpoch();
  finished_ = true;
  if (!ok()) return status_;
  Slice contents;
  BlockHandle epoch_index_handle;
  std::string tail;
  Footer footer;

  assert(!pending_epoch_entry_);
  contents = epoch_block_.Finish();
  epoch_index_handle.set_size(contents.size());
  epoch_index_handle.set_offset(index_log_->Ltell());

  footer.set_epoch_index_handle(epoch_index_handle);
  footer.set_num_epoches(num_epoches_);
  footer.EncodeTo(&tail);

  status_ = index_log_->Lwrite(contents);
  if (status_.ok()) {
    status_ = index_log_->Lwrite(tail);
  }

  return status_;
}

IOLogger::IOLogger(const Options& options, port::Mutex* mu, LogSink* data,
                   LogSink* index)
    : options_(options),
      mutex_(mu),
      bg_cv_(mu),
      has_bg_compaction_(false),
      table_logger_(options, data, index),
      mem_buf_(NULL),
      imm_buf_(NULL) {
  mem_buf_ = &buf0_;
}

IOLogger::~IOLogger() {
  mutex_->AssertHeld();
  while (has_bg_compaction_) {
    bg_cv_.Wait();
  }
}

Status IOLogger::MakeEpoch() {
  mutex_->AssertHeld();
  Status status = PrepareForIncomingWrite(true);
  return status;
}

Status IOLogger::Add(const Slice& key, const Slice& value) {
  mutex_->AssertHeld();
  Status status = PrepareForIncomingWrite(false);
  if (status.ok()) {
    mem_buf_->Add(key, value);
  }

  return status;
}

Status IOLogger::PrepareForIncomingWrite(bool force) {
  mutex_->AssertHeld();
  Status status;
  assert(mem_buf_ != NULL);
  if (!table_logger_.ok()) {
    status = table_logger_.status();
  } else if (!force && mem_buf_->CurrentBufferSize() < options_.table_size) {
    // There is room in current write buffer
  } else if (imm_buf_ != NULL) {
    status = Status::BufferFull(Slice());
  } else {
    // Attempt to switch to a new write buffer
    mem_buf_->Finish();
    imm_buf_ = mem_buf_;
    WriteBuffer* mem_buf = mem_buf_;
    MaybeSchedualCompaction();
    if (mem_buf == &buf0_) {
      mem_buf_ = &buf1_;
    } else {
      mem_buf_ = &buf0_;
    }
  }

  return status;
}

void IOLogger::MaybeSchedualCompaction() {
  mutex_->AssertHeld();
  if (imm_buf_ != NULL && !has_bg_compaction_) {
    has_bg_compaction_ = true;
    if (options_.compaction_pool != NULL) {
      options_.compaction_pool->Schedule(IOLogger::BGWork, this);
    } else {
      // XXX: Directly run in current thread context
      CompactWriteBuffer();
      has_bg_compaction_ = false;
      bg_cv_.SignalAll();
    }
  }
}

void IOLogger::BGWork(void* arg) {
  IOLogger* io = reinterpret_cast<IOLogger*>(arg);
  io->mutex_->Lock();
  assert(io->has_bg_compaction_);
  io->CompactWriteBuffer();
  io->has_bg_compaction_ = false;
  io->bg_cv_.SignalAll();
  io->mutex_->Unlock();
}

void IOLogger::CompactWriteBuffer() {
  mutex_->AssertHeld();
  TableLogger* const dest = &table_logger_;
  WriteBuffer* const buffer = imm_buf_;
  if (buffer != NULL) {
    imm_buf_ = NULL;
    mutex_->Unlock();
    Iterator* iter = buffer->NewIterator();
    iter->SeekToFirst();
    for (; iter->Valid(); iter->Next()) {
      dest->Add(iter->key(), iter->value());
      if (!dest->ok()) {
        break;
      }
    }

    if (dest->ok()) {
      dest->EndTable();
    }
    delete iter;
    buffer->Reset();
    mutex_->Lock();
  }
}

}  // namespace plfsio
}  // namespace pdlfs
