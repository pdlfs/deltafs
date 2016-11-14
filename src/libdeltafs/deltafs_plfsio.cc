/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio.h"
#include "pdlfs-common/coding.h"
#include "pdlfs-common/env.h"

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

TableLogger::TableLogger(const Options& options, LogFile* data, LogFile* index)
    : options_(options),
      data_block_(16),
      index_block_(1),
      epoch_block_(1),
      pending_index_entry_(false),
      pending_epoch_entry_(false),
      data_offset_(0),
      index_offset_(0),
      data_log_(data),
      index_log_(index),
      num_tables_(0),
      num_epoches_(0),
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
  status_ = index_log_->Append(contents);

  if (ok()) {
    index_block_.Reset();
    pending_epoch_handle_.set_size(contents.size());
    pending_epoch_handle_.set_offset(index_offset_);
    pending_epoch_entry_ = true;
    index_offset_ += contents.size();
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
  status_ = data_log_->Append(contents);
  if (ok()) {
    data_block_.Reset();
    pending_index_handle_.set_size(contents.size());
    pending_index_handle_.set_offset(data_offset_);
    pending_index_entry_ = true;
    data_offset_ += contents.size();
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
  BlockHandle epoch_index_handle;
  std::string tail;
  Footer footer;

  assert(!pending_epoch_entry_);
  Slice contents = epoch_block_.Finish();
  status_ = index_log_->Append(contents);

  if (ok()) {
    epoch_index_handle.set_size(contents.size());
    epoch_index_handle.set_offset(index_offset_);
    index_offset_ += contents.size();
  }

  if (ok()) {
    footer.set_epoch_index_handle(epoch_index_handle);
    footer.set_num_epoches(num_epoches_);
    footer.EncodeTo(&tail);
    status_ = index_log_->Append(tail);
  }

  if (ok()) {
    index_offset_ += tail.size();
  }
  return status_;
}

}  // namespace plfsio
}  // namespace pdlfs
