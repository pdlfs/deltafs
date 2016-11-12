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

void WriteBuffer::Append(const Slice& key, const Slice& value) {
  assert(!finished_);       // Finish() has not been called
  assert(key.size() != 0);  // Key cannot be empty
  size_t offset = buffer_.size();
  PutLengthPrefixedSlice(&buffer_, key);
  PutLengthPrefixedSlice(&buffer_, value);
  offsets_.push_back(offset);
  num_entries_++;
}

}  // namespace plfsio
}  // namespace pdlfs
