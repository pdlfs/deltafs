/*
 * Copyright (c) 2013 The RocksDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "index_block.h"
#include "block.h"
#include "block_builder.h"
#include "format.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/ect.h"
#include "pdlfs-common/leveldb/iterator.h"

namespace pdlfs {

IndexBuilder::~IndexBuilder() {}

IndexReader::~IndexReader() {}

class DefaultIndexBuilder : public IndexBuilder {
 public:
  DefaultIndexBuilder(const Options* options)
      : index_block_builder_(options->index_block_restart_interval,
                             options->comparator) {}

  virtual void AddIndexEntry(std::string* last_key, const Slice* next_key,
                             const BlockHandle& block_handle) {
    const Comparator* const comparator = index_block_builder_.comparator();
    if (next_key != NULL) {
      comparator->FindShortestSeparator(last_key, *next_key);
    } else {
      comparator->FindShortSuccessor(last_key);
    }

    std::string encoding;
    block_handle.EncodeTo(&encoding);
    index_block_builder_.Add(*last_key, encoding);
  }

  virtual Slice Finish() { return index_block_builder_.Finish(); }

  virtual size_t CurrentSizeEstimate() const {
    return index_block_builder_.CurrentSizeEstimate();
  }

  virtual Status ChangeOptions(const Options* options) {
    index_block_builder_.ChangeRestartInterval(
        options->index_block_restart_interval);
    return Status::OK();
  }

  virtual void OnKeyAdded(const Slice& key) {
    // empty
  }

 private:
  BlockBuilder index_block_builder_;
};

class DefaultIndexReader : public IndexReader {
 public:
  DefaultIndexReader(const BlockContents& contents, const Options* options)
      : cmp_(options->comparator), block_(contents) {}

  virtual size_t ApproximateMemoryUsage() const { return block_.size(); }

  virtual Iterator* NewIterator() { return block_.NewIterator(cmp_); }

 private:
  const Comparator* cmp_;
  Block block_;
};

#if 0
namespace {

// Metadata on a prefix group
struct PgInfo {
  size_t first_prefix;  // Rank of the first prefix key in this
                        // prefix group
  size_t first_block;   // Rank of the first block covered by this
                        // prefix group
  size_t num_blocks;    // Number of blocks covered by this prefix group

  void EncodeTo(std::string* dst) const {
    PutVarint32(dst, num_blocks);
    PutVarint32(dst, first_block);
    PutVarint32(dst, first_prefix);
  }

  bool DecodeFrom(Slice* input) {
    uint32_t v1;
    uint32_t v2;
    uint32_t v3;
    if (!GetVarint32(input, &v3) || !GetVarint32(input, &v2) ||
        !GetVarint32(input, &v1)) {
      return false;
    } else {
      num_blocks = v3;
      first_block = v2;
      first_prefix = v1;
      return true;
    }
  }
};

// Metadata on a single block
struct BlkInfo {
  // Block handle
  size_t offset;  // Offset of the block in the table
  size_t size;    // Dynamically deduced, not stored in index

  Slice suffix;  // Suffix key

  void EncodeTo(std::string* dst) const {
    unsigned char s = suffix.size();
    dst->push_back(static_cast<char>(s));
    dst->append(suffix.data(), s);
    PutVarint32(dst, offset);
  }

  bool DecodeFrom(Slice* input) {
    if (input->size() < 1) {
      return false;
    } else {
      size_t s = static_cast<unsigned char>((*input)[0]);
      input->remove_prefix(1);
      if (input->size() < s) {
        return false;
      } else {
        suffix = Slice(input->data(), s);
        input->remove_prefix(s);
      }
    }
    uint32_t off;
    if (!GetVarint32(input, &off)) {
      return false;
    } else {
      offset = off;
      return true;
    }
  }
};
}

class ThreeLevelCompactIndexBuilder : public IndexBuilder {
  void Flush() {
    seen_new_block_ = false;
    // A prefix group may contain only a single prefix; in such cases,
    // we set the ending prefix to be empty, which reduces the size of the
    // on-disk index representation.
    assert(starting_prefix_.size() == prefix_len_);
    if (starting_prefix_.compare(ending_prefix_) == 0) {
      ending_prefix_.clear();
    } else if (ending_prefix_.size() != 0) {
      assert(ending_prefix_.size() == prefix_len_);
    }

    PgInfo pg;
    size_t r = buffer_.size() / prefix_len_;
    pg.first_prefix = r;
    buffer_.append(starting_prefix_);
    starting_prefix_.clear();
    buffer_.append(ending_prefix_);
    ending_prefix_.clear();
    pg.first_block = starting_block_;
    starting_block_ = n_blocks_;
    pg.num_blocks = n_blocks_ - pg.first_block + 1;
    assert(pg.num_blocks >= 1);
    if (!key_added_into_new_block_) {
      assert(pg.num_blocks > 1);
      pg.num_blocks--;
    }
    pg.EncodeTo(&pg_info_);
    n_pgs_++;
  }

  static Slice ExtractPrefixKey(const Slice& key, size_t prefix_len) {
    assert(key.size() >= prefix_len);
    return Slice(key.data(), prefix_len);
  }

  static Slice ExtractSuffixKey(const Slice& key, size_t prefix_len) {
    Slice suffix = key;
    suffix.remove_prefix(prefix_len);
    return suffix;
  }

 public:
  ThreeLevelCompactIndexBuilder(const Options* options)
      : prefix_len_(8),
        cmp_(options->comparator),
        finished_(false),
        key_added_into_new_block_(false),
        seen_new_block_(false),
        starting_block_(0),
        n_pgs_(0),
        n_blocks_(0),
        n_keys_(0),
        off_(0) {}

  virtual void AddIndexEntry(std::string* last_key, const Slice* next_key,
                             const BlockHandle& handle) {
    assert(!finished_);
    // Any block generated must be non-empty
    assert(last_prefix_.size() != 0);
    if (starting_prefix_.empty()) {
      starting_prefix_ = last_prefix_;
    }

    BlkInfo blk;
    blk.offset = handle.offset();
    assert(blk.offset == off_);
    blk.size = handle.size();

    if (next_key != NULL) {
      Slice last_prefix = ExtractPrefixKey(*last_key, prefix_len_);
      Slice next_prefix = ExtractPrefixKey(*next_key, prefix_len_);

      assert(last_prefix == last_prefix_);
      if (last_prefix == next_prefix) {
        std::string last_suffix =
            ExtractSuffixKey(*last_key, prefix_len_).ToString();
        Slice next_suffix = ExtractSuffixKey(*next_key, prefix_len_);
        cmp_->FindShortestSeparator(&last_suffix, next_suffix);
        blk.suffix = last_suffix;

        // Force splitting the current prefix group if the last prefix
        // is going to span cross a block boundary
        if (last_prefix != starting_prefix_) {
          Flush();
        }
      }
    } else {
      // Last block
      last_prefix_.swap(ending_prefix_);
      last_prefix_.clear();
      Flush();
    }

    off_ += blk.size + kBlockTrailerSize;
    blk.EncodeTo(&blk_info_);
    key_added_into_new_block_ = false;
    seen_new_block_ = true;
    n_blocks_++;
  }

  virtual void OnKeyAdded(const Slice& key) {
    assert(!finished_);
    Slice prefix = ExtractPrefixKey(key, prefix_len_);
    assert(prefix.size() != 0);

    if (last_prefix_.empty()) {
      starting_prefix_ = last_prefix_ = prefix.ToString();
      starting_block_ = 0;
    } else {
      // Prefix must be pre-sorted in the raw byte order
      assert(prefix.compare(last_prefix_) >= 0);
      if (starting_prefix_.empty()) starting_prefix_ = last_prefix_;
      if (prefix.compare(last_prefix_) != 0) {
        last_prefix_.swap(ending_prefix_);
        last_prefix_ = prefix.ToString();
        if (seen_new_block_) {
          Flush();
        }
      }
    }

    key_added_into_new_block_ = true;
    n_keys_++;
  }

  virtual Slice Finish() {
    assert(!finished_);

    // Add a dummy prefix group to serve as a sentinel
    PgInfo pg;
    size_t r = buffer_.size() / prefix_len_;
    pg.first_prefix = r;
    pg.first_block = n_blocks_;
    pg.num_blocks = 0;
    pg.EncodeTo(&pg_info_);
    n_pgs_++;
    size_t pg_start = buffer_.size();
    PutVarint32(&buffer_, n_pgs_);
    buffer_.append(pg_info_);

    // Add a dummy block to serve as a sentinel
    BlkInfo blk;
    blk.offset = off_;
    blk.size = 0;
    blk.EncodeTo(&blk_info_);
    n_blocks_++;
    size_t blk_start = buffer_.size();
    PutVarint32(&buffer_, n_blocks_);
    buffer_.append(blk_info_);

    // Done
    finished_ = true;
    PutFixed32(&buffer_, prefix_len_);
    PutFixed32(&buffer_, pg_start);
    PutFixed32(&buffer_, blk_start);
    return Slice(buffer_);
  }

  virtual size_t CurrentSizeEstimate() const {
    if (!finished_) {
      return buffer_.size() + pg_info_.size() + blk_info_.size() +
             VarintLength(n_pgs_) + VarintLength(n_blocks_) +
             3 * sizeof(uint32_t);
    } else {
      return buffer_.size();
    }
  }

  virtual Status ChangeOptions(const Options* options) {
    return Status::NotSupported(Slice());
  }

 private:
  size_t prefix_len_;
  const Comparator* cmp_;
  bool finished_;
  bool key_added_into_new_block_;
  bool seen_new_block_;
  size_t starting_block_;
  std::string starting_prefix_;
  std::string ending_prefix_;
  std::string last_prefix_;
  std::string pg_info_;
  std::string blk_info_;
  std::string buffer_;
  size_t n_pgs_;
  size_t n_blocks_;
  size_t n_keys_;
  size_t off_;
};

namespace {

class ECTBuilder {
 public:
  ECTBuilder(const Slice& prefix_array, size_t prefix_len) {
    Slice input = prefix_array;
    while (!input.empty()) {
      assert(input.size() >= prefix_len);
      prefix_.push_back(Slice(input.data(), prefix_len));
      input.remove_prefix(prefix_len);
    }
  }

  // Caller should delete the result.
  ECT* ToECT() {
    return ECT::Default(prefix_[0].size(), prefix_.size(), &prefix_[0]);
  }

 private:
  std::vector<Slice> prefix_;
};
}
#endif

IndexBuilder* IndexBuilder::Create(const Options* options) {
  return new DefaultIndexBuilder(options);
}

IndexReader* IndexReader::Create(const BlockContents& contents,
                                 const Options* options) {
  return new DefaultIndexReader(contents, options);
}

}  // namespace pdlfs
