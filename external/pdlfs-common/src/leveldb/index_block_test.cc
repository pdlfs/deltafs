/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <map>
#include <vector>

#include "pdlfs-common/coding.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

#include "format.h"
#include "index_block.h"

namespace pdlfs {

class IndexLoader {
 public:
  struct PgInfo {
    uint32_t num_pref;  // Either 1 or 2
    uint32_t num_blks;
  };

  explicit IndexLoader() {
    num_prefix_ = 0;
    num_pgs_ = 0;
  }

  void LoadIndex(const Slice& contents) {
    ASSERT_TRUE(contents.size() >= 3 * sizeof(uint32_t));
    const char* p = contents.data() + contents.size() - 3 * sizeof(uint32_t);
    prefix_len_ = DecodeFixed32(p);
    p += sizeof(uint32_t);
    size_t pg_start = DecodeFixed32(p);
    p += sizeof(uint32_t);
    size_t blk_start = DecodeFixed32(p);
    Slice input;
    input = Slice(contents.data(), pg_start);
    LoadPrefix(&input);
    ASSERT_TRUE(input.empty());
    input = Slice(contents.data() + pg_start, blk_start - pg_start);
    LoadPg(&input);
    ASSERT_TRUE(input.empty());
  }

  void LoadPg(Slice* input) {
    ASSERT_TRUE(GetVarint32(input, &num_pgs_));
    ASSERT_TRUE(num_pgs_ >= 1);
    num_pgs_--;

    uint32_t last_starting_block;
    uint32_t last_num_blocks;
    uint32_t last_starting_prefix_rank;
    uint32_t last_ending_prefix_rank;
    ASSERT_TRUE(GetVarint32(input, &last_num_blocks));
    ASSERT_TRUE(GetVarint32(input, &last_starting_block));
    ASSERT_TRUE(GetVarint32(input, &last_starting_prefix_rank));
    ASSERT_TRUE(GetVarint32(input, &last_ending_prefix_rank));

    for (uint32_t i = 0; i < num_pgs_; i++) {
      uint32_t starting_block;
      uint32_t num_blocks;
      uint32_t starting_prefix_rank;
      uint32_t ending_prefix_rank;
      ASSERT_TRUE(GetVarint32(input, &num_blocks));
      ASSERT_TRUE(GetVarint32(input, &starting_block));
      ASSERT_TRUE(GetVarint32(input, &starting_prefix_rank));
      ASSERT_TRUE(GetVarint32(input, &ending_prefix_rank));

      PgInfo info;
      info.num_pref = starting_prefix_rank - last_starting_prefix_rank;
      info.num_blks = last_num_blocks;
      pg_info_.push_back(info);

      last_starting_block = starting_block;
      last_num_blocks = num_blocks;
      last_starting_prefix_rank = starting_prefix_rank;
      last_ending_prefix_rank = ending_prefix_rank;
    }

    ASSERT_EQ(num_pgs_, pg_info_.size());
  }

  void LoadPrefix(Slice* input) {
    while (!input->empty()) {
      prefix_.push_back(std::string(input->data(), prefix_len_));
      input->remove_prefix(prefix_len_);
      num_prefix_++;
    }
  }

  std::vector<PgInfo> pg_info_;
  std::vector<std::string> prefix_;
  uint32_t prefix_len_;
  uint32_t num_prefix_;
  uint32_t num_pgs_;
};

class IndexTest {
 public:
  explicit IndexTest() {
    options_.index_type = kCompact;
    builder_ = IndexBuilder::Create(&options_);
    num_blocks_ = 0;
  }

  ~IndexTest() { delete builder_; }

  void AddKey(const Slice& key, bool new_block = false) {
    if (last_key_.size() != 0) {
      builder_->OnKeyAdded(last_key_);
      if (new_block) builder_->AddIndexEntry(&last_key_, &key, MakeBlock());
    }
    last_key_ = key.ToString();
  }

  Slice Finish() {
    if (last_key_.size() != 0) {
      builder_->OnKeyAdded(last_key_);
      builder_->AddIndexEntry(&last_key_, NULL, MakeBlock());
    }
    return builder_->Finish();
  }

  BlockHandle MakeBlock() {
    BlockHandle handle;
    handle.set_size(0);
    handle.set_offset(kBlockTrailerSize * num_blocks_);
    num_blocks_++;
    return handle;
  }

  size_t num_blocks_;
  IndexBuilder* builder_;
  std::string last_key_;
  DBOptions options_;
};

static std::string KeyGen(int prefix, int suffix) {
  char tmp[16];
  EncodeFixed64(tmp, prefix);
  EncodeFixed64(tmp + 8, suffix);
  return std::string(tmp, 16);
}

static std::string KeyPrefixGen(int prefix) {
  char tmp[8];
  EncodeFixed64(tmp, prefix);
  return std::string(tmp, 8);
}

TEST(IndexTest, Empty) {
  Slice contents = Finish();
  IndexLoader loader;
  loader.LoadIndex(contents);
  ASSERT_EQ(loader.num_prefix_, 0);
  ASSERT_EQ(loader.num_pgs_, 0);
}

TEST(IndexTest, SingleBlock) {
  AddKey(KeyGen(1, 1));
  AddKey(KeyGen(1, 2));
  AddKey(KeyGen(2, 1));
  AddKey(KeyGen(2, 2));
  AddKey(KeyGen(3, 1));
  Slice contents = Finish();
  IndexLoader loader;
  loader.LoadIndex(contents);
  ASSERT_EQ(loader.num_prefix_, 2);
  ASSERT_EQ(loader.prefix_[0], KeyPrefixGen(1));
  ASSERT_EQ(loader.prefix_[1], KeyPrefixGen(3));
  ASSERT_EQ(loader.num_pgs_, 1);
  ASSERT_EQ(loader.pg_info_[0].num_pref, 2);
  ASSERT_EQ(loader.pg_info_[0].num_blks, 1);
}

TEST(IndexTest, MultiBlockMultiPg) {
  AddKey(KeyGen(1, 1));
  AddKey(KeyGen(1, 2));
  AddKey(KeyGen(2, 1));
  AddKey(KeyGen(2, 2));
  AddKey(KeyGen(3, 1));
  AddKey(KeyGen(4, 1), true);
  AddKey(KeyGen(4, 2));
  AddKey(KeyGen(5, 1));
  AddKey(KeyGen(5, 2));
  AddKey(KeyGen(6, 1));
  Slice contents = Finish();
  IndexLoader loader;
  loader.LoadIndex(contents);
  ASSERT_EQ(loader.num_prefix_, 4);
  ASSERT_EQ(loader.prefix_[0], KeyPrefixGen(1));
  ASSERT_EQ(loader.prefix_[1], KeyPrefixGen(3));
  ASSERT_EQ(loader.prefix_[2], KeyPrefixGen(4));
  ASSERT_EQ(loader.prefix_[3], KeyPrefixGen(6));
  ASSERT_EQ(loader.num_pgs_, 2);
  ASSERT_EQ(loader.pg_info_[0].num_pref, 2);
  ASSERT_EQ(loader.pg_info_[0].num_blks, 1);
  ASSERT_EQ(loader.pg_info_[1].num_pref, 2);
  ASSERT_EQ(loader.pg_info_[1].num_blks, 1);
}

TEST(IndexTest, MultiBlockCrossingPg1) {
  AddKey(KeyGen(1, 1));
  AddKey(KeyGen(1, 2));
  AddKey(KeyGen(2, 1));
  AddKey(KeyGen(2, 2));
  AddKey(KeyGen(3, 1));
  AddKey(KeyGen(3, 2), true);
  AddKey(KeyGen(3, 3));
  AddKey(KeyGen(4, 1));
  AddKey(KeyGen(4, 2));
  AddKey(KeyGen(5, 1));
  Slice contents = Finish();
  IndexLoader loader;
  loader.LoadIndex(contents);
  ASSERT_EQ(loader.num_prefix_, 5);
  ASSERT_EQ(loader.prefix_[0], KeyPrefixGen(1));
  ASSERT_EQ(loader.prefix_[1], KeyPrefixGen(2));
  ASSERT_EQ(loader.prefix_[2], KeyPrefixGen(3));
  ASSERT_EQ(loader.prefix_[3], KeyPrefixGen(4));
  ASSERT_EQ(loader.prefix_[4], KeyPrefixGen(5));
  ASSERT_EQ(loader.num_pgs_, 3);
  ASSERT_EQ(loader.pg_info_[0].num_pref, 2);
  ASSERT_EQ(loader.pg_info_[0].num_blks, 1);
  ASSERT_EQ(loader.pg_info_[1].num_pref, 1);
  ASSERT_EQ(loader.pg_info_[1].num_blks, 2);
  ASSERT_EQ(loader.pg_info_[2].num_pref, 2);
  ASSERT_EQ(loader.pg_info_[2].num_blks, 1);
}

TEST(IndexTest, MultiBlockCrossingPg2) {
  AddKey(KeyGen(1, 1));
  AddKey(KeyGen(1, 2));
  AddKey(KeyGen(1, 3));
  AddKey(KeyGen(1, 4));
  AddKey(KeyGen(2, 1));
  AddKey(KeyGen(2, 2), true);
  AddKey(KeyGen(2, 3));
  AddKey(KeyGen(3, 1));
  AddKey(KeyGen(3, 2));
  AddKey(KeyGen(3, 3));
  Slice contents = Finish();
  IndexLoader loader;
  loader.LoadIndex(contents);
  ASSERT_EQ(loader.num_prefix_, 3);
  ASSERT_EQ(loader.prefix_[0], KeyPrefixGen(1));
  ASSERT_EQ(loader.prefix_[1], KeyPrefixGen(2));
  ASSERT_EQ(loader.prefix_[2], KeyPrefixGen(3));
  ASSERT_EQ(loader.num_pgs_, 3);
  ASSERT_EQ(loader.pg_info_[0].num_pref, 1);
  ASSERT_EQ(loader.pg_info_[0].num_blks, 1);
  ASSERT_EQ(loader.pg_info_[1].num_pref, 1);
  ASSERT_EQ(loader.pg_info_[1].num_blks, 2);
  ASSERT_EQ(loader.pg_info_[2].num_pref, 1);
  ASSERT_EQ(loader.pg_info_[2].num_blks, 1);
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
