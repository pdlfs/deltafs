/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <map>
#include <set>
#include <vector>

#include "pdlfs-common/coding.h"
#include "pdlfs-common/dbfiles.h"
#include "pdlfs-common/leveldb/table_builder.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

#include "../xxhash.h"
#include "db/memtable.h"
#include "format.h"
#include "index_block.h"

namespace pdlfs {

#if 0
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
    ASSERT_TRUE(GetVarint32(input, &last_num_blocks));
    ASSERT_TRUE(GetVarint32(input, &last_starting_block));
    ASSERT_TRUE(GetVarint32(input, &last_starting_prefix_rank));

    for (uint32_t i = 0; i < num_pgs_; i++) {
      uint32_t starting_block;
      uint32_t num_blocks;
      uint32_t starting_prefix_rank;
      ASSERT_TRUE(GetVarint32(input, &num_blocks));
      ASSERT_TRUE(GetVarint32(input, &starting_block));
      ASSERT_TRUE(GetVarint32(input, &starting_prefix_rank));

      PgInfo info;
      info.num_pref = starting_prefix_rank - last_starting_prefix_rank;
      info.num_blks = last_num_blocks;
      pg_info_.push_back(info);

      last_starting_block = starting_block;
      last_num_blocks = num_blocks;
      last_starting_prefix_rank = starting_prefix_rank;
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
#endif

// ---------

class DumbComparatorImpl : public Comparator {
 public:
  virtual const char* Name() const { return "leveldb.DumbComparator"; }

  virtual void FindShortSuccessor(std::string*) const {}

  virtual void FindShortestSeparator(std::string*, const Slice& limit) const {}

  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }
};

static const Comparator* DumbComparator() {
  static DumbComparatorImpl cmp;
  return &cmp;
}

template <typename size_generator>
static size_t GenerateRandomTable(MemTable* mem, Random* rnd,
                                  size_generator* pg_size,
                                  std::vector<std::string>* keys = NULL,
                                  size_t fixed_value_size = 256,
                                  bool hash = true) {
  uint64_t seq = rnd->Next();
  uint64_t prefix_seed = rnd->Next64();
  std::string K, IK, V;
  std::set<uint64_t> prefix_set;
  char tmp[8];
  while (true) {
    uint64_t prefix = prefix_seed++;
    if (hash) {
      prefix = XXH64(&prefix_seed, sizeof(uint64_t), 0);
    }
    if (prefix_set.count(prefix) != 0) {
      continue;
    }
    prefix_set.insert(prefix);
    EncodeFixed64(tmp, prefix);
    K.resize(0);
    K.append(tmp, 8);
    uint64_t suffix_seed = rnd->Next64();
    size_t num_suffixes = pg_size->Next();
    std::set<uint64_t> suffix_set;
    while (suffix_set.size() < num_suffixes) {
      uint64_t suffix = suffix_seed++;
      if (hash) {
        suffix = XXH64(&suffix_seed, sizeof(uint64_t), 0);
      }
      if (suffix_set.count(suffix) != 0) {
        continue;
      }
      suffix_set.insert(suffix);
      EncodeFixed64(tmp, suffix);
      K.resize(8);
      K.append(tmp, 8);
      V.resize(0);
      test::RandomString(rnd, fixed_value_size - 24, &V);
      mem->Add(++seq, kTypeValue, K, V);
      IK.resize(0);
      AppendInternalKey(&IK, ParsedInternalKey(K, seq, kTypeValue));
      if (keys != NULL) {
        keys->push_back(IK);
      }
      if (mem->ApproximateMemoryUsage() >= (1.2 * (32 << 20))) {
        return prefix_set.size();
      }
    }
  }

  return 0;
}

class TableBuilderWrapper {
 public:
  typedef DBOptions Options;

  explicit TableBuilderWrapper(const Options* options) {
    dbname = test::TmpDir() + "/index_block_test";
    env = options->env;
    env->CreateDir(dbname);
    std::string fname = TableFileName(dbname, 1);
    env->DeleteFile(fname);
    ASSERT_OK(env->NewWritableFile(fname, &file));
    builder = new TableBuilder(*options, file);
    opts = options;
    assert(options->comparator != NULL);
    mem = new MemTable(
        reinterpret_cast<const InternalKeyComparator&>(*options->comparator));
    mem->Ref();
  }

  ~TableBuilderWrapper() {
    mem->Unref();
    delete builder;
    delete file;
  }

  template <typename size_generator>
  void CreateTable(Random* rnd, size_generator* size_gen) {
    num_prefiz = GenerateRandomTable(mem, rnd, size_gen, &keys);
    Iterator* it = mem->NewIterator();
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      builder->Add(it->key(), it->value());
    }
    ASSERT_OK(builder->Finish());
    delete it;
  }

  int NumPrefixes() const { return num_prefiz; }

  int NumBlocks() const { return builder->NumBlocks(); }

  int NumKeys() const { return builder->NumEntries(); }

  double TableSize() const {
    return static_cast<double>(builder->FileSize()) / 1024 / 1024;
  }

  double IndexBlockSize() const {
    return static_cast<double>(
               builder->index_builder()->CurrentSizeEstimate()) /
           1024;
  }

  void Report() {
    fprintf(stderr,
            "Keys: %d\tblocks: %d\tkeys_per_block: %.2f\tprefixes: "
            "%d\tkeys_per_prefix: "
            "%.2f\trestarts: %d\ttable_size: %.2fMB\tindex_size: %.2fKB\t"
            "index_bits_per_key: %.2f\n",
            NumKeys(), NumBlocks(),
            static_cast<double>(NumKeys()) / NumBlocks(), NumPrefixes(),
            static_cast<double>(NumKeys()) / NumPrefixes(),
            opts->index_block_restart_interval, TableSize(), IndexBlockSize(),
            IndexBlockSize() * 8 * 1024 / NumKeys());
  }

  const Options* opts;
  std::string dbname;
  size_t num_prefiz;
  std::vector<std::string> keys;
  TableBuilder* builder;
  WritableFile* file;
  MemTable* mem;
  Env* env;
};

class FixedSizeGenerator {
  uint32_t size_;

 public:
  FixedSizeGenerator(uint32_t s) : size_(s) {}
  uint32_t Next() { return size_; }
};

class IndexBench {
 public:
  typedef DBOptions Options;

  explicit IndexBench() {
    options.env = Env::Default();
    options.index_type = kMultiwaySearchTree;
    options.compression = kNoCompression;
    options.comparator = new InternalKeyComparator(BytewiseComparator());
    options.filter_policy = NULL;
    options.index_block_restart_interval = 1;
    options.block_restart_interval = 16;
    options.block_size = 65536;
  }

  ~IndexBench() { delete options.comparator; }

  Options options;
};

TEST(IndexBench, DumbIndexBlockBuilder) {
  for (int pg_size = 1; pg_size <= 4096; pg_size *= 4) {
    for (int restart = 1; restart <= 256; restart *= 4) {
      Random rnd(301);
      delete options.comparator;
      options.comparator = new InternalKeyComparator(DumbComparator());
      options.index_block_restart_interval = restart;
      TableBuilderWrapper builder(&options);
      FixedSizeGenerator size_gen(pg_size);
      builder.CreateTable(&rnd, &size_gen);
      builder.Report();
    }
  }
}

TEST(IndexBench, DefaultIndexBlockBuilder) {
  for (int pg_size = 1; pg_size <= 4096; pg_size *= 4) {
    for (int restart = 1; restart <= 256; restart *= 4) {
      Random rnd(301);
      options.index_block_restart_interval = restart;
      TableBuilderWrapper builder(&options);
      FixedSizeGenerator size_gen(pg_size);
      builder.CreateTable(&rnd, &size_gen);
      builder.Report();
    }
  }
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
