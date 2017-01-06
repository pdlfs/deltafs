/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_internal.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

#include <map>

namespace pdlfs {
namespace plfsio {

class WriterBufTest {
 public:
  explicit WriterBufTest(uint32_t seed = 301) : num_entries_(0), rnd_(seed) {}

  Iterator* Flush() {
    buffer_.Finish();
    ASSERT_EQ(buffer_.NumEntries(), num_entries_);
    return buffer_.NewIterator();
  }

  void Add(uint64_t seq, size_t value_size = 32) {
    std::string key;
    PutFixed64(&key, seq);
    std::string value;
    test::RandomString(&rnd_, value_size, &value);
    kv_.insert(std::make_pair(key, value));
    buffer_.Add(key, value);
    num_entries_++;
  }

  void CheckFirst(Iterator* iter) {
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    Slice value = iter->value();
    ASSERT_TRUE(value == kv_.begin()->second);
    Slice key = iter->key();
    ASSERT_TRUE(key == kv_.begin()->first);
  }

  void CheckLast(Iterator* iter) {
    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    Slice value = iter->value();
    ASSERT_TRUE(value == kv_.rbegin()->second);
    Slice key = iter->key();
    ASSERT_TRUE(key == kv_.rbegin()->first);
  }

  std::map<std::string, std::string> kv_;

  uint32_t num_entries_;
  WriteBuffer buffer_;
  Random rnd_;
};

TEST(WriterBufTest, FixedSizedValue) {
  Add(3);
  Add(2);
  Add(1);
  Add(5);
  Add(4);

  Iterator* iter = Flush();
  CheckFirst(iter);
  CheckLast(iter);
  delete iter;
}

TEST(WriterBufTest, VariableSizedValue) {
  Add(3, 16);
  Add(2, 18);
  Add(1, 20);
  Add(5, 14);
  Add(4, 18);

  Iterator* iter = Flush();
  CheckFirst(iter);
  CheckLast(iter);
  delete iter;
}

class WriterTest {
 public:
  explicit WriterTest() {
    dirname_ = test::TmpDir() + "/plfsio_test";
    DestroyDir(dirname_, Options());
    options_.compaction_pool = ThreadPool::NewFixed(1);
    options_.env = Env::Default();
    Status status = Writer::Open(options_, dirname_, &writer_);
    ASSERT_OK(status);
  }

  ~WriterTest() {
    delete writer_;
    delete options_.compaction_pool;
  }

  Options options_;
  std::string dirname_;
  Writer* writer_;
};

TEST(WriterTest, Empty) {
  ASSERT_OK(writer_->MakeEpoch());
  ASSERT_OK(writer_->Finish());
}

}  // namespace plfsio
}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
