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

static inline Env* TestEnv() {
  Env* env = port::posix::GetUnBufferedIOEnv();
  return env;
}

template <int lg_parts = 0>
class WriterTest {
 public:
  WriterTest() {
    dirname_ = test::TmpDir() + "/plfsio_test";
    DestroyDir(dirname_, Options());
    options_.env = TestEnv();
    options_.lg_parts = lg_parts;
    Status s = Writer::Open(options_, dirname_, &writer_);
    ASSERT_OK(s);
  }

  ~WriterTest() {
    if (writer_ != NULL) {
      delete writer_;
    }
  }

  Options options_;
  std::string dirname_;
  Writer* writer_;
};

template <int lg_parts = 0>
class ReaderTest {
 public:
  ReaderTest() {
    dirname_ = test::TmpDir() + "/plfsio_test";
    options_.verify_checksums = true;
    options_.env = TestEnv();
    options_.lg_parts = lg_parts;
    Status s = Reader::Open(options_, dirname_, &reader_);
    ASSERT_OK(s);
  }

  ~ReaderTest() {
    if (reader_ != NULL) {
      delete reader_;
    }
  }

  Options options_;
  std::string dirname_;
  Reader* reader_;
};

TEST(WriterTest<0>, Empty0) {
  ASSERT_OK(writer_->MakeEpoch());
  ASSERT_OK(writer_->Finish());
}

TEST(ReaderTest<0>, EmptyRead0) {
  std::string tmp;
  ASSERT_OK(reader_->ReadAll("non-exists", &tmp));
  ASSERT_TRUE(tmp.empty());
}

TEST(WriterTest<0>, SingleEpoch0) {
  ASSERT_OK(writer_->Append("k1", "v1"));
  ASSERT_OK(writer_->Append("k2", "v2"));
  ASSERT_OK(writer_->Append("k3", "v3"));
  ASSERT_OK(writer_->Append("k4", "v4"));
  ASSERT_OK(writer_->Append("k5", "v5"));
  ASSERT_OK(writer_->Append("k6", "v6"));
  ASSERT_OK(writer_->MakeEpoch());
  ASSERT_OK(writer_->Finish());
}

TEST(ReaderTest<0>, SingleEpochRead0) {
  std::string tmp;
  ASSERT_OK(reader_->ReadAll("k1", &tmp));
  ASSERT_EQ(tmp, "v1");
  tmp = "";
  ASSERT_OK(reader_->ReadAll("k2", &tmp));
  ASSERT_EQ(tmp, "v2");
  tmp = "";
  ASSERT_OK(reader_->ReadAll("k3", &tmp));
  ASSERT_EQ(tmp, "v3");
  tmp = "";
  ASSERT_OK(reader_->ReadAll("k4", &tmp));
  ASSERT_EQ(tmp, "v4");
  tmp = "";
  ASSERT_OK(reader_->ReadAll("k5", &tmp));
  ASSERT_EQ(tmp, "v5");
  tmp = "";
  ASSERT_OK(reader_->ReadAll("k6", &tmp));
  ASSERT_EQ(tmp, "v6");
}

TEST(WriterTest<0>, MultiEpoch0) {
  ASSERT_OK(writer_->Append("k1", "v1"));
  ASSERT_OK(writer_->Append("k2", "v2"));
  ASSERT_OK(writer_->MakeEpoch());
  ASSERT_OK(writer_->Append("k1", "v3"));
  ASSERT_OK(writer_->Append("k2", "v4"));
  ASSERT_OK(writer_->MakeEpoch());
  ASSERT_OK(writer_->Append("k1", "v5"));
  ASSERT_OK(writer_->Append("k2", "v6"));
  ASSERT_OK(writer_->MakeEpoch());
  ASSERT_OK(writer_->Finish());
}

TEST(ReaderTest<0>, MultiEpochRead0) {
  std::string tmp;
  ASSERT_OK(reader_->ReadAll("k1", &tmp));
  ASSERT_EQ(tmp, "v1v3v5");
  tmp = "";
  ASSERT_OK(reader_->ReadAll("k2", &tmp));
  ASSERT_EQ(tmp, "v2v4v6");
}

TEST(WriterTest<1>, Empty1) {
  ASSERT_OK(writer_->MakeEpoch());
  ASSERT_OK(writer_->Finish());
}

TEST(ReaderTest<1>, EmptyRead1) {
  std::string tmp;
  ASSERT_OK(reader_->ReadAll("non-exists", &tmp));
  ASSERT_TRUE(tmp.empty());
}

TEST(WriterTest<1>, SingleEpoch1) {
  ASSERT_OK(writer_->Append("k1", "v1"));
  ASSERT_OK(writer_->Append("k2", "v2"));
  ASSERT_OK(writer_->Append("k3", "v3"));
  ASSERT_OK(writer_->Append("k4", "v4"));
  ASSERT_OK(writer_->Append("k5", "v5"));
  ASSERT_OK(writer_->Append("k6", "v6"));
  ASSERT_OK(writer_->MakeEpoch());
  ASSERT_OK(writer_->Finish());
}

TEST(ReaderTest<1>, SingleEpochRead1) {
  std::string tmp;
  ASSERT_OK(reader_->ReadAll("k1", &tmp));
  ASSERT_EQ(tmp, "v1");
  tmp = "";
  ASSERT_OK(reader_->ReadAll("k2", &tmp));
  ASSERT_EQ(tmp, "v2");
  tmp = "";
  ASSERT_OK(reader_->ReadAll("k3", &tmp));
  ASSERT_EQ(tmp, "v3");
  tmp = "";
  ASSERT_OK(reader_->ReadAll("k4", &tmp));
  ASSERT_EQ(tmp, "v4");
  tmp = "";
  ASSERT_OK(reader_->ReadAll("k5", &tmp));
  ASSERT_EQ(tmp, "v5");
  tmp = "";
  ASSERT_OK(reader_->ReadAll("k6", &tmp));
  ASSERT_EQ(tmp, "v6");
}

TEST(WriterTest<1>, MultiEpoch1) {
  ASSERT_OK(writer_->Append("k1", "v1"));
  ASSERT_OK(writer_->Append("k2", "v2"));
  ASSERT_OK(writer_->MakeEpoch());
  ASSERT_OK(writer_->Append("k1", "v3"));
  ASSERT_OK(writer_->Append("k2", "v4"));
  ASSERT_OK(writer_->MakeEpoch());
  ASSERT_OK(writer_->Append("k1", "v5"));
  ASSERT_OK(writer_->Append("k2", "v6"));
  ASSERT_OK(writer_->MakeEpoch());
  ASSERT_OK(writer_->Finish());
}

TEST(ReaderTest<1>, MultiEpochRead1) {
  std::string tmp;
  ASSERT_OK(reader_->ReadAll("k1", &tmp));
  ASSERT_EQ(tmp, "v1v3v5");
  tmp = "";
  ASSERT_OK(reader_->ReadAll("k2", &tmp));
  ASSERT_EQ(tmp, "v2v4v6");
}

}  // namespace plfsio
}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
