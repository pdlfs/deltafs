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
#include "pdlfs-common/murmur.h"
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

class PlfsIoTest {
 public:
  PlfsIoTest() {
    dirname_ = test::TmpDir() + "/plfsio_test";
    options_.verify_checksums = true;
    options_.env = TestEnv();
    writer_ = NULL;
    reader_ = NULL;
  }

  ~PlfsIoTest() {
    if (writer_ != NULL) {
      delete writer_;
    }
    if (reader_ != NULL) {
      delete reader_;
    }
  }

  void OpenWriter() {
    DestroyDir(dirname_, options_);
    Status s = Writer::Open(options_, dirname_, &writer_);
    ASSERT_OK(s);
  }

  void Finish() {
    ASSERT_OK(writer_->Finish());
    Stats();
    delete writer_;
    writer_ = NULL;
  }

  void OpenReader() {
    Status s = Reader::Open(options_, dirname_, &reader_);
    ASSERT_OK(s);
  }

  void MakeEpoch() {
    if (writer_ == NULL) OpenWriter();
    ASSERT_OK(writer_->MakeEpoch());
  }

  void Write(const Slice& key, const Slice& value) {
    if (writer_ == NULL) OpenWriter();
    ASSERT_OK(writer_->Append(key, value));
  }

  std::string Read(const Slice& key) {
    std::string tmp;
    if (writer_ != NULL) Finish();
    if (reader_ == NULL) OpenReader();
    ASSERT_OK(reader_->ReadAll(key, &tmp));
    return tmp;
  }

  void Stats() {
    char tmp[200];
    const CompactionStats* stats = NULL;
    if (writer_ != NULL) {
      stats = writer_->stats();
    }
    if (stats != NULL) {
      snprintf(tmp, sizeof(tmp),
               "isz=%8llu B, idu=%8llu B, "
               "dsz=%8llu B, ddu=%8llu B, "
               "micros=%llu us",
               static_cast<unsigned long long>(stats->index_size),
               static_cast<unsigned long long>(stats->index_written),
               static_cast<unsigned long long>(stats->data_size),
               static_cast<unsigned long long>(stats->data_written),
               static_cast<unsigned long long>(stats->write_micros));
      fprintf(stderr, "%s\n", tmp);
    }
  }

  DirOptions options_;
  std::string dirname_;
  Writer* writer_;
  Reader* reader_;
};

TEST(PlfsIoTest, Empty0) {
  MakeEpoch();
  std::string val = Read("non-exists");
  ASSERT_TRUE(val.empty());
}

TEST(PlfsIoTest, SingleEpoch0) {
  Write("k1", "v1");
  Write("k2", "v2");
  Write("k3", "v3");
  Write("k4", "v4");
  Write("k5", "v5");
  Write("k6", "v6");
  MakeEpoch();
  ASSERT_EQ(Read("k1"), "v1");
  ASSERT_TRUE(Read("k1.1").empty());
  ASSERT_EQ(Read("k2"), "v2");
  ASSERT_TRUE(Read("k2.1").empty());
  ASSERT_EQ(Read("k3"), "v3");
  ASSERT_TRUE(Read("k3.1").empty());
  ASSERT_EQ(Read("k4"), "v4");
  ASSERT_TRUE(Read("k4.1").empty());
  ASSERT_EQ(Read("k5"), "v5");
  ASSERT_TRUE(Read("k5.1").empty());
  ASSERT_EQ(Read("k6"), "v6");
}

TEST(PlfsIoTest, MultiEpoch0) {
  Write("k1", "v1");
  Write("k2", "v2");
  MakeEpoch();
  Write("k1", "v3");
  Write("k2", "v4");
  MakeEpoch();
  Write("k1", "v5");
  Write("k2", "v6");
  MakeEpoch();
  ASSERT_EQ(Read("k1"), "v1v3v5");
  ASSERT_TRUE(Read("k1.1").empty());
  ASSERT_EQ(Read("k2"), "v2v4v6");
}

TEST(PlfsIoTest, NoFilter0) {
  options_.bf_bits_per_key = 0;
  Write("k1", "v1");
  Write("k2", "v2");
  MakeEpoch();
  Write("k3", "v3");
  Write("k4", "v4");
  MakeEpoch();
  Write("k5", "v5");
  Write("k6", "v6");
  MakeEpoch();
  ASSERT_EQ(Read("k1"), "v1");
  ASSERT_TRUE(Read("k1.1").empty());
  ASSERT_EQ(Read("k2"), "v2");
  ASSERT_TRUE(Read("k2.1").empty());
  ASSERT_EQ(Read("k3"), "v3");
  ASSERT_TRUE(Read("k3.1").empty());
  ASSERT_EQ(Read("k4"), "v4");
  ASSERT_TRUE(Read("k4.1").empty());
  ASSERT_EQ(Read("k5"), "v5");
  ASSERT_TRUE(Read("k5.1").empty());
  ASSERT_EQ(Read("k6"), "v6");
}

TEST(PlfsIoTest, NoUniKeys0) {
  options_.unique_keys = false;
  Write("k1", "v1");
  Write("k1", "v2");
  MakeEpoch();
  Write("k0", "v3");
  Write("k1", "v4");
  Write("k1", "v5");
  MakeEpoch();
  Write("k1", "v6");
  Write("k1", "v7");
  Write("k5", "v8");
  MakeEpoch();
  Write("k1", "v9");
  MakeEpoch();
  ASSERT_EQ(Read("k1"), "v1v2v4v5v6v7v9");
}

static void BM_LogAndApply(size_t num_entries) {
  Writer* writer;
  DirOptions options;
  options.verify_checksums = false;
  options.env = TestEnv();
  options.key_size = 10;
  options.value_size = 40;
  options.bf_bits_per_key = 10;
  std::string dirhome = test::TmpDir() + "/plfsio_test_benchmark";
  DestroyDir(dirhome, options);
  ASSERT_OK(Writer::Open(options, dirhome, &writer));

  uint64_t start = Env::Default()->NowMicros();

  char key[16];
  std::string dummy_value(options.value_size, 'x');
  for (size_t i = 0; i < num_entries; i++) {
    murmur_x64_128(&i, sizeof(i), 0, key);
    ASSERT_OK(writer->Append(Slice(key, options.key_size), dummy_value));
  }
  ASSERT_OK(writer->Finish());

  uint64_t end = Env::Default()->NowMicros();

  fprintf(stderr, "== %9llu keys, %5.1f s, %5.2f us/key, %8.3f MB/s (value)\n",
          static_cast<unsigned long long>(num_entries),
          double(end - start) / double(1000000),
          double(end - start) / double(num_entries),
          double(num_entries * options.value_size) / double(end - start));

  char tmp[200];
  const CompactionStats* stats = NULL;
  if (writer != NULL) {
    stats = writer->stats();
  }
  if (stats != NULL) {
    snprintf(tmp, sizeof(tmp),
             "isz=%12llu B, idu=%12llu B, "
             "dsz=%12llu B, ddu=%12llu B, "
             "micros=%llu us",
             static_cast<unsigned long long>(stats->index_size),
             static_cast<unsigned long long>(stats->index_written),
             static_cast<unsigned long long>(stats->data_size),
             static_cast<unsigned long long>(stats->data_written),
             static_cast<unsigned long long>(stats->write_micros));
    fprintf(stderr, "%s\n", tmp);
  }

  delete writer;
}

}  // namespace plfsio
}  // namespace pdlfs

int main(int argc, char* argv[]) {
  if (argc > 1 && std::string(argv[1]) == "--benchmark") {
    ::pdlfs::plfsio::BM_LogAndApply(2 << 20);
    ::pdlfs::plfsio::BM_LogAndApply(4 << 20);
    ::pdlfs::plfsio::BM_LogAndApply(8 << 20);
    ::pdlfs::plfsio::BM_LogAndApply(16 << 20);
    return 0;
  }

  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
