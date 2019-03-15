/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "table_stats.h"

#include "pdlfs-common/leveldb/comparator.h"
#include "pdlfs-common/leveldb/db/dbformat.h"
#include "pdlfs-common/leveldb/db/options.h"
#include "pdlfs-common/leveldb/table.h"
#include "pdlfs-common/leveldb/table_builder.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

#include <map>
#include <string>

namespace pdlfs {

// An STL comparator that uses a Comparator
namespace {
struct STLLessThan {
  const Comparator* cmp;

  STLLessThan() : cmp(BytewiseComparator()) {}
  STLLessThan(const Comparator* c) : cmp(c) {}
  bool operator()(const std::string& a, const std::string& b) const {
    return cmp->Compare(Slice(a), Slice(b)) < 0;
  }
};
}  // namespace

class StringSink : public WritableFile {
 public:
  virtual ~StringSink() {}

  const std::string& contents() const { return contents_; }

  virtual Status Close() { return Status::OK(); }
  virtual Status Flush() { return Status::OK(); }
  virtual Status Sync() { return Status::OK(); }

  virtual Status Append(const Slice& data) {
    contents_.append(data.data(), data.size());
    return Status::OK();
  }

 private:
  std::string contents_;
};

class StringSource : public RandomAccessFile {
 public:
  StringSource(const Slice& contents)
      : contents_(contents.data(), contents.size()) {}

  virtual ~StringSource() {}

  uint64_t Size() const { return contents_.size(); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    if (offset > contents_.size()) {
      return Status::InvalidArgument(Slice());
    }
    if (offset + n > contents_.size()) {
      n = contents_.size() - offset;
    }
    memcpy(scratch, &contents_[offset], n);
    *result = Slice(scratch, n);
    return Status::OK();
  }

 private:
  std::string contents_;
};

typedef std::map<std::string, std::string, STLLessThan> KVMap;

class TableWriter {
  typedef DBOptions Options;

 private:
  StringSink file_;
  TableBuilder builder_;
  KVMap data_;

 public:
  TableWriter(const Options& options) : builder_(options, &file_) {}

  std::string contents() const { return file_.contents(); }

  void Put(const std::string& key, const std::string& value) {
    data_[key] = value;
  }

  Slice SmallestKey() const {
    if (data_.empty()) {
      return Slice();
    } else {
      return data_.begin()->first;
    }
  }

  Slice LargestKey() const {
    if (data_.empty()) {
      return Slice();
    } else {
      return data_.rbegin()->first;
    }
  }

  void Finish() {
    for (KVMap::iterator it = data_.begin(); it != data_.end(); ++it) {
      builder_.Add(it->first, it->second);
    }
    Status s = builder_.Finish();
    ASSERT_TRUE(s.ok());
  }
};

class TableReader {
  typedef DBOptions Options;

 private:
  StringSource file_;
  Table* table_;

 public:
  TableReader(const Options& options, const std::string& contents)
      : file_(contents) {
    Status s;
    s = Table::Open(options, &file_, file_.Size(), &table_);
    ASSERT_TRUE(s.ok());
  }

  ~TableReader() { delete table_; }

  Slice SmallestKey() {
    ASSERT_TRUE(TableStats::HasStats(table_));
    return TableStats::FirstKey(table_);
  }

  Slice LargestKey() {
    ASSERT_TRUE(TableStats::HasStats(table_));
    return TableStats::LastKey(table_);
  }

  uint64_t MinSeq() {
    ASSERT_TRUE(TableStats::HasStats(table_));
    return TableStats::MinSeq(table_);
  }

  uint64_t MaxSeq() {
    ASSERT_TRUE(TableStats::HasStats(table_));
    return TableStats::MaxSeq(table_);
  }
};

static const uint64_t kMinSequenceNumber = 301;

static const int kNumEntries = 1024;

static const int kTableKeyLength = 16;

class TableTest {
 protected:
  typedef DBOptions Options;
};

static std::string RandomKey(Random* rnd) {
  return test::RandomKey(rnd, kTableKeyLength);
}

static std::string RandomInternalKey(Random* rnd, uint64_t seq) {
  std::string encoded;
  std::string rkey = RandomKey(rnd);
  ParsedInternalKey ikey(rkey, seq, kTypeValue);
  AppendInternalKey(&encoded, ikey);
  return encoded;
}

static std::string CreateTable(TableWriter* writer) {
  Random rnd(301);
  const uint64_t seq = rnd.Next();
  for (int i = 0; i < kNumEntries; i++) {
    writer->Put(
        RandomInternalKey(&rnd, (seq + i) % kNumEntries + kMinSequenceNumber),
        "abcdfeg");
  }
  writer->Finish();
  return writer->contents();
}

TEST(TableTest, ReadWrite) {
  Options options;
  TableWriter writer(options);
  std::string contents = CreateTable(&writer);
  TableReader reader(options, contents);

  ASSERT_EQ(reader.SmallestKey(), writer.SmallestKey());
  ASSERT_EQ(reader.LargestKey(), writer.LargestKey());
  ASSERT_EQ(reader.MinSeq(), kMinSequenceNumber);
  ASSERT_EQ(reader.MaxSeq(), kMinSequenceNumber + kNumEntries - 1);
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
