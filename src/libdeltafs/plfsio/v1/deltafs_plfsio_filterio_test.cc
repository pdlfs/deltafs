/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_filterio.h"

#include "pdlfs-common/env.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

#include <map>

#if __cplusplus >= 201103
#define OVERRIDE override
#else
#define OVERRIDE
#endif

namespace pdlfs {
namespace plfsio {

// Test Env ...
namespace {
// A simply file implementation that stores data in memory
class TestWritableFile : public WritableFileWrapper {
 public:
  explicit TestWritableFile(std::string* dst) : dst_(dst) {}

  virtual ~TestWritableFile() {}

  virtual Status Append(const Slice& buf) OVERRIDE {
    if (!buf.empty()) dst_->append(buf.data(), buf.size());
    return Status::OK();
  }

 private:
  std::string* const dst_;
};

class TestRandomAccessFile : public RandomAccessFile {
 public:
  explicit TestRandomAccessFile(std::string* src) : src_(src) {}

  virtual ~TestRandomAccessFile() {}

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    *result = *src_;
    if (offset > result->size()) {
      offset = result->size();
    }
    result->remove_prefix(offset);
    if (result->size() > n) {
      result->remove_suffix(result->size() - n);
    }
    return Status::OK();
  }

 private:
  const std::string* const src_;
};

class TestEnv : public EnvWrapper {
 public:
  TestEnv() : EnvWrapper(Env::Default()) {}

  virtual ~TestEnv() {}

  virtual Status GetFileSize(const char* f, uint64_t* s) OVERRIDE {
    std::map<std::string, std::string>::iterator const it = files_.find(f);
    if (it != files_.end()) {
      *s = it->second.size();
      return Status::OK();
    } else {
      *s = 0;
      return Status::NotFound(Slice());
    }
  }

  virtual Status NewRandomAccessFile(const char* f,
                                     RandomAccessFile** r) OVERRIDE {
    std::map<std::string, std::string>::iterator const it = files_.find(f);
    if (it != files_.end()) {
      *r = new TestRandomAccessFile(&it->second);
      return Status::OK();
    } else {
      return Status::NotFound(Slice());
    }
  }

  virtual Status NewWritableFile(const char* f, WritableFile** r) OVERRIDE {
    std::string& dst = files_[f];
    dst.resize(0);
    *r = new TestWritableFile(&dst);
    return Status::OK();
  }

 private:
  std::map<std::string, std::string> files_;
};

}  // namespace

class FilterIoTest {
 public:
  FilterIoTest() {
    env_ = new TestEnv;
    src_sz_ = 0;
    reader_ = NULL;
    src_ = NULL;
    writer_ = NULL;
    dst_ = NULL;
  }

  ~FilterIoTest() {
    delete reader_;
    delete src_;
    delete writer_;
    delete dst_;
    delete env_;
  }

  void OpenWriter() {
    ASSERT_OK(env_->NewWritableFile("test.ftl", &dst_));
    writer_ = new FilterWriter(dst_);
  }

  void Put(uint32_t epoch, const Slice& contents) {
    if (!writer_) OpenWriter();
    ASSERT_OK(writer_->EpochFlush(epoch, contents));
  }

  void Finish() {
    if (!writer_) OpenWriter();
    writer_->Finish();
    dst_->Close();
  }

  void OpenReader() {
    ASSERT_OK(env_->NewRandomAccessFile("test.ftl", &src_));
    ASSERT_OK(env_->GetFileSize("test.ftl", &src_sz_));
    reader_ = new FilterReader(src_, src_sz_);
  }

  uint32_t NumPuts() {
    if (!reader_) OpenReader();
    return reader_->TEST_NumEpochs();
  }

  std::string Get(uint32_t epoch) {
    if (!reader_) OpenReader();
    std::string scratch;
    Slice result;
    ASSERT_OK(reader_->Read(epoch, &result, &scratch));
    return result.ToString();
  }

  RandomAccessFile* src_;
  uint64_t src_sz_;
  FilterReader* reader_;
  WritableFile* dst_;
  FilterWriter* writer_;
  Env* env_;
};

TEST(FilterIoTest, Empty) {
  Finish();
  ASSERT_EQ(NumPuts(), 0);
  ASSERT_EQ(Get(0), Slice());
}

TEST(FilterIoTest, ReadWrite) {
  Put(1, "111");
  Put(3, "333");
  Finish();
  ASSERT_EQ(NumPuts(), 2);
  ASSERT_EQ(Get(1), "111");
  ASSERT_EQ(Get(3), "333");
  ASSERT_EQ(Get(2), Slice());
}

TEST(FilterIoTest, MoreReadWrite) {
  const uint32_t num_puts = 1000;
  char tmp[4];
  Slice t(tmp, sizeof(tmp));
  uint32_t i = 0;
  for (; i < num_puts; i++) {
    EncodeFixed32(tmp, i);
    Put(i, t);
  }
  Finish();
  ASSERT_EQ(NumPuts(), num_puts);
  i = 0;
  for (; i < num_puts; i++) {
    EncodeFixed32(tmp, i);
    ASSERT_EQ(Get(i), t);
  }
}

}  // namespace plfsio
}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return pdlfs::test::RunAllTests(&argc, &argv);
}
