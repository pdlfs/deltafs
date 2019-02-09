/*
 * Copyright (c) 2018-2019 Carnegie Mellon University and
 *         Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_filterio.h"

#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {
namespace plfsio {

class FilterIoTest {
 public:
  FilterIoTest() {
    dirname_ = test::TmpDir() + "/plfsfilterio_test";
    fname_ = dirname_ + "/test.ftl";
    env_ = Env::Default();
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
  }

  void OpenWriter() {
    env_->CreateDir(dirname_.c_str());
    ASSERT_OK(env_->NewWritableFile(fname_.c_str(), &dst_));
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
    ASSERT_OK(env_->NewRandomAccessFile(fname_.c_str(), &src_));
    ASSERT_OK(env_->GetFileSize(fname_.c_str(), &src_sz_));
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

  std::string dirname_, fname_;
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
