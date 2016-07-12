/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdio.h>

#include "blkdb.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {

class BlkDBTest {
 private:
  Env* env_;
  std::string dbname_;
  BlkDB* blk_;
  DB* db_;

 public:
  BlkDBTest() {
    env_ = Env::Default();
    dbname_ = test::NewTmpDirectory("blkdb_test", env_);
    DBOptions dbopts;
    dbopts.env = env_;
    DestroyDB(dbname_, dbopts);
    dbopts.create_if_missing = true;
    ASSERT_OK(DB::Open(dbopts, dbname_, &db_));
    BlkDBOptions blkopts;
    blkopts.db = db_;
    blk_ = new BlkDB(blkopts);
  }

  ~BlkDBTest() {
    delete blk_;
    delete db_;
  }

  static std::string Name(int id) {
    char tmp[20];
    snprintf(tmp, sizeof(tmp), "s%d", id);
    return tmp;
  }

  Stream* Open(int id, bool ocreat = false) {
    DirId pid(0, 0, 0);
    std::string nhash;
    DirIndex::PutHash(&nhash, Name(id));
    Stat stat;
    stat.SetRegId(0);
    stat.SetSnapId(0);
    stat.SetInodeNo(id);
    Stream* stream;
    Status s = blk_->Open(pid, nhash, stat, ocreat, false, &stream);
    if (s.ok()) {
      return stream;
    } else {
      return NULL;
    }
  }

  Stream* Creat(int id) { return Open(id, true); }

  void Write(Stream* stream, const Slice& buf, uint64_t off) {
    ASSERT_OK(blk_->Pwrite(stream, buf, off));
  }

  std::string Read(Stream* stream, uint64_t off, uint64_t size) {
    char* tmp = new char[size];
    Slice result;
    ASSERT_OK(blk_->Pread(stream, &result, off, size, tmp));
    std::string data = result.ToString();
    delete tmp;
    std::string::iterator it = data.begin();
    for (; it != data.end(); ++it) {
      if (*it == 0) {
        *it = 'x';
      }
    }
    return data;
  }

  void Close(Stream* stream) {
    ASSERT_OK(blk_->Sync(stream));
    blk_->Close(stream);
  }
};

TEST(BlkDBTest, Empty) {
  // empty
}

TEST(BlkDBTest, CreateDeleteStreams) {
  Stream* stream = Open(1);
  ASSERT_TRUE(stream == NULL);
  stream = Creat(1);
  ASSERT_TRUE(stream != NULL);
  Close(stream);
  stream = Open(1);
  ASSERT_TRUE(stream != NULL);
  Close(stream);
  stream = Open(2);
  ASSERT_TRUE(stream == NULL);
}

TEST(BlkDBTest, SequentialIO) {
  Stream* stream = Creat(1);
  Write(stream, "1234", 0);
  Write(stream, "56", 4);
  Write(stream, "78", 6);
  Write(stream, "9", 8);
  Close(stream);

  stream = Open(1);
  ASSERT_TRUE(stream != NULL);
  // Reads all
  ASSERT_EQ(Read(stream, 0, 10), "123456789");
  // Aligned reads
  ASSERT_EQ(Read(stream, 0, 4), "1234");
  ASSERT_EQ(Read(stream, 4, 2), "56");
  ASSERT_EQ(Read(stream, 6, 2), "78");
  ASSERT_EQ(Read(stream, 8, 1), "9");
  ASSERT_EQ(Read(stream, 9, 1), "");
  // Un-aligned reads
  ASSERT_EQ(Read(stream, 0, 3), "123");
  ASSERT_EQ(Read(stream, 3, 2), "45");
  ASSERT_EQ(Read(stream, 5, 5), "6789");
  Close(stream);
}

TEST(BlkDBTest, RandomIO) {
  Stream* stream = Creat(1);
  Write(stream, "9", 8);
  Write(stream, "56", 4);
  Write(stream, "1234", 0);
  Write(stream, "78", 6);
  Close(stream);

  stream = Open(1);
  ASSERT_TRUE(stream != NULL);
  // Reads all
  ASSERT_EQ(Read(stream, 0, 10), "123456789");
  // Aligned reads
  ASSERT_EQ(Read(stream, 8, 1), "9");
  ASSERT_EQ(Read(stream, 4, 2), "56");
  ASSERT_EQ(Read(stream, 6, 2), "78");
  ASSERT_EQ(Read(stream, 9, 1), "");
  ASSERT_EQ(Read(stream, 0, 4), "1234");
  // Un-aligned reads
  ASSERT_EQ(Read(stream, 3, 2), "45");
  ASSERT_EQ(Read(stream, 0, 3), "123");
  ASSERT_EQ(Read(stream, 5, 5), "6789");
  Close(stream);
}

TEST(BlkDBTest, FileWithHoles) {
  Stream* stream = Creat(1);
  Write(stream, "12", 0);
  Write(stream, "5", 4);
  Write(stream, "7", 6);
  Write(stream, "9", 8);
  Close(stream);

  stream = Open(1);
  ASSERT_TRUE(stream != NULL);
  // Reads all
  ASSERT_EQ(Read(stream, 0, 10), "12xx5x7x9");
  // Aligned reads
  ASSERT_EQ(Read(stream, 0, 4), "12xx");
  ASSERT_EQ(Read(stream, 4, 2), "5x");
  ASSERT_EQ(Read(stream, 6, 2), "7x");
  ASSERT_EQ(Read(stream, 8, 1), "9");
  ASSERT_EQ(Read(stream, 9, 1), "");
  // Un-aligned reads
  ASSERT_EQ(Read(stream, 0, 3), "123");
  ASSERT_EQ(Read(stream, 3, 2), "45");
  ASSERT_EQ(Read(stream, 5, 5), "6789");
  Close(stream);
}

}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
