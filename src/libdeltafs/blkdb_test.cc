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

  int Open(int id, bool ocreat = false) {
    Fentry ent;
    ent.pid = DirId(0, 0, 0);
    DirIndex::PutHash(&ent.nhash, Name(id));
    ent.zserver = 0;
    ent.stat.SetRegId(0);
    ent.stat.SetSnapId(0);
    ent.stat.SetInodeNo(id);
    int fd;
    Status s = blk_->Open(ent, ocreat, false, &fd);
    if (s.ok()) {
      return fd;
    } else {
      return -1;
    }
  }

  int Creat(int id) { return Open(id, true); }

  void Write(int fd, const Slice& buf, uint64_t off) {
    ASSERT_OK(blk_->Pwrite(fd, buf, off));
  }

  std::string Read(int fd, uint64_t off, uint64_t size) {
    char* tmp = new char[size];
    Slice result;
    ASSERT_OK(blk_->Pread(fd, &result, off, size, tmp));
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

  void Close(int fd) {
    ASSERT_OK(blk_->Sync(fd));
    blk_->Close(fd);
  }
};

TEST(BlkDBTest, Empty) {
  // empty
}

TEST(BlkDBTest, CreateDeleteStreams) {
  int fd = Open(1);
  ASSERT_TRUE(fd == -1);
  fd = Creat(1);
  ASSERT_TRUE(fd != -1);
  Close(fd);
  fd = Open(1);
  ASSERT_TRUE(fd != -1);
  Close(fd);
  fd = Open(2);
  ASSERT_TRUE(fd == -1);
}

TEST(BlkDBTest, SequentialIO) {
  int fd = Creat(1);
  Write(fd, "1234", 0);
  Write(fd, "56", 4);
  Write(fd, "78", 6);
  Write(fd, "9", 8);
  Close(fd);

  fd = Open(1);
  ASSERT_TRUE(fd != -1);
  // Reads all
  ASSERT_EQ(Read(fd, 0, 10), "123456789");
  // Aligned reads
  ASSERT_EQ(Read(fd, 0, 4), "1234");
  ASSERT_EQ(Read(fd, 4, 2), "56");
  ASSERT_EQ(Read(fd, 6, 2), "78");
  ASSERT_EQ(Read(fd, 8, 1), "9");
  ASSERT_EQ(Read(fd, 9, 1), "");
  // Un-aligned reads
  ASSERT_EQ(Read(fd, 0, 3), "123");
  ASSERT_EQ(Read(fd, 3, 2), "45");
  ASSERT_EQ(Read(fd, 5, 5), "6789");
  Close(fd);
}

TEST(BlkDBTest, RandomIO) {
  int fd = Creat(1);
  Write(fd, "9", 8);
  Write(fd, "56", 4);
  Write(fd, "1234", 0);
  Write(fd, "78", 6);
  Close(fd);

  fd = Open(1);
  ASSERT_TRUE(fd != -1);
  // Reads all
  ASSERT_EQ(Read(fd, 0, 10), "123456789");
  // Aligned reads
  ASSERT_EQ(Read(fd, 8, 1), "9");
  ASSERT_EQ(Read(fd, 4, 2), "56");
  ASSERT_EQ(Read(fd, 6, 2), "78");
  ASSERT_EQ(Read(fd, 9, 1), "");
  ASSERT_EQ(Read(fd, 0, 4), "1234");
  // Un-aligned reads
  ASSERT_EQ(Read(fd, 3, 2), "45");
  ASSERT_EQ(Read(fd, 0, 3), "123");
  ASSERT_EQ(Read(fd, 5, 5), "6789");
  Close(fd);
}

TEST(BlkDBTest, FileWithHoles) {
  int fd = Creat(1);
  Write(fd, "12", 0);
  Write(fd, "5", 4);
  Write(fd, "7", 6);
  Write(fd, "9", 8);
  Close(fd);

  fd = Open(1);
  ASSERT_TRUE(fd != -1);
  // Reads all
  ASSERT_EQ(Read(fd, 0, 10), "12xx5x7x9");
  // Aligned reads
  ASSERT_EQ(Read(fd, 0, 4), "12xx");
  ASSERT_EQ(Read(fd, 4, 2), "5x");
  ASSERT_EQ(Read(fd, 6, 2), "7x");
  ASSERT_EQ(Read(fd, 8, 1), "9");
  ASSERT_EQ(Read(fd, 9, 1), "");
  // Un-aligned reads
  ASSERT_EQ(Read(fd, 0, 3), "12x");
  ASSERT_EQ(Read(fd, 3, 2), "x5");
  ASSERT_EQ(Read(fd, 5, 5), "x7x9");
  Close(fd);
}

}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
