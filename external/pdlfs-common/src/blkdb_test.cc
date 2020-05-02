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

#include "pdlfs-common/blkdb.h"
#include "pdlfs-common/gigaplus.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {

class BlkDBTest {
 public:
  struct File {
    Fio::Handle* fh;
    Fentry fentry;
  };

  BlkDBTest() {
    env_ = Env::Default();
    dbname_ = test::PrepareTmpDir("blkdb_test", env_);
    DBOptions dbopts;
    dbopts.env = env_;
    DestroyDB(dbname_, dbopts);
    dbopts.create_if_missing = true;
    ASSERT_OK(DB::Open(dbopts, dbname_, &db_));
    BlkDBOptions blkopts;
    blkopts.owns_db = false;
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

  File* Open(int id, bool ocreat = false) {
    Fentry fentry;
#if defined(DELTAFS)
    fentry.pid = DirId(0, 0, 0);
#else
    fentry.pid = DirId(0);
#endif
    DirIndex::PutHash(&fentry.nhash, Name(id));
    fentry.zserver = 0;
#if defined(DELTAFS)
    fentry.stat.SetRegId(0);
    fentry.stat.SetSnapId(0);
#endif
    fentry.stat.SetInodeNo(id);
    uint64_t ignored_mtime;
    uint64_t ignored_size;
    Fio::Handle* fh;
    Status s = blk_->Open(fentry, ocreat, false, false, &ignored_mtime,
                          &ignored_size, &fh);
    if (!s.ok()) {
      return NULL;
    } else {
      File* f = new File;
      f->fentry = fentry;
      f->fh = fh;
      return f;
    }
  }

  File* Creat(int id) { return Open(id, true); }

  void Write(File* f, const Slice& buf, uint64_t off) {
    ASSERT_OK(blk_->Pwrite(f->fentry, f->fh, buf, off));
  }

  std::string Read(File* f, uint64_t off, uint64_t size) {
    char* tmp = new char[size];
    Slice result;
    ASSERT_OK(blk_->Pread(f->fentry, f->fh, &result, off, size, tmp));
    std::string data = result.ToString();
    delete[] tmp;
    std::string::iterator it = data.begin();
    for (; it != data.end(); ++it) {
      if (*it == 0) {
        *it = 'x';
      }
    }
    return data;
  }

  void Close(File* f) {
    ASSERT_OK(blk_->Flush(f->fentry, f->fh));
    blk_->Close(f->fentry, f->fh);
    delete f;
  }

  Env* env_;
  std::string dbname_;
  BlkDB* blk_;
  DB* db_;
};

TEST(BlkDBTest, Empty) {
  // empty
}

TEST(BlkDBTest, OpenCreateAndClose) {
  File* f = Open(1);
  ASSERT_TRUE(f == NULL);
  f = Creat(1);
  ASSERT_TRUE(f != NULL);
  Close(f);
  f = Open(1);
  ASSERT_TRUE(f != NULL);
  Close(f);
  f = Open(2);
  ASSERT_TRUE(f == NULL);
}

TEST(BlkDBTest, SequentialIO) {
  File* f = Creat(1);
  Write(f, "1234", 0);
  Write(f, "56", 4);
  Write(f, "78", 6);
  Write(f, "9", 8);
  Close(f);

  f = Open(1);
  ASSERT_TRUE(f != NULL);
  // Reads all
  ASSERT_EQ(Read(f, 0, 10), "123456789");
  // Aligned reads
  ASSERT_EQ(Read(f, 0, 4), "1234");
  ASSERT_EQ(Read(f, 4, 2), "56");
  ASSERT_EQ(Read(f, 6, 2), "78");
  ASSERT_EQ(Read(f, 8, 1), "9");
  ASSERT_EQ(Read(f, 9, 1), "");
  // Un-aligned reads
  ASSERT_EQ(Read(f, 0, 3), "123");
  ASSERT_EQ(Read(f, 3, 2), "45");
  ASSERT_EQ(Read(f, 5, 5), "6789");
  Close(f);
}

TEST(BlkDBTest, RandomIO) {
  File* f = Creat(1);
  Write(f, "9", 8);
  Write(f, "56", 4);
  Write(f, "1234", 0);
  Write(f, "78", 6);
  Close(f);

  f = Open(1);
  ASSERT_TRUE(f != NULL);
  // Reads all
  ASSERT_EQ(Read(f, 0, 10), "123456789");
  // Aligned reads
  ASSERT_EQ(Read(f, 8, 1), "9");
  ASSERT_EQ(Read(f, 4, 2), "56");
  ASSERT_EQ(Read(f, 6, 2), "78");
  ASSERT_EQ(Read(f, 9, 1), "");
  ASSERT_EQ(Read(f, 0, 4), "1234");
  // Un-aligned reads
  ASSERT_EQ(Read(f, 3, 2), "45");
  ASSERT_EQ(Read(f, 0, 3), "123");
  ASSERT_EQ(Read(f, 5, 5), "6789");
  Close(f);
}

TEST(BlkDBTest, FileWithHoles) {
  File* f = Creat(1);
  Write(f, "12", 0);
  Write(f, "5", 4);
  Write(f, "7", 6);
  Write(f, "9", 8);
  Close(f);

  f = Open(1);
  ASSERT_TRUE(f != NULL);
  // Reads all
  ASSERT_EQ(Read(f, 0, 10), "12xx5x7x9");
  // Aligned reads
  ASSERT_EQ(Read(f, 0, 4), "12xx");
  ASSERT_EQ(Read(f, 4, 2), "5x");
  ASSERT_EQ(Read(f, 6, 2), "7x");
  ASSERT_EQ(Read(f, 8, 1), "9");
  ASSERT_EQ(Read(f, 9, 1), "");
  // Un-aligned reads
  ASSERT_EQ(Read(f, 0, 3), "12x");
  ASSERT_EQ(Read(f, 3, 2), "x5");
  ASSERT_EQ(Read(f, 5, 5), "x7x9");
  Close(f);
}

}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
