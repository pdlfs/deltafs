/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/gigaplus.h"
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/testharness.h"

#include "rados_conn.h"

namespace pdlfs {
namespace rados {

// Make sure we only connect to rados once during the entire run.
static port::OnceType once = PDLFS_ONCE_INIT;
static RadosConn* rados_conn = NULL;
static void OpenRadosConn() {
  rados_conn = new RadosConn;
  Status s = rados_conn->Open(RadosOptions());
  ASSERT_OK(s);
}

class RadosFioTest {
 public:
  struct File {
    std::string fentry_encoding;
    Fio::Handle* fh;
  };

  RadosFioTest() {
    Status s;
    port::InitOnce(&once, OpenRadosConn);
    s = rados_conn->OpenFio(&fio_);
    ASSERT_OK(s);
  }

  ~RadosFioTest() { delete fio_; }

  static std::string Name(int id) {
    char tmp[20];
    snprintf(tmp, sizeof(tmp), "s%d", id);
    return tmp;
  }

  File* Open(int id, bool ocreat = false, bool otrunc = false) {
    Fentry ent;
    ent.pid = DirId(0, 0, 0);
    DirIndex::PutHash(&ent.nhash, Name(id));
    ent.zserver = 0;
    ent.stat.SetRegId(0);
    ent.stat.SetSnapId(0);
    ent.stat.SetInodeNo(id);
    uint64_t ignored_mtime;
    uint64_t ignored_size;
    char tmp[DELTAFS_FENTRY_BUFSIZE];
    Slice encoding = ent.EncodeTo(tmp);
    Fio::Handle* fh;
    Status s = fio_->Open(encoding, ocreat, otrunc, &ignored_mtime,
                          &ignored_size, &fh);
    if (!s.ok()) {
      return NULL;
    } else {
      File* f = new File;
      f->fentry_encoding = encoding.ToString();
      f->fh = fh;
      return f;
    }
  }

  File* Creat(int id) { return Open(id, true, true); }

  void Write(File* f, const Slice& buf, uint64_t off) {
    ASSERT_OK(fio_->Pwrite(f->fentry_encoding, f->fh, buf, off));
  }

  std::string Read(File* f, uint64_t off, uint64_t size) {
    char* tmp = new char[size];
    Slice result;
    ASSERT_OK(fio_->Pread(f->fentry_encoding, f->fh, &result, off, size, tmp));
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

  void Close(File* f) {
    ASSERT_OK(fio_->Flush(f->fentry_encoding, f->fh));
    fio_->Close(f->fentry_encoding, f->fh);
    delete f;
  }

  Fio* fio_;
};

TEST(RadosFioTest, Empty) {
  // empty
}

TEST(RadosFioTest, OpenCreateAndClose) {
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

TEST(RadosFioTest, SequentialIO) {
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

TEST(RadosFioTest, RandomIO) {
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

TEST(RadosFioTest, FileWithHoles) {
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

}  // namespace rados
}  // namespace pdlfs

int main(int argc, char** argv) {
  setenv("TESTS", "X", 0);  // XXX: don't run any tests by default
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
