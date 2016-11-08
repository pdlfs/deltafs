/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include "mds_srv.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {

class ServerTest {
 private:
  std::string dbname_;
  MDSEnv mds_env_;
  MDS* mds_;
  MDB* mdb_;
  DB* db_;

 public:
  ServerTest() {
    Env* env = Env::Default();
    dbname_ = test::NewTmpDirectory("mds_srv_test", env);
    DBOptions dbopts;
    dbopts.env = env;
    DestroyDB(dbname_, dbopts);
    dbopts.create_if_missing = true;
    ASSERT_OK(DB::Open(dbopts, dbname_, &db_));
    MDBOptions mdbopts;
    mdbopts.db = db_;
    mdb_ = new MDB(mdbopts);
    mds_env_.env = env;
    MDSOptions mdsopts;
    mdsopts.mds_env = &mds_env_;
    mdsopts.mdb = mdb_;
    mds_ = MDS::Open(mdsopts);
  }

  ~ServerTest() {
    delete mds_;
    delete mdb_;
    delete db_;
  }

  static std::string NodeName(int i) {
    char tmp[50];
    snprintf(tmp, sizeof(tmp), "node%d", i);
    return tmp;
  }

  // Return the ino of the node being searched, or "-err_code" on errors.
  int Fstat(int dir_ino, int nod_no) {
    MDS::FstatOptions options;
    options.dir_id = DirId(0, 0, dir_ino);
    std::string name = NodeName(nod_no);
    options.name = name;
    std::string name_hash;
    DirIndex::PutHash(&name_hash, name);
    options.name_hash = name_hash;
    MDS::FstatRet ret;
    Status s = mds_->Fstat(options, &ret);
    if (s.ok()) {
      return static_cast<int>(ret.stat.InodeNo());
    } else {
      return -1 * s.err_code();
    }
  }

  // Return the ino of the newly created file, or "-err_code" on errors.
  int Mknod(int dir_ino, int nod_no) {
    MDS::FcreatOptions options;
    options.dir_id = DirId(0, 0, dir_ino);
    options.flags = O_EXCL;
    options.mode = ACCESSPERMS;
    options.uid = 0;
    options.gid = 0;
    std::string name = NodeName(nod_no);
    options.name = name;
    std::string name_hash;
    DirIndex::PutHash(&name_hash, name);
    options.name_hash = name_hash;
    MDS::FcreatRet ret;
    Status s = mds_->Fcreat(options, &ret);
    if (s.ok()) {
      return static_cast<int>(ret.stat.InodeNo());
    } else {
      return -1 * s.err_code();
    }
  }

  int Mkdir(int dir_ino, int nod_no) {
    MDS::MkdirOptions options;
    options.dir_id = DirId(0, 0, dir_ino);
    options.flags = O_EXCL;
    options.mode = ACCESSPERMS;
    options.uid = 0;
    options.gid = 0;
    std::string name = NodeName(nod_no);
    options.name = name;
    std::string name_hash;
    DirIndex::PutHash(&name_hash, name);
    options.name_hash = name_hash;
    MDS::MkdirRet ret;
    Status s = mds_->Mkdir(options, &ret);
    if (s.ok()) {
      return static_cast<int>(ret.stat.InodeNo());
    } else {
      return -1 * s.err_code();
    }
  }

  int Listdir(int dir_ino) {
    MDS::ListdirOptions options;
    options.dir_id = DirId(0, 0, dir_ino);
    std::vector<std::string> names;
    MDS::ListdirRet ret;
    ret.names = &names;
    Status s = mds_->Listdir(options, &ret);
    if (s.ok()) {
      return names.size();
    } else {
      return -1 * s.err_code();
    }
  }
};

TEST(ServerTest, StartStop) {
  // empty
}

TEST(ServerTest, Files) {
  int r1 = Fstat(0, 1);
  ASSERT_TRUE(r1 == -1 * Status::kNotFound);
  int r2 = Mknod(0, 1);
  ASSERT_TRUE(r2 > 0);
  int r3 = Fstat(0, 1);
  ASSERT_TRUE(r3 == r2);
  int r4 = Mknod(0, 1);
  ASSERT_TRUE(r4 == -1 * Status::kAlreadyExists);
}

TEST(ServerTest, Dirs) {
  int r1 = Fstat(0, 1);
  ASSERT_TRUE(r1 == -1 * Status::kNotFound);
  int r2 = Mkdir(0, 1);
  ASSERT_TRUE(r2 > 0);
  int r3 = Fstat(0, 1);
  ASSERT_TRUE(r3 == r2);
  int r4 = Mkdir(0, 1);
  ASSERT_TRUE(r4 == -1 * Status::kAlreadyExists);
}

TEST(ServerTest, Scan) {
  Mknod(0, 1);
  Mknod(0, 2);
  Mknod(0, 3);
  Mknod(0, 4);
  Mknod(0, 5);
  Mkdir(0, 6);
  Mkdir(0, 7);
  Mkdir(0, 8);
  Mkdir(0, 9);
  int r = Listdir(0);
  ASSERT_TRUE(r == 9);
}

}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
