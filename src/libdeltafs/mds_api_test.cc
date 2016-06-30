/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "mds_api.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {

class MDSImpl : public MDS {
 public:
  FstatOptions fstat_options_;
  FstatRet fstat_ret_;
  virtual Status Fstat(const FstatOptions& options, FstatRet* ret) {
    ASSERT_EQ(options.dir_ino, fstat_options_.dir_ino);
    ASSERT_EQ(options.name, fstat_options_.name);
    *ret = fstat_ret_;
    return Status::OK();
  }

  FcreatOptions fcreat_options_;
  FcreatRet fcreat_ret_;
  virtual Status Fcreat(const FcreatOptions& options, FcreatRet* ret) {
    ASSERT_EQ(options.dir_ino, fcreat_options_.dir_ino);
    ASSERT_EQ(options.name, fcreat_options_.name);
    ASSERT_EQ(options.mode, fcreat_options_.mode);
    ASSERT_EQ(options.uid, fcreat_options_.uid);
    ASSERT_EQ(options.gid, fcreat_options_.gid);
    *ret = fcreat_ret_;
    return Status::OK();
  }

  virtual Status Mkdir(const MkdirOptions& options, MkdirRet* ret) {
    return Status::OK();
  }

  virtual Status Chmod(const ChmodOptions& options, ChmodRet* ret) {
    return Status::OK();
  }

  virtual Status Lookup(const LookupOptions& options, LookupRet* ret) {
    return Status::OK();
  }

  virtual Status Listdir(const ListdirOptions& options, ListdirRet* ret) {
    return Status::OK();
  }
};

class MDSTest {
 public:
  MDSImpl impl_;
  rpc::If* rpc_mds_;
  MDS* mds_;

  MDSTest() {
    rpc_mds_ = new MDS::RPC::SRV(&impl_);
    mds_ = new MDS::RPC::CLI(rpc_mds_);
  }

  ~MDSTest() {
    delete mds_;
    delete rpc_mds_;
  }
};

static void TestFstat(MDS::FstatOptions& options, MDS::FstatRet& ret,
                      MDS* mds) {
  options.dir_ino = 301;
  options.name = "X";
  ret.stat.SetZerothServer(23);
  ret.stat.SetChangeTime(123);
  ret.stat.SetModifyTime(321);
  ret.stat.SetFileMode(789);
  ret.stat.SetFileSize(20);
  ret.stat.SetGroupId(4);
  ret.stat.SetUserId(11);
  ret.stat.SetInodeNo(301);
  mds->Fstat(options, &ret);
  ASSERT_EQ(ret.stat.ZerothServer(), 23);
  ASSERT_EQ(ret.stat.ChangeTime(), 123);
  ASSERT_EQ(ret.stat.ModifyTime(), 321);
  ASSERT_EQ(ret.stat.FileMode(), 789);
  ASSERT_EQ(ret.stat.FileSize(), 20);
  ASSERT_EQ(ret.stat.GroupId(), 4);
  ASSERT_EQ(ret.stat.UserId(), 11);
  ASSERT_EQ(ret.stat.InodeNo(), 301);
}

static void TestFcreat(MDS::FcreatOptions& options, MDS::FcreatRet& ret,
                       MDS* mds) {
  options.dir_ino = 301;
  options.name = "X";
  options.mode = 789;
  options.gid = 4;
  options.uid = 11;
  ret.stat.SetZerothServer(23);
  ret.stat.SetChangeTime(123);
  ret.stat.SetModifyTime(321);
  ret.stat.SetFileMode(789);
  ret.stat.SetFileSize(20);
  ret.stat.SetGroupId(4);
  ret.stat.SetUserId(11);
  ret.stat.SetInodeNo(301);
  mds->Fcreat(options, &ret);
  ASSERT_EQ(ret.stat.ZerothServer(), 23);
  ASSERT_EQ(ret.stat.ChangeTime(), 123);
  ASSERT_EQ(ret.stat.ModifyTime(), 321);
  ASSERT_EQ(ret.stat.FileMode(), 789);
  ASSERT_EQ(ret.stat.FileSize(), 20);
  ASSERT_EQ(ret.stat.GroupId(), 4);
  ASSERT_EQ(ret.stat.UserId(), 11);
  ASSERT_EQ(ret.stat.InodeNo(), 301);
}

TEST(MDSTest, EncodeDecode) {
  TestFstat(impl_.fstat_options_, impl_.fstat_ret_, mds_);
  TestFcreat(impl_.fcreat_options_, impl_.fcreat_ret_, mds_);
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
