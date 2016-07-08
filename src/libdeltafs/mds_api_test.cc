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

template <typename T>
class APITest {
 public:
  T target_;
  rpc::If* rpc_;
  MDS* mds_;
#define t_opts_ target_.options_
#define t_ret_ target_.ret_
#define t_status_ target_.status_
#define t_re_ target_.re_

  APITest() {
    rpc_ = new MDS::RPC::SRV(&target_);
    mds_ = new MDS::RPC::CLI(rpc_);
  }

  ~APITest() {
    delete mds_;
    delete rpc_;
  }
};

class FstatWrapper : public MDSWrapper {
 public:
  Redirect re_;
  FstatOptions options_;
  FstatRet ret_;
  Status status_;
  virtual Status Fstat(const FstatOptions& options, FstatRet* ret) {
    ASSERT_TRUE(options.dir_id.compare(options_.dir_id) == 0);
    ASSERT_EQ(options.name_hash, options_.name_hash);
    ASSERT_EQ(options.name, options_.name);
    if (!re_.empty()) {
      throw re_;
    } else {
      *ret = ret_;
      return status_;
    }
  }
};

TEST(APITest<FstatWrapper>, Fstat) {
  t_opts_.dir_id = DirId(31, 13, 301);
  t_opts_.name_hash = "aabbccdd";
  t_opts_.name = "X";
  t_ret_.stat.SetZerothServer(23);
  t_ret_.stat.SetChangeTime(123);
  t_ret_.stat.SetModifyTime(321);
  t_ret_.stat.SetFileMode(789);
  t_ret_.stat.SetFileSize(20);
  t_ret_.stat.SetGroupId(4);
  t_ret_.stat.SetUserId(11);
  t_ret_.stat.SetInodeNo(301);
  t_status_ = Status::NotFound(Slice());
  Status s = mds_->Fstat(t_opts_, &t_ret_);
  ASSERT_EQ(s.err_code(), t_status_.err_code());
  ASSERT_EQ(t_ret_.stat.ZerothServer(), 23);
  ASSERT_EQ(t_ret_.stat.ChangeTime(), 123);
  ASSERT_EQ(t_ret_.stat.ModifyTime(), 321);
  ASSERT_EQ(t_ret_.stat.FileMode(), 789);
  ASSERT_EQ(t_ret_.stat.FileSize(), 20);
  ASSERT_EQ(t_ret_.stat.GroupId(), 4);
  ASSERT_EQ(t_ret_.stat.UserId(), 11);
  ASSERT_EQ(t_ret_.stat.InodeNo(), 301);
}

TEST(APITest<FstatWrapper>, FstatException) {
  t_re_ = "test";
  try {
    mds_->Fstat(t_opts_, &t_ret_);
  } catch (std::string& re) {
    ASSERT_EQ(re, "test");
    return;
  }
  ASSERT_TRUE(false) << "No exception!";
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
