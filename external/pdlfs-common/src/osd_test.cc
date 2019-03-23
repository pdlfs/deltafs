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

#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

#include "pdlfs-common/env.h"
#include "pdlfs-common/ofs.h"
#include "pdlfs-common/osd.h"

namespace pdlfs {

class OSD {
 public:
  OSD() : rnd_(test::RandomSeed()) {
    Env* env = Env::Default();
    root_ = test::PrepareTmpDir("osd_test", env);
    osd_ = Osd::FromEnv(root_.c_str(), env);
  }

  ~OSD() {
    if (osd_) {
      delete osd_;
    }
  }

  Random rnd_;
  std::string root_;
  Osd* osd_;
};

TEST(OSD, ReadWriteExists) {
  std::string rnddatastor;
  Slice rnddata = test::RandomString(&rnd_, 16, &rnddatastor);
  WriteStringToFile(osd_, rnddata, "name1");
  ASSERT_TRUE(osd_->Exists("name1"));
  uint64_t size = 0;
  ASSERT_OK(osd_->Size("name1", &size));
  ASSERT_EQ(size, rnddata.size());
  std::string tmp;
  ReadFileToString(osd_, "name1", &tmp);
  ASSERT_EQ(tmp, rnddata);
}

TEST(OSD, PutGet) {
  std::string rnddatastor;
  Slice rnddata = test::RandomString(&rnd_, 16, &rnddatastor);
  ASSERT_OK(osd_->Put("name1", rnddata));
  ASSERT_TRUE(osd_->Exists("name1"));
  uint64_t size = 0;
  ASSERT_OK(osd_->Size("name1", &size));
  ASSERT_EQ(size, rnddata.size());
  std::string tmp;
  ASSERT_OK(osd_->Get("name1", &tmp));
  ASSERT_EQ(tmp, rnddata);
  ASSERT_OK(osd_->Delete("name1"));
  ASSERT_FALSE(osd_->Exists("name1"));
}

TEST(OSD, Copy) {
  std::string rnddatastor;
  Slice rnddata = test::RandomString(&rnd_, 16, &rnddatastor);
  ASSERT_OK(osd_->Put("name1", rnddata));
  ASSERT_OK(osd_->Copy("name1", "name2"));
  ASSERT_TRUE(osd_->Exists("name2"));
  uint64_t size = 0;
  ASSERT_OK(osd_->Size("name2", &size));
  ASSERT_EQ(size, rnddata.size());
  std::string tmp;
  ASSERT_OK(osd_->Get("name2", &tmp));
  ASSERT_EQ(tmp, rnddata);
}

}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
