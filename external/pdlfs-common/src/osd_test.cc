/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/osd.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/osd_env.h"
#include "pdlfs-common/testharness.h"

namespace pdlfs {

class OSDTest {
 public:
  MountOptions mount_opts_;
  UnmountOptions unmount_opts_;
  std::string root_;
  OSD* osd_;
  OSDEnv* ose_;

  OSDTest() {
    root_ = test::NewTmpDirectory("osd_test");
    osd_ = NewOSDAdaptor(root_);
    ose_ = new OSDEnv(osd_);
  }

  ~OSDTest() {
    delete ose_;
    delete osd_;
  }

  bool Mounted() {
    bool r = ose_->FileSetExists("/mnt/fset");
    return r;
  }

  Status Mount() {
    Status s = ose_->MountFileSet(mount_opts_, "/mnt/fset");
    return s;
  }

  Status Unmount() {
    Status s = ose_->UnmountFileSet(unmount_opts_, "/mnt/fset");
    return s;
  }
};

TEST(OSDTest, Empty) {
  ASSERT_TRUE(!ose_->FileExists("/mnt/fset/a"));
  ASSERT_TRUE(!Mounted());
  ASSERT_NOTFOUND(Unmount());
}

TEST(OSDTest, MountUnmount) {
  mount_opts_.read_only = true;
  ASSERT_NOTFOUND(Mount());
  mount_opts_.read_only = false;
  mount_opts_.create_if_missing = false;
  ASSERT_NOTFOUND(Mount());
  mount_opts_.create_if_missing = true;
  ASSERT_OK(Mount());
  ASSERT_TRUE(Mounted());
  ASSERT_CONFLICT(Mount());
  ASSERT_OK(Unmount());
  mount_opts_.error_if_exists = true;
  ASSERT_CONFLICT(Mount());
  mount_opts_.error_if_exists = false;
  mount_opts_.read_only = true;
  ASSERT_OK(Mount());
  unmount_opts_.deletion = true;
  ASSERT_OK(Unmount());
  mount_opts_.create_if_missing = false;
  ASSERT_NOTFOUND(Mount());
}

TEST(OSDTest, CreateDeleteFile) {
  ASSERT_OK(Mount());
  WritableFile* wf;
  ASSERT_OK(ose_->NewWritableFile("/mnt/fset/a", &wf));
  wf->Close();
  delete wf;
  ASSERT_OK(Unmount());
  ASSERT_OK(Mount());
  SequentialFile* sf;
  ASSERT_OK(ose_->NewSequentialFile("/mnt/fset/a", &sf));
  delete sf;
  ASSERT_OK(ose_->DeleteFile("/mnt/fset/a"));
  ASSERT_OK(Unmount());
  ASSERT_OK(Mount());
  ASSERT_TRUE(!ose_->FileExists("/mnt/fset/a"));
  unmount_opts_.deletion = true;
  ASSERT_OK(Unmount());
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
