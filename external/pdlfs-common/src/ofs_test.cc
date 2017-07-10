/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/ofs.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/osd.h"
#include "pdlfs-common/testharness.h"

namespace pdlfs {

class OsdTest {
 public:
  OsdTest() {
    root_ = test::PrepareTmpDir("osd_test");
    osd_ = Osd::FromEnv(root_.c_str());
    ofs_ = new Ofs(osd_);
  }

  ~OsdTest() {
    delete ofs_;
    delete osd_;
  }

  bool Mounted() {
    bool r = ofs_->FileSetExists("/mnt/fset");
    return r;
  }

  Status Mount() {
    Status s = ofs_->MountFileSet(mount_opts_, "/mnt/fset");
    return s;
  }

  Status Unmount() {
    Status s = ofs_->UnmountFileSet(unmount_opts_, "/mnt/fset");
    return s;
  }

  MountOptions mount_opts_;
  UnmountOptions unmount_opts_;
  std::string root_;
  Ofs* ofs_;
  Osd* osd_;
};

TEST(OsdTest, Empty) {
  ASSERT_TRUE(!ofs_->FileExists("/mnt/fset/a"));
  ASSERT_TRUE(!Mounted());
  ASSERT_NOTFOUND(Unmount());
}

TEST(OsdTest, MountUnmount) {
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

TEST(OsdTest, CreateDeleteFile) {
  ASSERT_OK(Mount());
  WritableFile* wf;
  ASSERT_OK(ofs_->NewWritableFile("/mnt/fset/a", &wf));
  wf->Close();
  delete wf;
  ASSERT_OK(Unmount());
  ASSERT_OK(Mount());
  SequentialFile* sf;
  ASSERT_OK(ofs_->NewSequentialFile("/mnt/fset/a", &sf));
  delete sf;
  ASSERT_OK(ofs_->DeleteFile("/mnt/fset/a"));
  ASSERT_OK(Unmount());
  ASSERT_OK(Mount());
  ASSERT_TRUE(!ofs_->FileExists("/mnt/fset/a"));
  unmount_opts_.deletion = true;
  ASSERT_OK(Unmount());
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
