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

#include "pdlfs-common/env.h"
#include "pdlfs-common/ofs.h"
#include "pdlfs-common/osd.h"

namespace pdlfs {

class OFS {
 public:
  OFS() {
    root_ = test::PrepareTmpDir("ofs_test");
    osd_ = Osd::FromEnv(root_.c_str());
    ofs_ = new Ofs(osd_);
  }

  ~OFS() {
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

TEST(OFS, Empty) {
  ASSERT_TRUE(!ofs_->FileExists("/mnt/fset/a"));
  ASSERT_TRUE(!Mounted());
  ASSERT_NOTFOUND(Unmount());
}

TEST(OFS, MountUnmount) {
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

TEST(OFS, CreateDeleteFile) {
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
