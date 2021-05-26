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
#include "pdlfs-common/ofs.h"

#include "pdlfs-common/env.h"
#include "pdlfs-common/osd.h"
#include "pdlfs-common/testharness.h"

#include <set>

namespace pdlfs {

class OFS {
 public:
  OFS() {
    fsetpath_ = "/mnt/fset";
    root_ = test::PrepareTmpDir("ofs_test");
    osd_ = Osd::FromEnv(root_.c_str());
    ofs_ = new Ofs(options_, osd_);
  }

  ~OFS() {
    delete ofs_;
    delete osd_;
  }

  bool Mounted() { return ofs_->FileSetExists(fsetpath_.c_str()); }

  Status Mount() { return ofs_->MountFileSet(mount_opts_, fsetpath_.c_str()); }

  Status Unmount() {
    return ofs_->UnmountFileSet(unmount_opts_, fsetpath_.c_str());
  }

  std::set<std::string> List() {
    std::vector<std::string> list;
    ofs_->GetChildren(fsetpath_.c_str(), &list);
    std::set<std::string> rv;
    for (size_t i = 0; i < list.size(); i++) {
      rv.insert(list[i]);
    }
    return rv;
  }

  Status Access(const char* fname) {
    SequentialFile* file;
    std::string f = fsetpath_ + "/" + fname;
    Status s = ofs_->NewSequentialFile(f.c_str(), &file);
    if (s.ok()) {
      delete file;
    }
    return s;
  }

  Status Create(const char* fname) {
    WritableFile* file;
    std::string f = fsetpath_ + "/" + fname;
    Status s = ofs_->NewWritableFile(f.c_str(), &file);
    if (s.ok()) {
      file->Append("xyz");
      file->Close();
      delete file;
    }
    return s;
  }

  Status Copy(const char* fname1, const char* fname2) {
    std::string f1 = fsetpath_ + "/" + fname1;
    std::string f2 = fsetpath_ + "/" + fname2;
    return ofs_->CopyFile(f1.c_str(), f2.c_str());
  }

  Status Rename(const char* fname1, const char* fname2) {
    std::string f1 = fsetpath_ + "/" + fname1;
    std::string f2 = fsetpath_ + "/" + fname2;
    return ofs_->Rename(f1.c_str(), f2.c_str());
  }

  Status Delete(const char* fname) {
    std::string f = fsetpath_ + "/" + fname;
    return ofs_->DeleteFile(f.c_str());
  }

  bool Exists(const char* fname) {
    std::string f = fsetpath_ + "/" + fname;
    return ofs_->FileExists(f.c_str());
  }

  std::string fsetpath_;
  std::string root_;
  MountOptions mount_opts_;
  UnmountOptions unmount_opts_;
  OfsOptions options_;
  Ofs* ofs_;
  Osd* osd_;
};

TEST(OFS, Empty) {
  ASSERT_TRUE(!ofs_->FileExists(fsetpath_.c_str()));
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
  ASSERT_OK(Create("a"));
  ASSERT_OK(Create("b"));
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Create("a"));
  ASSERT_OK(Create("b"));
  ASSERT_OK(Create("c"));
  ASSERT_OK(Unmount());
  ASSERT_OK(Mount());
  ASSERT_OK(Access("a"));
  ASSERT_OK(Access("b"));
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Create("a"));
  ASSERT_OK(Create("b"));
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Create("d"));
  ASSERT_OK(Unmount());
  ASSERT_OK(Mount());
  ASSERT_TRUE(!Exists("a"));
  ASSERT_TRUE(!Exists("b"));
  ASSERT_OK(Access("c"));
  ASSERT_OK(Access("d"));
  ASSERT_OK(Delete("c"));
  ASSERT_OK(Delete("d"));
  ASSERT_OK(Unmount());
  ASSERT_OK(Mount());
  ASSERT_TRUE(!Exists("c"));
  ASSERT_TRUE(!Exists("d"));
  unmount_opts_.deletion = true;
  ASSERT_OK(Unmount());
}

TEST(OFS, List) {
  ASSERT_OK(Mount());
  ASSERT_OK(Create("a"));
  ASSERT_OK(Create("b"));
  ASSERT_OK(Create("c"));
  ASSERT_OK(Unmount());
  ASSERT_OK(Mount());
  std::set<std::string> files1 = List();
  ASSERT_EQ(files1.size(), 3);
  ASSERT_EQ(files1.count("a"), 1);
  ASSERT_EQ(files1.count("b"), 1);
  ASSERT_EQ(files1.count("c"), 1);
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Create("d"));
  ASSERT_OK(Create("e"));
  std::set<std::string> files2 = List();
  ASSERT_EQ(files2.size(), 3);
  ASSERT_EQ(files2.count("c"), 1);
  ASSERT_EQ(files2.count("d"), 1);
  ASSERT_EQ(files2.count("e"), 1);
  ASSERT_OK(Unmount());
  ASSERT_OK(Mount());
  ASSERT_OK(Delete("c"));
  ASSERT_OK(Delete("d"));
  ASSERT_OK(Delete("e"));
  ASSERT_OK(Unmount());
  ASSERT_OK(Mount());
  std::set<std::string> files3 = List();
  ASSERT_EQ(files3.size(), 0);
  unmount_opts_.deletion = true;
  ASSERT_OK(Unmount());
}

TEST(OFS, Copy) {
  ASSERT_OK(Mount());
  ASSERT_OK(Create("a"));
  ASSERT_OK(Copy("a", "b"));
  ASSERT_OK(Access("a"));
  ASSERT_OK(Access("b"));
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("b"));
  unmount_opts_.deletion = true;
  ASSERT_OK(Unmount());
}

TEST(OFS, Rename) {
  ASSERT_OK(Mount());
  ASSERT_OK(Create("a"));
  ASSERT_OK(Rename("a", "b"));
  ASSERT_OK(Access("b"));
  ASSERT_OK(Delete("b"));
  ASSERT_TRUE(!Exists("a"));
  ASSERT_TRUE(!Exists("b"));
  unmount_opts_.deletion = true;
  ASSERT_OK(Unmount());
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
