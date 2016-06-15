/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <algorithm>
#include <vector>

#include "pdlfs-common/dbfiles.h"
#include "pdlfs-common/osd_env.h"
#include "pdlfs-common/testharness.h"

#include "rados_api.h"

// The following tests are paired with "rados.sh".
// Run "sh rados.sh start" to create a new Ceph cluster on the local machine
// to prepare the environment necessary to back the tests.
// Root permission is needed in order to run this script.
// Otherwise, set the following flag to TRUE to run tests against a simulated
// Ceph rados cluster.
#if defined(GFLAGS)
#include <gflags/gflags.h>
DEFINE_bool(use_posix_osd, true, "Use POSIX to simulate a ceph rados cluster");
#else
static const bool FLAGS_use_posix_osd = true;
#endif

namespace pdlfs {
namespace rados {

static const int kVerbose = 0;

static void TestReadWriteFile(Env* env, const Slice& dirname,
                              const Slice& fname) {
  const char data[] = "xxxxxxxyyyyzz";
  env->DeleteFile(fname);
  ASSERT_OK(WriteStringToFile(env, Slice(data), fname));
  ASSERT_TRUE(env->FileExists(fname));
  std::string tmp;
  ASSERT_OK(ReadFileToString(env, fname, &tmp));
  ASSERT_EQ(Slice(tmp), Slice(data));
  std::vector<std::string> names;
  ASSERT_OK(env->GetChildren(dirname, &names));
  Slice slice = fname;
  slice.remove_prefix(dirname.size() + 1);
  std::string name = slice.ToString();
  ASSERT_TRUE(std::find(names.begin(), names.end(), name) != names.end());
  env->DeleteFile(fname);
}

class RadosTest {
 public:
  OSD* osd_;
  Env* env_;
  RadosOptions opts_;
  std::string root_;

  std::string WorkingDir() { return root_ + "/dbhome"; }

  RadosTest() {
    root_ = test::NewTmpDirectory("rados_test");
    if (FLAGS_use_posix_osd) {
      std::string osd_root = test::NewTmpDirectory("rados_test_osd");
      osd_ = NewOSDAdaptor(osd_root);
    } else {
      opts_.conf_path = "/tmp/pdlfs-rados/ceph.conf";
      opts_.pool_name = "metadata";
      opts_.client_mount_timeout = 1;
      opts_.mon_op_timeout = 1;
      opts_.osd_op_timeout = 1;
      osd_ = NewRadosOSD(opts_);
    }
    env_ = NewRadosEnv(opts_, root_, osd_);
    env_->CreateDir(WorkingDir());
  }

  ~RadosTest() {
    env_->DeleteDir(WorkingDir());
    delete osd_;
    delete env_;
  }

  void Reload() {
    if (kVerbose > 0) {
      fprintf(stderr, "Reloading ...\n");
    }
    ASSERT_OK(SoftDeleteDir(env_, WorkingDir()));
    ASSERT_OK(env_->CreateDir(WorkingDir()));
    if (kVerbose > 0) {
      fprintf(stderr, "Reloading done\n");
    }
  }

  void ReloadReadonly() {
    if (kVerbose > 0) {
      fprintf(stderr, "Reloading read-only ...\n");
    }
    ASSERT_OK(SoftDeleteDir(env_, WorkingDir()));
    ASSERT_OK(SoftCreateDir(env_, WorkingDir()));
    if (kVerbose > 0) {
      fprintf(stderr, "Reloading read-only done\n");
    }
  }
};

TEST(RadosTest, OSD_PutAndExists) {
  const std::string name = "a";
  osd_->Delete(name);
  ASSERT_OK(osd_->Put(name, Slice()));
  ASSERT_TRUE(osd_->Exists(name));
  osd_->Delete(name);
  ASSERT_TRUE(!osd_->Exists(name));
}

TEST(RadosTest, OSD_ReadWrite) {
  const std::string name = "a";
  const char data[] = "xxxxxxxyyyyzz";
  osd_->Delete(name);
  ASSERT_OK(WriteStringToFileSync(osd_, Slice(data), name));
  uint64_t size;
  ASSERT_OK(osd_->Size(name, &size));
  ASSERT_TRUE(size == strlen(data));
  std::string tmp;
  ASSERT_OK(ReadFileToString(osd_, name, &tmp));
  ASSERT_EQ(Slice(tmp), Slice(data));
  osd_->Delete(name);
}

TEST(RadosTest, OSD_PutGetCopy) {
  const std::string src = "a";
  const std::string dst = "b";
  const char data[] = "xxxxxxxyyyyzz";
  std::string tmp;
  osd_->Delete(src);
  osd_->Delete(dst);
  ASSERT_OK(osd_->Put(src, Slice(data)));
  ASSERT_OK(osd_->Copy(src, dst));
  ASSERT_OK(osd_->Get(dst, &tmp));
  ASSERT_EQ(Slice(tmp), Slice(data));
  osd_->Delete(src);
  osd_->Delete(dst);
}

TEST(RadosTest, FileLock) {
  FileLock* lock;
  ASSERT_OK(env_->LockFile(LockFileName(WorkingDir()), &lock));
  ASSERT_OK(env_->UnlockFile(lock));
  ASSERT_OK(env_->DeleteFile(LockFileName(WorkingDir())));
}

TEST(RadosTest, SetCurrentFile) {
  ASSERT_OK(SetCurrentFile(env_, WorkingDir(), 1));
  ASSERT_TRUE(env_->FileExists(CurrentFileName(WorkingDir())));
  ASSERT_OK(env_->DeleteFile(CurrentFileName(WorkingDir())));
}

TEST(RadosTest, Reloading) {
  std::string fname = TableFileName(WorkingDir(), 4);
  for (int i = 0; i < 3; i++) {
    WriteStringToFile(env_, "xxxxxxxxx", fname);
    ASSERT_TRUE(env_->FileExists(fname));
    ReloadReadonly();
    ASSERT_TRUE(env_->FileExists(fname));
    ReloadReadonly();
    ASSERT_TRUE(env_->FileExists(fname));
    ReloadReadonly();
    ASSERT_TRUE(env_->FileExists(fname));
    Reload();
    ASSERT_TRUE(env_->FileExists(fname));
    Reload();
    ASSERT_TRUE(env_->FileExists(fname));
    Reload();
    ASSERT_TRUE(env_->FileExists(fname));
    Reload();
  }

  ASSERT_OK(env_->DeleteFile(fname));
}

TEST(RadosTest, ReadWriteFile) {
  TestReadWriteFile(env_, WorkingDir(), DescriptorFileName(WorkingDir(), 1));
  TestReadWriteFile(env_, WorkingDir(), LogFileName(WorkingDir(), 2));
  TestReadWriteFile(env_, WorkingDir(), TableFileName(WorkingDir(), 3));
  TestReadWriteFile(env_, WorkingDir(), InfoLogFileName(WorkingDir()));
  TestReadWriteFile(env_, WorkingDir(), OldInfoLogFileName(WorkingDir()));
}

}  // namespace rados
}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
