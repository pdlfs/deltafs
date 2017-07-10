/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "rados_conn.h"

#include "pdlfs-common/ofs.h"
#include "pdlfs-common/testharness.h"

#include <algorithm>
#include <vector>

// The following tests are paired with "$top_srcdir/dev/rados.sh".
// Run "sh $top_srcdir/dev/rados.sh start" to create a new rados cluster
// on the local machine to prepare an environment necessary
// to run the following tests.
// Root permission is required in order to run this script.
// Otherwise, set the following flag to TRUE to run tests against a simulated
// rados cluster.
#if defined(PDLFS_GFLAGS)
#include <gflags/gflags.h>
DEFINE_bool(useposixosd, true, "Use POSIX to simulate a ceph rados cluster");
#else
static const bool FLAGS_useposixosd = true;
#endif

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

static void TestRWEnvFile(Env* env, const char* dirname, const char* fname) {
  for (int i = 0; i < 3; i++) {
    env->DeleteFile(fname);
    ASSERT_OK(WriteStringToFile(env, Slice("xxxxxxxyyyyzz"), fname));
    ASSERT_TRUE(env->FileExists(fname));
    std::string tmp;
    ASSERT_OK(ReadFileToString(env, fname, &tmp));
    ASSERT_EQ(Slice(tmp), Slice("xxxxxxxyyyyzz"));
    std::vector<std::string> names;
    ASSERT_OK(env->GetChildren(dirname, &names));
    Slice slice = fname;
    slice.remove_prefix(strlen(dirname) + 1);
    std::string name = slice.ToString();
    ASSERT_TRUE(std::find(names.begin(), names.end(), name) != names.end());
  }

  env->DeleteFile(fname);
}

class RadosTest {
 public:
  RadosTest();

  // Reload the working dir.
  // Check the existence of a specified file under the next context.
  void Reload(const char* fname) {
    fprintf(stderr, "Reloading...\n");
    ASSERT_OK(env_->DetachDir(working_dir_.c_str()));
    ASSERT_OK(env_->CreateDir(working_dir_.c_str()));
    ASSERT_TRUE(env_->FileExists(fname));
  }

  // Reload the working dir readonly.
  // Check the existence of a specified file under the next context.
  void ReloadReadonly(const char* fname) {
    fprintf(stderr, "Reloading readonly...\n");
    ASSERT_OK(env_->DetachDir(working_dir_.c_str()));
    ASSERT_OK(env_->AttachDir(working_dir_.c_str()));
    ASSERT_TRUE(env_->FileExists(fname));
  }

  ~RadosTest();
  std::string pool_name_;
  std::string working_dir_;
  std::string root_;
  Osd* osd_;
  Env* env_;
};

RadosTest::RadosTest() {
  pool_name_ = "metadata";
  root_ = test::PrepareTmpDir("rados_test");
  working_dir_ = root_;
  if (FLAGS_useposixosd) {
    std::string osd_root = test::PrepareTmpDir("rados_test_objs");
    osd_ = Osd::FromEnv(osd_root.c_str());
  } else {
    port::InitOnce(&once, OpenRadosConn);
    Status s = rados_conn->OpenOsd(&osd_, pool_name_);
    ASSERT_OK(s);
  }
  Status s = rados_conn->OpenEnv(&env_, root_, pool_name_, osd_);
  ASSERT_OK(s);
  env_->CreateDir(working_dir_.c_str());
}

RadosTest::~RadosTest() {
  env_->DeleteDir(working_dir_.c_str());
  delete env_;
  delete osd_;
}

TEST(RadosTest, OSD_PutAndExists) {
  const char* name = "a";
  osd_->Delete(name);
  ASSERT_OK(osd_->Put(name, Slice()));
  ASSERT_TRUE(osd_->Exists(name));
  osd_->Delete(name);
  ASSERT_TRUE(!osd_->Exists(name));
}

TEST(RadosTest, OSD_ReadWrite) {
  const char* name = "a";
  const char* data = "xxxxxxxyyyyzz";
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
  const char* src = "a";
  const char* dst = "b";
  const char* data = "xxxxxxxyyyyzz";
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
  std::string lockname = LockFileName(working_dir_);
  ASSERT_OK(env_->LockFile(lockname.c_str(), &lock));
  ASSERT_OK(env_->UnlockFile(lock));
  ASSERT_OK(env_->DeleteFile(lockname.c_str()));
}

TEST(RadosTest, SetCurrentFile) {
  ASSERT_OK(SetCurrentFile(env_, working_dir_, 1));
  std::string curr = CurrentFileName(working_dir_);
  ASSERT_TRUE(env_->FileExists(curr.c_str()));
  ASSERT_OK(env_->DeleteFile(curr.c_str()));
}

TEST(RadosTest, ReadWriteFiles) {
  std::vector<std::string> fnames;
  fnames.push_back(DescriptorFileName(working_dir_, 1));
  fnames.push_back(LogFileName(working_dir_, 2));
  fnames.push_back(TableFileName(working_dir_, 3));
  fnames.push_back(SSTTableFileName(working_dir_, 4));
  fnames.push_back(TempFileName(working_dir_, 5));
  fnames.push_back(InfoLogFileName(working_dir_));
  fnames.push_back(OldInfoLogFileName(working_dir_));
  for (size_t i = 0; i < fnames.size(); i++) {
    TestRWEnvFile(env_, working_dir_.c_str(), fnames[i].c_str());
  }
}

TEST(RadosTest, Reloading) {
  std::string fname = TableFileName(working_dir_, 7);
  for (int i = 0; i < 3; i++) {
    WriteStringToFile(env_, "xxxxxxxxx", fname.c_str());
    ReloadReadonly(fname.c_str());
    ReloadReadonly(fname.c_str());
    ReloadReadonly(fname.c_str());
    Reload(fname.c_str());
    Reload(fname.c_str());
    Reload(fname.c_str());
    Reload(fname.c_str());
  }

  ASSERT_OK(env_->DeleteFile(fname.c_str()));
}

}  // namespace rados
}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
