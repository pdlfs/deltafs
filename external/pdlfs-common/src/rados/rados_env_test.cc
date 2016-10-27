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

#include "pdlfs-common/osd_env.h"
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/testharness.h"

#include "rados_conn.h"

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

static const int kVerbose = 5;

static void TestRWEnvFile(Env* env, const Slice& dirname, const Slice& fname) {
  const char data[] = "xxxxxxxyyyyzz";
  for (int i = 0; i < 3; i++) {
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
  }
  env->DeleteFile(fname);
}

// Make sure we only connect to rados once during the entire run.
static port::OnceType once = PDLFS_ONCE_INIT;
static RadosConn* rados_conn = NULL;
static void OpenRadosConn() {
  rados_conn = new RadosConn;
  Status s = rados_conn->Open(RadosOptions());
  ASSERT_OK(s);
}

class RadosTest {
 public:
  RadosTest() {
    Status s;
    std::string pool_name = "metadata";
    root_ = test::NewTmpDirectory("rados_test");
    if (FLAGS_useposixosd) {
      std::string osd_root = test::NewTmpDirectory("rados_test_objs");
      osd_ = NewOSDAdaptor(osd_root);
    } else {
      port::InitOnce(&once, OpenRadosConn);
      s = rados_conn->OpenOsd(&osd_, pool_name);
      ASSERT_OK(s);
    }
    s = rados_conn->OpenEnv(&env_, root_, pool_name, osd_);
    ASSERT_OK(s);
    env_->CreateDir(WorkingDir());
  }

  ~RadosTest() {
    env_->DeleteDir(WorkingDir());
    delete env_;
    delete osd_;
  }

  // Reload the working dir.
  // Check the existence of a specified file under the next context.
  void Reload(const Slice& f) {
    Verbose(__LOG_ARGS__, kVerbose, "Reloading ... ");
    ASSERT_OK(env_->DetachDir(WorkingDir()));
    ASSERT_OK(env_->CreateDir(WorkingDir()));
    ASSERT_TRUE(env_->FileExists(f));
  }

  // Reload the working dir readonly.
  // Check the existence of a specified file under the next context.
  void ReloadReadonly(const Slice& f) {
    Verbose(__LOG_ARGS__, kVerbose, "Reloading readonly ... ");
    ASSERT_OK(env_->DetachDir(WorkingDir()));
    ASSERT_OK(env_->AttachDir(WorkingDir()));
    ASSERT_TRUE(env_->FileExists(f));
  }

  std::string WorkingDir() {
    return root_ + "/dbhome";  // emulating a leveldb home
  }

  std::string root_;
  OSD* osd_;
  Env* env_;
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

TEST(RadosTest, ReadWriteFiles) {
  TestRWEnvFile(env_, WorkingDir(), DescriptorFileName(WorkingDir(), 1));
  TestRWEnvFile(env_, WorkingDir(), LogFileName(WorkingDir(), 2));
  TestRWEnvFile(env_, WorkingDir(), TableFileName(WorkingDir(), 3));
  TestRWEnvFile(env_, WorkingDir(), InfoLogFileName(WorkingDir()));
  TestRWEnvFile(env_, WorkingDir(), OldInfoLogFileName(WorkingDir()));
}

TEST(RadosTest, Reloading) {
  std::string fname = TableFileName(WorkingDir(), 4);
  for (int i = 0; i < 3; i++) {
    WriteStringToFile(env_, "xxxxxxxxx", fname);
    ReloadReadonly(fname);
    ReloadReadonly(fname);
    ReloadReadonly(fname);
    Reload(fname);
    Reload(fname);
    Reload(fname);
    Reload(fname);
  }

  ASSERT_OK(env_->DeleteFile(fname));
}

}  // namespace rados
}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
