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
#include "rados_db_env.h"

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/options.h"
#include "pdlfs-common/testharness.h"

#include <algorithm>
#include <stdio.h>
#include <string.h>
#include <vector>

// Parameters for opening ceph.
namespace {
const char* FLAGS_user_name = "client.admin";
const char* FLAGS_rados_cluster_name = "ceph";
const char* FLAGS_pool_name = "test";
const char* FLAGS_conf = NULL;  // Use ceph defaults
}  // namespace

namespace pdlfs {
namespace rados {

class RadosDbEnvTest {
 public:
  RadosDbEnvTest() : working_dir_(test::TmpDir() + "/rados_env_test") {
    RadosConnMgrOptions options;
    mgr_ = new RadosConnMgr(options);
    env_ = NULL;
  }

  void Open() {
    RadosConn* conn;
    Osd* osd;
    ASSERT_OK(mgr_->OpenConn(  ///
        FLAGS_rados_cluster_name, FLAGS_user_name, FLAGS_conf,
        RadosConnOptions(), &conn));
    ASSERT_OK(mgr_->OpenOsd(conn, FLAGS_pool_name, RadosOptions(), &osd));
    env_ = mgr_->OpenEnv(osd, true, RadosEnvOptions());
    env_ = mgr_->CreateDbEnvWrapper(env_, true, RadosDbEnvOptions());
    // Mount the directory read-write
    env_->CreateDir(working_dir_.c_str());
    DBOptions options;
    options.env = env_;
    DestroyDB(working_dir_, options);  // This will delete the dir
    env_->CreateDir(working_dir_.c_str());
    mgr_->Release(conn);
  }

  ~RadosDbEnvTest() {
    env_->DetachDir(working_dir_.c_str());
    delete env_;
    delete mgr_;
  }

  std::string GetFromDb(const std::string& key, DB* db) {
    std::string tmp;
    Status s = db->Get(ReadOptions(), key, &tmp);
    if (s.IsNotFound()) {
      tmp = "NOT_FOUND";
    } else if (!s.ok()) {
      tmp = s.ToString();
    }
    return tmp;
  }

  std::string working_dir_;
  RadosConnMgr* mgr_;
  Env* env_;
};

TEST(RadosDbEnvTest, FileLock) {
  Open();
  FileLock* lock;
  std::string fname = LockFileName(working_dir_);
  ASSERT_OK(env_->LockFile(fname.c_str(), &lock));
  ASSERT_OK(env_->UnlockFile(lock));
  ASSERT_OK(env_->DeleteFile(fname.c_str()));
}

TEST(RadosDbEnvTest, SetCurrentFile) {
  Open();
  ASSERT_OK(SetCurrentFile(env_, working_dir_, 1));
  std::string fname = CurrentFileName(working_dir_);
  ASSERT_TRUE(env_->FileExists(fname.c_str()));
  ASSERT_OK(env_->DeleteFile(fname.c_str()));
}

TEST(RadosDbEnvTest, ListDbFiles) {
  Open();
  std::vector<std::string> fnames;
  fnames.push_back(DescriptorFileName(working_dir_, 1));
  fnames.push_back(LogFileName(working_dir_, 2));
  fnames.push_back(TableFileName(working_dir_, 3));
  fnames.push_back(SSTTableFileName(working_dir_, 4));
  fnames.push_back(TempFileName(working_dir_, 5));
  fnames.push_back(InfoLogFileName(working_dir_));
  fnames.push_back(OldInfoLogFileName(working_dir_));
  for (size_t i = 0; i < fnames.size(); i++) {
    ASSERT_OK(WriteStringToFile(env_, "xyz", fnames[i].c_str()));
  }
  std::vector<std::string> r;
  ASSERT_OK(env_->GetChildren(working_dir_.c_str(), &r));
  for (size_t i = 0; i < fnames.size(); i++) {
    ASSERT_TRUE(std::find(r.begin(), r.end(),
                          fnames[i].substr(working_dir_.size() + 1)) !=
                r.end());
    ASSERT_OK(env_->DeleteFile(fnames[i].c_str()));
  }
}

TEST(RadosDbEnvTest, Db) {
  Open();
  DBOptions options;
  options.info_log = Logger::Default();
  options.max_mem_compact_level = 0;
  options.sync_log_on_close = true;
  options.create_if_missing = true;
  options.env = env_;
  DB* db;
  ASSERT_OK(DB::Open(options, working_dir_, &db));
  WriteOptions wo;
  ASSERT_OK(db->Put(wo, "k1", "v1"));
  FlushOptions fo;
  ASSERT_OK(db->FlushMemTable(fo));
  ASSERT_OK(db->Put(wo, "k2", "v2"));
  // This will force the generation of a new L0 table and then a merge of two L0
  // tables into a new L1 table
  db->CompactRange(NULL, NULL);
  ASSERT_OK(db->Put(wo, "k3", "v3"));  // Leaves data in the write-ahead log
  delete db;
  options.error_if_exists = false;
  ASSERT_OK(DB::Open(options, working_dir_, &db));
  ASSERT_EQ("v3", GetFromDb("k3", db));
  ASSERT_EQ("v2", GetFromDb("k2", db));
  ASSERT_EQ("v1", GetFromDb("k1", db));
  delete db;
}

}  // namespace rados
}  // namespace pdlfs

namespace {
inline void PrintUsage() {
  fprintf(stderr, "Use --cluster, --user, --conf, and --pool to conf test.\n");
  exit(1);
}

void ParseArgs(int argc, char* argv[]) {
  for (int i = 1; i < argc; ++i) {
    ::pdlfs::Slice a = argv[i];
    if (a.starts_with("--cluster=")) {
      FLAGS_rados_cluster_name = argv[i] + strlen("--cluster=");
    } else if (a.starts_with("--user=")) {
      FLAGS_user_name = argv[i] + strlen("--user=");
    } else if (a.starts_with("--conf=")) {
      FLAGS_conf = argv[i] + strlen("--conf=");
    } else if (a.starts_with("--pool=")) {
      FLAGS_pool_name = argv[i] + strlen("--pool=");
    } else {
      PrintUsage();
    }
  }

  printf("Cluster name: %s\n", FLAGS_rados_cluster_name);
  printf("User name: %s\n", FLAGS_user_name);
  printf("Storage pool: %s\n", FLAGS_pool_name);
  printf("Conf: %s\n", FLAGS_conf);
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc > 1) {
    ParseArgs(argc, argv);
    return ::pdlfs::test::RunAllTests(&argc, &argv);
  } else {
    return 0;
  }
}
