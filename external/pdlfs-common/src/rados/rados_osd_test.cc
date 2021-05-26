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
#include "rados_osd.h"

#include "pdlfs-common/testharness.h"

#include <stdio.h>
#include <string.h>

// Parameters for opening ceph.
namespace {
const char* FLAGS_user_name = "client.admin";
const char* FLAGS_rados_cluster_name = "ceph";
const char* FLAGS_pool_name = "test";
const char* FLAGS_conf = NULL;  // Use ceph defaults
}  // namespace

namespace pdlfs {
namespace rados {

class RadosOsdTest {
 public:
  RadosOsdTest() {
    RadosConnMgrOptions options;
    mgr_ = new RadosConnMgr(options);
    osd_ = NULL;
  }

  void Open() {
    RadosConn* conn;
    ASSERT_OK(mgr_->OpenConn(FLAGS_rados_cluster_name, FLAGS_user_name,
                             FLAGS_conf, RadosConnOptions(), &conn));
    ASSERT_OK(mgr_->OpenOsd(conn, FLAGS_pool_name, RadosOptions(), &osd_));
    mgr_->Release(conn);
  }

  ~RadosOsdTest() {
    delete osd_;
    delete mgr_;
  }

  RadosConnMgr* mgr_;
  Osd* osd_;
};

TEST(RadosOsdTest, PutAndExists) {
  Open();
  const char* name = "a";
  osd_->Delete(name);
  ASSERT_OK(osd_->Put(name, Slice()));
  ASSERT_TRUE(osd_->Exists(name));
  osd_->Delete(name);
  ASSERT_TRUE(!osd_->Exists(name));
}

TEST(RadosOsdTest, ReadWrite) {
  Open();
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

TEST(RadosOsdTest, PutGetCopy) {
  Open();
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
