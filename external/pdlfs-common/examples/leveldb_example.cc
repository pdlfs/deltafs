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

#include <stdio.h>
#include <stdlib.h>
#include <string>

#include "pdlfs-common/env.h"
#include "pdlfs-common/leveldb/db/db.h"
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/slice.h"
#include "pdlfs-common/status.h"

#if defined(PDLFS_GFLAGS)
#include <gflags/gflags.h>
#endif

#if defined(PDLFS_GLOG)
#include <glog/logging.h>
#define Assert_true(b) LOG_ASSERT((b))
#define Assert_OK(s) LOG_ASSERT((s).ok())
#else
static inline void Assert_true(bool b) {
  if (!b) {
    abort();
  }
}
static inline void Assert_OK(const pdlfs::Status& s) {
  if (!s.ok()) {
    abort();
  }
}
#endif

int main(int argc, char* argv[]) {
#if defined(PDLFS_GLOG)
  FLAGS_logtostderr = true;
#endif
#if defined(PDLFS_GFLAGS)
  std::string usage("Sample usage: ");
  usage += argv[0];
  google::SetUsageMessage(usage);
  google::SetVersionString(PDLFS_COMMON_VERSION);
  google::ParseCommandLineFlags(&argc, &argv, true);
#endif
#if defined(PDLFS_GLOG)
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
#endif
  pdlfs::DB* db = NULL;
  pdlfs::DestroyDB("/tmp/pdlfs_leveldb", pdlfs::DBOptions());
  pdlfs::DBOptions options;
#if defined(PDLFS_GLOG)
  options.info_log = pdlfs::Logger::Default();
#endif
  options.create_if_missing = true;
  options.rotating_manifest = true;
  options.skip_lock_file = true;
  Assert_OK(pdlfs::DB::Open(options, "/tmp/pdlfs_leveldb", &db));
  Assert_true(db != NULL);

  // K1 -> ""
  Assert_OK(db->Put(pdlfs::WriteOptions(), "K1", pdlfs::Slice()));
  // K1 -> V1
  Assert_OK(db->Put(pdlfs::WriteOptions(), "K1", "V1"));
  // K2 -> ""
  Assert_OK(db->Delete(pdlfs::WriteOptions(), "K2"));
  // K2 -> V2
  Assert_OK(db->Put(pdlfs::WriteOptions(), "K2", "V2"));

  // Flush
  db->FlushMemTable(pdlfs::FlushOptions());

  // K3 -> K3
  Assert_OK(db->Put(pdlfs::WriteOptions(), "K3", "V3"));
  // K3 -> ""
  Assert_OK(db->Delete(pdlfs::WriteOptions(), "K3"));
  // K4 -> V4
  Assert_OK(db->Put(pdlfs::WriteOptions(), "K4", "V4"));

  // Get with a pre-allocated buffer
  char buf[100];
  pdlfs::Slice value;
  Assert_OK(db->Get(pdlfs::ReadOptions(), "K1", &value, buf, sizeof(buf)));
  Assert_true(value == "V1");
  Assert_OK(db->Get(pdlfs::ReadOptions(), "K2", &value, buf, sizeof(buf)));
  Assert_true(value == "V2");
  // Get with an empty buffer
  char empty[0];
  pdlfs::Status s = db->Get(pdlfs::ReadOptions(), "K4", &value, empty, 0);
  Assert_true(s.IsBufferFull());
  Assert_true(value.size() == pdlfs::Slice("V4").size());

  delete db;
  fprintf(stderr, "Done\n");
  return 0;
}
