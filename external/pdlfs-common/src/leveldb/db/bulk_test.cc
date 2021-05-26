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
#include "db_impl.h"

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/filenames.h"
#include "pdlfs-common/leveldb/options.h"

#include "pdlfs-common/cache.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/testharness.h"

#include <stdlib.h>

namespace pdlfs {

class BulkTest {
 public:
  std::string dbloc_;
  std::string dbtmp_;
  Cache* empty_cache_;
  std::vector<const Snapshot*> snapshots_;
  DBOptions options_;
  DB* db_;

  BulkTest() {
    dbloc_ = test::TmpDir() + "/bulk_test";
    DestroyDB(dbloc_, DBOptions());
    dbtmp_ = dbloc_ + "/tmp";
    DestroyDB(dbtmp_, DBOptions());
    empty_cache_ = NewLRUCache(0);
    options_.block_cache = empty_cache_;
    options_.create_if_missing = true;
    options_.compression = kNoCompression;
    ASSERT_OK(DB::Open(options_, dbloc_, &db_));
  }

  ~BulkTest() {
    ReleaseSnapshots();
    delete db_;
    delete empty_cache_;
  }

  void Put(const std::string& k, const std::string& v) {
    ASSERT_OK(db_->Put(WriteOptions(), k, v));
  }

  void Delete(const std::string& k) {
    ASSERT_OK(db_->Delete(WriteOptions(), k));
  }

  std::string Get(const std::string& k, const Snapshot* snapshot = NULL) {
    ReadOptions options;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  // Return the index of the new snapshot
  int DoSnapshot() {
    int index;
    snapshots_.push_back(db_->GetSnapshot());
    index = static_cast<int>(snapshots_.size()) - 1;
    return index;
  }

  void ReleaseSnapshots() {
    for (size_t i = 0; i < snapshots_.size(); i++) {
      db_->ReleaseSnapshot(snapshots_[i]);
    }
    snapshots_.clear();
  }

  void Flush() {
    DBImpl* impl = reinterpret_cast<DBImpl*>(db_);
    impl->TEST_CompactMemTable();
  }

  void Compact() {
    DBImpl* impl = reinterpret_cast<DBImpl*>(db_);
    impl->TEST_CompactRange(0, NULL, NULL);
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
    impl->TEST_CompactRange(1, NULL, NULL);
    ASSERT_EQ(0, NumTableFilesAtLevel(1));
  }

  int NumTableFilesAtLevel(int level) {
    std::string property;
    ASSERT_TRUE(db_->GetProperty(
        "leveldb.num-files-at-level" + NumberToString(level), &property));
    return atoi(property.c_str());
  }

  // Return total number of files copied.
  int CopyDbToTmp() {
    Env* env = options_.env;
    std::vector<std::string> filenames;
    ASSERT_OK(env->GetChildren(dbloc_.c_str(), &filenames));
    env->CreateDir(dbtmp_.c_str());
    uint64_t number;
    FileType type;
    int num = 0;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) && type == kTableFile) {
        std::string src = TableFileName(dbloc_, number);
        std::string dst = TableFileName(dbtmp_, number);
        ASSERT_OK(env->CopyFile(src.c_str(), dst.c_str()));
        num++;
      }
    }
    return num;
  }

  void BulkInsert(bool overlapping_keys = true, SequenceNumber seq = 0) {
    InsertOptions opt;
    opt.no_seq_adjustment = !overlapping_keys;
    opt.suggested_max_seq = seq;
    db_->AddL0Tables(opt, dbtmp_);
  }

  void Reopen(bool destroy = false) {
    ReleaseSnapshots();
    delete db_;
    if (destroy) {
      DestroyDB(dbloc_, options_);
    }
    ASSERT_OK(DB::Open(options_, dbloc_, &db_));
  }
};

TEST(BulkTest, NoOverlappingKeys) {
  Put("a", "v1");
  Put("p", "v1");
  Flush();
  CopyDbToTmp();
  Reopen(true);
  Put("b", "v1");
  Put("q", "v1");
  BulkInsert(false, 10);
  ASSERT_EQ("v1", Get("a"));
  ASSERT_EQ("v1", Get("b"));
  ASSERT_EQ("v1", Get("p"));
  ASSERT_EQ("v1", Get("q"));
  Put("a", "v2");
  Put("b", "v2");
  Put("p", "v2");
  Put("q", "v2");
  ASSERT_EQ("v2", Get("a"));
  ASSERT_EQ("v2", Get("b"));
  ASSERT_EQ("v2", Get("p"));
  ASSERT_EQ("v2", Get("q"));
  Reopen();
  ASSERT_EQ("v2", Get("a"));
  ASSERT_EQ("v2", Get("b"));
  ASSERT_EQ("v2", Get("p"));
  ASSERT_EQ("v2", Get("q"));
  Reopen();
  Compact();
  ASSERT_EQ("v2", Get("a"));
  ASSERT_EQ("v2", Get("b"));
  ASSERT_EQ("v2", Get("p"));
  ASSERT_EQ("v2", Get("q"));
}

TEST(BulkTest, OverlappingKeys) {
  Put("a", "v1");
  Put("p", "v1");
  int s1 = DoSnapshot();
  Flush();
  CopyDbToTmp();
  Put("a", "v2");
  Put("p", "v2");
  int s2 = DoSnapshot();
  Flush();
  BulkInsert();
  int s3 = DoSnapshot();
  ASSERT_EQ("v1", Get("a"));
  ASSERT_EQ("v1", Get("p"));
  Put("a", "v3");
  Put("p", "v3");
  int s4 = DoSnapshot();
  Flush();
  ASSERT_EQ("v3", Get("a"));
  ASSERT_EQ("v3", Get("p"));

  ASSERT_EQ("v1", Get("a", snapshots_[s1]));
  ASSERT_EQ("v1", Get("p", snapshots_[s1]));
  ASSERT_EQ("v2", Get("a", snapshots_[s2]));
  ASSERT_EQ("v2", Get("p", snapshots_[s2]));
  ASSERT_EQ("v1", Get("a", snapshots_[s3]));
  ASSERT_EQ("v1", Get("p", snapshots_[s3]));
  ASSERT_EQ("v3", Get("a", snapshots_[s4]));
  ASSERT_EQ("v3", Get("p", snapshots_[s4]));

  Reopen();
  ASSERT_EQ("v3", Get("a"));
  ASSERT_EQ("v3", Get("p"));
  Reopen();
  Compact();
  ASSERT_EQ("v3", Get("a"));
  ASSERT_EQ("v3", Get("p"));
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
