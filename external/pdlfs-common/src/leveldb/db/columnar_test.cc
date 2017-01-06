/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "columnar_impl.h"
#include "db_impl.h"

#include "pdlfs-common/cache.h"
#include "pdlfs-common/dbfiles.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {

class TestColumnSelector : public ColumnSelector {
 public:
  virtual const char* Name() const { return "leveldb.TestColumnSelector"; }

  virtual size_t Select(const Slice& k) const { return 0; }
};

class ColumnarTest {
 public:
  ColumnarTest() {
    dbname_ = test::TmpDir() + "/columnar_test";
    DestroyDB(ColumnName(dbname_, 0), Options());
    DestroyDB(dbname_, Options());
    options_.create_if_missing = true;
    options_.skip_lock_file = true;
    ColumnStyle styles[1];
    styles[0] = kLSMStyle;
    Status s =
        ColumnarDB::Open(options_, dbname_, &column_selector_, styles, 1, &db_);
    ASSERT_OK(s);
  }

  ~ColumnarTest() {
    delete db_;  //
  }

  std::string Get(const Slice& key) {
    std::string value;
    Status s = db_->Get(ReadOptions(), key, &value);
    if (!s.ok()) {
      return "NOT_FOUND";
    } else {
      return value;
    }
  }

  void CompactMemTable() {
    Status s =
        reinterpret_cast<ColumnarDBWrapper*>(db_)->TEST_CompactMemTable();
    ASSERT_OK(s);
  }

  typedef DBOptions Options;
  TestColumnSelector column_selector_;
  std::string dbname_;
  Options options_;
  DB* db_;
};

TEST(ColumnarTest, Empty) {
  // Empty
}

TEST(ColumnarTest, Put) {
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "bar", "v2"));
  ASSERT_EQ(Get("foo"), "v1");
  ASSERT_EQ(Get("bar"), "v2");

  CompactMemTable();
  ASSERT_EQ(Get("foo"), "v1");
  ASSERT_EQ(Get("bar"), "v2");
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
