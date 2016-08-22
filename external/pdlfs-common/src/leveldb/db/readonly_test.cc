/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "db_impl.h"
#include "readonly_impl.h"

#include "pdlfs-common/cache.h"
#include "pdlfs-common/leveldb/db/readonly.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {

class ReadonlyTest {
 public:
  std::string dbname_;
  Cache* blk_cache_;
  DBOptions options_;

  DBImpl* dbfull(DB* db) { return reinterpret_cast<DBImpl*>(db); }

  ReadonlyTest() {
    dbname_ = test::NewTmpDirectory("readonly_test");
    blk_cache_ = NewLRUCache(0);
    options_.block_cache = blk_cache_;
    options_.create_if_missing = true;
    options_.write_buffer_size = 512 * 1024;
    DestroyDB(dbname_, DBOptions());
  }

  ~ReadonlyTest() { delete blk_cache_; }

  void BuildImage(DB* db, int start, int end) {
    std::string key_space, value_space;
    WriteBatch batch;
    for (int i = start; i < end; i++) {
      Slice key = Key(i, &key_space);
      batch.Clear();
      batch.Put(key, Value(i, &value_space));
      ASSERT_OK(db->Write(WriteOptions(), &batch));
    }
  }

  void Check(DB* db, int min_expected, int max_expected) {
    uint64_t next_expected = 0;
    int missed = 0;
    int bad_keys = 0;
    int bad_values = 0;
    int correct = 0;
    std::string value_space;
    Iterator* iter = db->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      uint64_t key;
      Slice in(iter->key());
      if (!ConsumeDecimalNumber(&in, &key) || !in.empty() ||
          key < next_expected) {
        bad_keys++;
        continue;
      }
      missed += (key - next_expected);
      next_expected = key + 1;
      if (iter->value() != Value(key, &value_space)) {
        bad_values++;
      } else {
        correct++;
      }
    }
    delete iter;

    fprintf(stderr,
            "expected=%d..%d; got=%d; bad_keys=%d; bad_values=%d; missed=%d\n",
            min_expected, max_expected, correct, bad_keys, bad_values, missed);
    ASSERT_LE(min_expected, correct);
    ASSERT_GE(max_expected, correct);
  }

  Slice Key(int i, std::string* storage) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%016d", i);
    storage->assign(buf, strlen(buf));
    return Slice(*storage);
  }

  Slice Value(int k, std::string* storage) {
    Random r(1 + 2 * k);
    return test::RandomString(&r, 200, storage);
  }
};

TEST(ReadonlyTest, Readonly) {
  Status s;
  DB* db;
  s = DB::Open(options_, dbname_, &db);
  ASSERT_OK(s);
  BuildImage(db, 0, 10000);
  dbfull(db)->TEST_CompactMemTable();
  delete db;
  db = NULL;
  s = ReadonlyDB::Open(options_, dbname_, &db);
  ASSERT_OK(s);
  Check(db, 10000, 10000);
  delete db;
  db = NULL;
  s = DB::Open(options_, dbname_, &db);
  ASSERT_OK(s);
  BuildImage(db, 10000, 20000);
  dbfull(db)->TEST_CompactMemTable();
  delete db;
  db = NULL;
  s = ReadonlyDB::Open(options_, dbname_, &db);
  ASSERT_OK(s);
  Check(db, 20000, 20000);
  delete db;
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
