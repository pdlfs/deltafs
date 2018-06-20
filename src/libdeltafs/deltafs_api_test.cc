/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs/deltafs_api.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

#include <fcntl.h>

#include <string>
#include <vector>

namespace pdlfs {
class PlfsDirTest {
 public:
  PlfsDirTest() {
    dirname_ = test::TmpDir() + "/plfsdir_test";
    wdir_ = rdir_ = NULL;
    epoch_ = 0;
  }

  ~PlfsDirTest() {
    if (wdir_ != NULL) {
      deltafs_plfsdir_free_handle(wdir_);
    }
    if (rdir_ != NULL) {
      deltafs_plfsdir_free_handle(rdir_);
    }
  }

  void OpenWriter() {
    wdir_ =
        deltafs_plfsdir_create_handle("", O_WRONLY, DELTAFS_PLFSDIR_DEFAULT);
    ASSERT_TRUE(wdir_ != NULL);
    deltafs_plfsdir_set_unordered(wdir_, 0);
    deltafs_plfsdir_force_leveldb_fmt(wdir_, 0);
    deltafs_plfsdir_set_fixed_kv(wdir_, 0);
    deltafs_plfsdir_set_side_io_buf_size(wdir_, 4096);
    deltafs_plfsdir_enable_side_io(wdir_, 1);
    deltafs_plfsdir_destroy(wdir_, dirname_.c_str());
    int r = deltafs_plfsdir_open(wdir_, dirname_.c_str());
    ASSERT_TRUE(r == 0);
  }

  void Finish() {
    int r = deltafs_plfsdir_finish(wdir_);
    ASSERT_TRUE(r == 0);
    deltafs_plfsdir_free_handle(wdir_);
    wdir_ = NULL;
  }

  void OpenReader() {
    rdir_ =
        deltafs_plfsdir_create_handle("", O_RDONLY, DELTAFS_PLFSDIR_DEFAULT);
    ASSERT_TRUE(rdir_ != NULL);
    deltafs_plfsdir_enable_side_io(rdir_, 1);
    int r = deltafs_plfsdir_open(rdir_, dirname_.c_str());
    ASSERT_TRUE(r == 0);
  }

  void Put(const Slice& k, const Slice& v) {
    if (wdir_ == NULL) OpenWriter();
    ssize_t r = deltafs_plfsdir_put(wdir_, k.data(), k.size(), epoch_, v.data(),
                                    v.size());
    ASSERT_TRUE(r == v.size());
  }

  void IoWrite(const Slice& d) {
    if (wdir_ == NULL) OpenWriter();
    ssize_t r = deltafs_plfsdir_io_append(wdir_, d.data(), d.size());
    ASSERT_TRUE(r == d.size());
  }

  void FinishEpoch() {
    if (wdir_ == NULL) OpenWriter();
    int r = deltafs_plfsdir_epoch_flush(wdir_, epoch_);
    ASSERT_TRUE(r == 0);
    r = deltafs_plfsdir_io_flush(wdir_);
    ASSERT_TRUE(r == 0);
    epoch_++;
  }

  std::string Get(const Slice& k) {
    if (wdir_ != NULL) Finish();
    if (rdir_ == NULL) OpenReader();
    size_t sz = 0;
    char* result =
        deltafs_plfsdir_get(rdir_, k.data(), k.size(), -1, &sz, NULL, NULL);
    ASSERT_TRUE(result != NULL);
    std::string tmp = Slice(result, sz).ToString();
    free(result);
    return tmp;
  }

  std::string IoRead(uint64_t off, size_t sz) {
    if (wdir_ != NULL) Finish();
    if (rdir_ == NULL) OpenReader();
    char* result = static_cast<char*>(malloc(sz));
    ssize_t r = deltafs_plfsdir_io_pread(rdir_, result, sz, off);
    ASSERT_TRUE(r >= 0);
    std::string tmp = Slice(result, r).ToString();
    free(result);
    return tmp;
  }

  std::string dirname_;
  std::vector<std::string> options_;
  deltafs_plfsdir_t* wdir_;
  deltafs_plfsdir_t* rdir_;
  int epoch_;
};

TEST(PlfsDirTest, Empty) {
  FinishEpoch();
  std::string val = Get("non-exists");
  ASSERT_TRUE(val.empty());
}

TEST(PlfsDirTest, SingleEpoch) {
  Put("k1", "v1");
  IoWrite("a");
  Put("k2", "v2");
  IoWrite("b");
  Put("k3", "v3");
  IoWrite("c");
  Put("k4", "v4");
  IoWrite("x");
  Put("k5", "v5");
  IoWrite("y");
  Put("k6", "v6");
  IoWrite("z");
  FinishEpoch();
  ASSERT_EQ(IoRead(0, 6), "abcxyz");
  ASSERT_EQ(Get("k1"), "v1");
  ASSERT_EQ(Get("k2"), "v2");
  ASSERT_EQ(Get("k3"), "v3");
  ASSERT_EQ(Get("k4"), "v4");
  ASSERT_EQ(Get("k5"), "v5");
  ASSERT_EQ(Get("k6"), "v6");
}

}  // namespace pdlfs

int main(int argc, char* argv[]) {
  pdlfs::Slice token;
  if (argc > 1) {
    token = pdlfs::Slice(argv[argc - 1]);
  }
  if (!token.starts_with("--bench")) {
    return pdlfs::test::RunAllTests(&argc, &argv);
  } else {
    return 0;
  }
}
