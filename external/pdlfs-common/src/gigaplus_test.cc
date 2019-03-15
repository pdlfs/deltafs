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
#include <algorithm>
#include <iostream>
#include <set>

#include "pdlfs-common/gigaplus.h"
#include "pdlfs-common/testharness.h"

namespace pdlfs {

class DirIndexTest {
 public:
  enum { kNumRadix = 14, kNumServers = 1 << kNumRadix };

  DirIndexTest() {
    zeroth_server_ = 253;
    options_.num_servers = kNumServers;
    options_.num_virtual_servers = kNumServers;
    idx_ = NewIndex();
  }

  ~DirIndexTest() {
    idx_->TEST_RevertAll();
    ASSERT_EQ(idx_->Radix(), 0);
    delete idx_;
  }

  static bool Migrate(int index, const char* hash) {
    return DirIndex::ToBeMigrated(index, hash);
  }

  DirIndex* NewIndex() { return new DirIndex(zeroth_server_, &options_); }

  DirIndex* Recover() {
    DirIndex* result = NewIndex();
    int b = result->Update(idx_->Encode());
    ASSERT_TRUE(b);
    return result;
  }

  void Reset() {
    delete idx_;
    idx_ = NewIndex();
  }

  int zeroth_server_;
  DirIndexOptions options_;
  DirIndex* idx_;
};

// -------------------------------------------------------------
// Test Cases
// -------------------------------------------------------------

TEST(DirIndexTest, Empty) {
  ASSERT_EQ(idx_->Radix(), 0);
  ASSERT_EQ(idx_->ZerothServer(), zeroth_server_);
  ASSERT_TRUE(idx_->IsSet(0));
}

TEST(DirIndexTest, ScaleUp1) {
  int b[kNumRadix + 1] = {0};
  b[0x1] = 1;
  b[0x2] = 2;
  b[0x3] = 4;
  b[0x4] = 8;
  b[0x5] = 16;
  b[0x6] = 32;
  b[0x7] = 64;
  b[0x8] = 128;
  b[0x9] = 256;
  b[0xa] = 512;
  b[0xb] = 1024;
  b[0xc] = 2048;
  b[0xd] = 4096;
  b[0xe] = 8192;

  for (int r = 1; r <= kNumRadix; r++) {
    idx_->Set(b[r]);
    ASSERT_TRUE(idx_->IsSet(b[r]));
    ASSERT_EQ(idx_->Radix(), r);
  }
}

TEST(DirIndexTest, ScaleUp2) {
  int b[kNumRadix + 1] = {0};
  b[0x1] = 1;
  b[0x2] = 3;
  b[0x3] = 7;
  b[0x4] = 15;
  b[0x5] = 31;
  b[0x6] = 63;
  b[0x7] = 127;
  b[0x8] = 255;
  b[0x9] = 511;
  b[0xa] = 1023;
  b[0xb] = 2047;
  b[0xc] = 4095;
  b[0xd] = 8191;
  b[0xe] = 16383;

  for (int r = 1; r <= kNumRadix; r++) {
    idx_->Set(b[r]);
    ASSERT_TRUE(idx_->IsSet(b[r]));
    ASSERT_EQ(idx_->Radix(), r);
  }
}

TEST(DirIndexTest, Merge1) {
  idx_->Set(1);
  idx_->Set(2);
  DirIndex* another = NewIndex();
  another->Set(1);
  another->Set(3);
  idx_->Update(another->Encode());
  ASSERT_TRUE(idx_->IsSet(0));
  ASSERT_TRUE(idx_->IsSet(1));
  ASSERT_TRUE(idx_->IsSet(2));
  ASSERT_TRUE(idx_->IsSet(3));
  ASSERT_EQ(idx_->Radix(), 2);
  ASSERT_EQ(idx_->ZerothServer(), zeroth_server_);
  delete another;
}

TEST(DirIndexTest, Merge2) {
  idx_->Set(1);
  idx_->Set(2);
  idx_->Set(4);
  idx_->Set(8);
  idx_->Set(16);
  idx_->Set(32);
  idx_->Set(64);
  idx_->Set(128);
  DirIndex* another = NewIndex();
  another->Set(1);
  another->Set(3);
  another->Set(7);
  another->Set(15);
  another->Set(31);
  another->Set(63);
  another->Set(127);
  another->Set(255);
  another->Set(511);
  idx_->Update(another->Encode());
  ASSERT_TRUE(idx_->IsSet(0));
  ASSERT_TRUE(idx_->IsSet(1));
  ASSERT_TRUE(idx_->IsSet(3));
  ASSERT_TRUE(idx_->IsSet(7));
  ASSERT_TRUE(idx_->IsSet(15));
  ASSERT_TRUE(idx_->IsSet(31));
  ASSERT_TRUE(idx_->IsSet(63));
  ASSERT_TRUE(idx_->IsSet(127));
  ASSERT_TRUE(idx_->IsSet(255));
  ASSERT_TRUE(idx_->IsSet(511));
  ASSERT_TRUE(idx_->IsSet(2));
  ASSERT_TRUE(idx_->IsSet(4));
  ASSERT_TRUE(idx_->IsSet(8));
  ASSERT_TRUE(idx_->IsSet(16));
  ASSERT_TRUE(idx_->IsSet(32));
  ASSERT_TRUE(idx_->IsSet(64));
  ASSERT_TRUE(idx_->IsSet(128));
  ASSERT_EQ(idx_->Radix(), 9);
  ASSERT_EQ(idx_->ZerothServer(), zeroth_server_);
  delete another;
}

TEST(DirIndexTest, Recover1) {
  idx_->Set(1);
  idx_->Set(3);
  idx_->Set(7);
  DirIndex* recovered = Recover();
  ASSERT_TRUE(recovered->IsSet(1));
  ASSERT_TRUE(recovered->IsSet(3));
  ASSERT_TRUE(recovered->IsSet(7));
  ASSERT_EQ(recovered->Radix(), 3);
  ASSERT_EQ(recovered->ZerothServer(), zeroth_server_);
  delete recovered;
}

TEST(DirIndexTest, Recover2) {
  idx_->Set(1);
  idx_->Set(3);
  idx_->Set(7);
  idx_->Set(15);
  idx_->Set(31);
  idx_->Set(63);
  idx_->Set(127);
  idx_->Set(255);
  idx_->Set(511);
  DirIndex* recovered = Recover();
  ASSERT_TRUE(recovered->IsSet(1));
  ASSERT_TRUE(recovered->IsSet(3));
  ASSERT_TRUE(recovered->IsSet(7));
  ASSERT_TRUE(recovered->IsSet(15));
  ASSERT_TRUE(recovered->IsSet(31));
  ASSERT_TRUE(recovered->IsSet(63));
  ASSERT_TRUE(recovered->IsSet(127));
  ASSERT_TRUE(recovered->IsSet(255));
  ASSERT_TRUE(recovered->IsSet(511));
  ASSERT_EQ(recovered->Radix(), 9);
  ASSERT_EQ(recovered->ZerothServer(), zeroth_server_);
  delete recovered;
}

static std::string File(int i) {
  char buf[100];
  snprintf(buf, 100, "file%d", i);
  return buf;
}

static void PrintHash(const char* hash) {
  fprintf(stderr, "Hash: %02X-%02X-%02X-%02X-%02X-%02X-%02X-%02X (8 bytes)\n",
          (unsigned char)hash[0], (unsigned char)hash[1],
          (unsigned char)hash[2], (unsigned char)hash[3],
          (unsigned char)hash[4], (unsigned char)hash[5],
          (unsigned char)hash[6], (unsigned char)hash[7]);
}

TEST(DirIndexTest, Hash) {
  char hash0[40];
  char hash1[40];
  char hash2[40];

  DirIndex::Hash("", hash0);
  DirIndex::Hash(File(1), hash1);
  DirIndex::Hash(File(2), hash2);

  PrintHash(hash0);
  PrintHash(hash1);
  PrintHash(hash2);

  ASSERT_TRUE(memcmp(hash0, "\0\0\0\0\0\0\0\0", 8) == 0);
  ASSERT_TRUE(memcmp(hash1, hash2, 8) != 0);

  char hash[40];
  std::set<std::string> set;
  for (int i = 0; i < 1000000; i++) {
    DirIndex::Hash(File(i), hash);
    set.insert(std::string(hash, 8));
  }

  PrintHash(set.begin()->data());
  PrintHash(set.rbegin()->data());

  ASSERT_EQ(1000000, set.size());
}

TEST(DirIndexTest, Split1) {
  int b[kNumRadix + 1] = {0};
  b[0x1] = 1;
  b[0x2] = 2;
  b[0x3] = 4;
  b[0x4] = 8;
  b[0x5] = 16;
  b[0x6] = 32;
  b[0x7] = 64;
  b[0x8] = 128;
  b[0x9] = 256;
  b[0xa] = 512;
  b[0xb] = 1024;
  b[0xc] = 2048;
  b[0xd] = 4096;
  b[0xe] = 8192;

  for (int r = 1; r <= kNumRadix; r++) {
    ASSERT_TRUE(!idx_->IsSet(b[r]));
    ASSERT_TRUE(idx_->IsSplittable(b[0]));
    ASSERT_TRUE(idx_->NewIndexForSplitting(b[0]) == b[r]);
    idx_->Set(b[r]);
  }

  ASSERT_TRUE(!idx_->IsSplittable(b[0]));
}

TEST(DirIndexTest, Split2) {
  int b[kNumRadix + 1] = {0};
  b[0x1] = 1;
  b[0x2] = 3;
  b[0x3] = 7;
  b[0x4] = 15;
  b[0x5] = 31;
  b[0x6] = 63;
  b[0x7] = 127;
  b[0x8] = 255;
  b[0x9] = 511;
  b[0xa] = 1023;
  b[0xb] = 2047;
  b[0xc] = 4095;
  b[0xd] = 8191;
  b[0xe] = 16383;

  for (int r = 1; r <= kNumRadix; r++) {
    ASSERT_TRUE(!idx_->IsSet(b[r]));
    ASSERT_TRUE(idx_->IsSplittable(b[r - 1]));
    ASSERT_TRUE(idx_->NewIndexForSplitting(b[r - 1]) == b[r]);
    idx_->Set(b[r]);
  }

  ASSERT_TRUE(!idx_->IsSplittable(b[kNumRadix]));
}

static int Sum(const int* array, int size) {
  int sum = 0;
  for (int i = 0; i < size; ++i) {
    sum += array[i];
  }
  return sum;
}

static void Print(const int* array, int size) {
  fprintf(stderr, "[ ");
  for (int i = 0; i < size; i++) {
    if (array[i] != 0) {
      fprintf(stderr, "%d:%d ", i, array[i]);
    }
  }
  fprintf(stderr, "]\n");
}

TEST(DirIndexTest, SelectServer1) {
  int info[kNumServers] = {0};
  for (int i = 0; i < 10000; i++) {
    int s = idx_->SelectServer(File(i));
    info[s]++;
  }

  ASSERT_TRUE(Sum(info, kNumServers) == 10000);

  Print(info, kNumServers);
}

TEST(DirIndexTest, SelectServer2) {
  idx_->Set(0);
  idx_->Set(1);
  int info[kNumServers] = {0};
  for (int i = 0; i < 10000; i++) {
    int s = idx_->SelectServer(File(i));
    info[s]++;
  }

  ASSERT_TRUE(Sum(info, kNumServers) == 10000);

  Print(info, kNumServers);
}

TEST(DirIndexTest, SelectServer3) {
  idx_->Set(0);
  idx_->Set(1);
  idx_->Set(2);
  int info[kNumServers] = {0};
  for (int i = 0; i < 10000; i++) {
    int s = idx_->SelectServer(File(i));
    info[s]++;
  }

  ASSERT_TRUE(Sum(info, kNumServers) == 10000);

  ASSERT_TRUE(info[3] == 0);

  Print(info, kNumServers);
}

TEST(DirIndexTest, SelectServer4) {
  idx_->Set(0);
  idx_->Set(1);
  idx_->Set(2);
  idx_->Set(4);
  int info[kNumServers] = {0};
  for (int i = 0; i < 10000; i++) {
    int s = idx_->SelectServer(File(i));
    info[s]++;
  }

  ASSERT_TRUE(Sum(info, kNumServers) == 10000);

  ASSERT_TRUE(info[3] == 0);
  ASSERT_TRUE(info[5] == 0);
  ASSERT_TRUE(info[6] == 0);
  ASSERT_TRUE(info[7] == 0);

  Print(info, kNumServers);
}

TEST(DirIndexTest, Migration1) {
  int index = 0;
  int moved = 0;
  for (int i = 0; i < 10000; i++) {
    char hash[40];
    DirIndex::Hash(File(i), hash);
    if (Migrate(index, hash)) {
      moved++;
    }
  }

  ASSERT_TRUE(moved == 10000);
}

TEST(DirIndexTest, Migration2) {
  int index = 1;
  int moved = 0;
  for (int i = 0; i < 10000; i++) {
    char hash[40];
    DirIndex::Hash(File(i), hash);
    if (Migrate(index, hash)) {
      moved++;
    }
  }

  fprintf(stderr, "%d/%d files migrated\n", moved, 10000);

  ASSERT_TRUE(moved > 0 && moved < 10000);
}

TEST(DirIndexTest, Migration3) {
  int child = 2;
  int parent = 1;
  int moved = 0;
  int original = 0;
  for (int i = 0; i < 10000; i++) {
    char hash[40];
    DirIndex::Hash(File(i), hash);
    if (Migrate(parent, hash)) {
      original++;
    }
    if (Migrate(child, hash)) {
      moved++;
    }
  }

  fprintf(stderr, "%d/%d files migrated\n", moved, original);

  ASSERT_TRUE(moved > 0 && moved < 10000);
}

TEST(DirIndexTest, Migration4) {
  const int child = 4;
  const int parent = 2;
  const int uncle = 1;
  int moved = 0;
  int original = 0;
  for (int i = 0; i < 10000; i++) {
    char hash[40];
    DirIndex::Hash(File(i), hash);
    if (!Migrate(uncle, hash) && Migrate(parent, hash)) {
      original++;
    }
    if (Migrate(child, hash)) {
      moved++;
    }
  }

  fprintf(stderr, "%d/%d files migrated\n", moved, original);

  ASSERT_TRUE(moved > 0 && moved < 10000);
}

static void PrintStates(const std::vector<int>& states) {
  static int run = 0;
  fprintf(stderr, "case %02d: ", ++run);
  fprintf(stderr, "num_server: %4d\t", int(states.size()));
  int max = *std::max_element(states.begin(), states.end());
  fprintf(stderr, "max: %d\t", max);
  int min = *std::min_element(states.begin(), states.end());
  fprintf(stderr, "min: %d\t", min);
  fprintf(stderr, "max-min diff: %.2f%%\n",
          (double(max) / double(min) - 1) * 100);
#if 0
  Random rnd(301);
  fprintf(stderr, "samples: [");
  for (int i = 0; i < 8; i++) {
    int a = rnd.Uniform(states.size());
    fprintf(stderr, " %d:%d ", a, states[a]);
  }
  fprintf(stderr, "]\n");
#endif
}

TEST(DirIndexTest, RandomServer1) {
  for (size_t num_servers = 2; num_servers <= 4096; num_servers *= 2) {
    std::vector<int> states(num_servers);
    int num_dirs = 10 * 1000 * 1000;
    for (int i = 0; i < num_dirs; i++) {
      states[DirIndex::RandomServer(File(i), 0) % num_servers]++;
    }
    PrintStates(states);
  }
}

class Client {
 public:
  int PickupServer(const std::string& dir) {
    std::pair<int, int> r = DirIndex::RandomServers(dir, 0);
    int s1 = r.first % states_.size();
    int s2 = r.second % states_.size();
    if (states_[s1] < states_[s2]) {
      states_[s1]++;
      return s1;
    } else {
      states_[s2]++;
      return s2;
    }
  }
  Client(size_t num_servers) : states_(num_servers) {}
  std::vector<int> states_;
};

TEST(DirIndexTest, RandomServer2) {
  for (size_t num_servers = 2; num_servers <= 4096; num_servers *= 2) {
    std::vector<int> states(num_servers);
    Client client(num_servers);
    int num_dirs = 10 * 1000 * 1000;
    for (int i = 0; i < num_dirs; i++) {
      states[client.PickupServer(File(i))]++;
    }
    PrintStates(states);
  }
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
