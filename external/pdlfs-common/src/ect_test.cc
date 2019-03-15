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

#include <set>
#include <string>
#include <vector>

#include "pdlfs-common/ect.h"
#include "pdlfs-common/random.h"
#include "pdlfs-common/slice.h"
#include "pdlfs-common/testharness.h"

#include "spooky_hash.h"

namespace pdlfs {

class ECTTest {};

class TrieWrapper {
 private:
  const size_t k_len_;
  std::vector<size_t> k_offs_;
  std::string k_buffer_;
  size_t num_k_;
  ECT* ect_;

 public:
  TrieWrapper(size_t key_len) : k_len_(key_len), num_k_(0), ect_(NULL) {}

  ~TrieWrapper() { delete ect_; }

  size_t Locate(const Slice& key) { return ect_->Find(key); }

  size_t MemUsage() const { return ect_->MemUsage(); }

  void Insert(const Slice& key) {
    k_offs_.push_back(k_buffer_.size());
    k_buffer_.append(key.data(), key.size());
    num_k_++;
  }

  void Flush() {
    delete ect_;
    k_offs_.push_back(k_buffer_.size());
    std::vector<Slice> tmp_keys;
    tmp_keys.resize(num_k_);
    for (size_t i = 0; i < num_k_; i++) {
      tmp_keys[i] = Slice(&k_buffer_[k_offs_[i]], k_offs_[i + 1] - k_offs_[i]);
    }
    ect_ = ECT::Default(k_len_, tmp_keys.size(), &tmp_keys[0]);
    k_offs_.clear();
    k_buffer_.clear();
    num_k_ = 0;
  }
};

TEST(ECTTest, EmptyTrie) {
  TrieWrapper trie(10);
  trie.Flush();
}

TEST(ECTTest, KeyLenIs1) {
  TrieWrapper trie(1);
  trie.Insert("x");
  trie.Insert("y");
  trie.Insert("z");
  trie.Flush();
  BETWEEN(trie.Locate("x"), 0, 0);
  BETWEEN(trie.Locate("y"), 1, 1);
  BETWEEN(trie.Locate("z"), 2, 2);
}

TEST(ECTTest, KeyLenIs5) {
  TrieWrapper trie(5);
  trie.Insert("aaaaa");
  trie.Insert("fghdc");
  trie.Insert("fzhdc");
  trie.Insert("zdfgr");
  trie.Insert("zzfgr");
  trie.Insert("zzzgr");
  trie.Flush();
  BETWEEN(trie.Locate("abcde"), 0, 1);
  BETWEEN(trie.Locate("fffff"), 0, 1);
  BETWEEN(trie.Locate("fghdc"), 1, 1);
  BETWEEN(trie.Locate("fgxxx"), 1, 2);
  BETWEEN(trie.Locate("fzbbb"), 1, 2);
  BETWEEN(trie.Locate("fzhdc"), 2, 2);
  BETWEEN(trie.Locate("fzyyy"), 2, 3);
  BETWEEN(trie.Locate("zabcd"), 2, 3);
  BETWEEN(trie.Locate("zdfgr"), 3, 3);
  BETWEEN(trie.Locate("zezzz"), 3, 4);
  BETWEEN(trie.Locate("zfzzz"), 3, 4);
  BETWEEN(trie.Locate("zyzzz"), 3, 4);
  BETWEEN(trie.Locate("zzfgr"), 4, 4);
  BETWEEN(trie.Locate("zzzgr"), 5, 5);
  BETWEEN(trie.Locate("zzzxz"), 5, 6);
  BETWEEN(trie.Locate("zzzzz"), 5, 6);
}

TEST(ECTTest, KeyLenIs8) {
  TrieWrapper trie(8);
  trie.Insert("00000000");
  trie.Insert("12345678");
  trie.Insert("23456789");
  trie.Insert("34567891");
  trie.Insert("45678912");
  trie.Insert("56789123");
  trie.Insert("67891234");
  trie.Insert("78912345");
  trie.Insert("89123456");
  trie.Flush();
  BETWEEN(trie.Locate("11111111"), 0, 1);
  BETWEEN(trie.Locate("22222222"), 1, 2);
  BETWEEN(trie.Locate("33333333"), 2, 3);
  BETWEEN(trie.Locate("44444444"), 3, 4);
  BETWEEN(trie.Locate("55555555"), 4, 5);
  BETWEEN(trie.Locate("66666666"), 5, 6);
  BETWEEN(trie.Locate("77777777"), 6, 7);
  BETWEEN(trie.Locate("88888888"), 7, 8);
  BETWEEN(trie.Locate("99999999"), 8, 9);
}

#if 0
static std::string RandomKey(Random* rnd, int k_len) {
  std::string result;
  for (int i = 0; i < k_len; i++)
    result += static_cast<unsigned char>(rnd->Uniform(256));
  return result;
}
#else
static std::string RandomKey(Random* rnd, int k_len) {
  uint32_t seed = rnd->Next();
  uint64_t h1 = 301;
  uint64_t h2 = 103;
  SpookyHash::Hash128(&seed, sizeof(seed), &h1, &h2);
  std::string result;
  result.append((char*)&h1, sizeof(h1));
  result.append((char*)&h2, sizeof(h2));
  result.resize(k_len);
  return result;
}
#endif

TEST(ECTTest, ECTBench) {
  for (int k_len = 4; k_len <= 16; k_len += 4) {
    for (int num_k = 16; num_k <= 8192; num_k *= 2) {
      Random rnd(301);
      std::set<std::string> keys;
      while (keys.size() < num_k) keys.insert(RandomKey(&rnd, k_len));
      TrieWrapper trie(k_len);
      std::set<std::string>::const_iterator iter;
      for (iter = keys.begin(); iter != keys.end(); ++iter) {
        trie.Insert(*iter);
      }
      trie.Flush();
      const size_t bits = trie.MemUsage();
      fprintf(stderr, "klen=%d\t#k=%d\ttotal_bits=%zu bits\tbits_per_k=%.2f\n",
              k_len, num_k, bits, bits / static_cast<double>(num_k));
    }
  }
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
