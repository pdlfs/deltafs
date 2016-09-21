/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <string>
#include <vector>

#include "pdlfs-common/random.h"
#include "pdlfs-common/slice.h"
#include "pdlfs-common/testharness.h"

#include "ectindex.h"

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

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
