/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <vector>

#include "ectrie/bit_vector.h"
#include "ectrie/trie.h"

#include "pdlfs-common/ect.h"

namespace pdlfs {

ECT::~ECT() {}

namespace {

class ECTCoder {
 public:
  static const ECTCoder* Get() {
    static ECTCoder singleton;
    return &singleton;
  }

  template <typename T>
  size_t Decode(const T& encoding, const uint8_t* key, size_t k_len,
                size_t num_k) const {
    size_t iter = 0;
    size_t rank = trie_.locate(encoding, iter, key, k_len, 0, num_k);
    return rank;
  }

  template <typename T>
  void Encode(T& encoding, size_t k_len, size_t num_k,
              const uint8_t** keys) const {
    trie_.encode(encoding, keys, k_len, 0, num_k);
  }

 private:
  ECTCoder() : trie_(&huffbuf_) {}

  typedef ectrie::huffman_buffer<> huffbuf_t;
  huffbuf_t huffbuf_;
  typedef ectrie::trie<> trie_t;
  trie_t trie_;
};

class ECTIndex : public ECT {
 public:
  ECTIndex(size_t k_len) : key_len_(k_len), n_(0) {}
  virtual ~ECTIndex() {}

  virtual size_t MemUsage() const { return bitvec_.size(); }

  size_t Locate(const uint8_t* key) const {
    return ECTCoder::Get()->Decode(bitvec_, key, key_len_, n_);
  }

  virtual size_t Find(const Slice& key) const {
    return Locate(reinterpret_cast<const uint8_t*>(key.data()));
  }

  virtual void InsertKeys(size_t n, const uint8_t** keys) {
    assert(n_ == 0);
    ECTCoder::Get()->Encode(bitvec_, key_len_, n, keys);
    bitvec_.compact();
    n_ = n;
  }

 private:
  typedef ectrie::bit_vector<> bitvec_t;
  bitvec_t bitvec_;
  size_t key_len_;
  size_t n_;
};

}  // anonymous namespace

void ECT::InitTrie(ECT* ect, size_t n, const Slice* keys) {
  std::vector<const uint8_t*> ukeys;
  ukeys.reserve(n);
  for (size_t i = 0; i < n; i++) {
    ukeys.push_back(reinterpret_cast<const uint8_t*>(&keys[i][0]));
  }
  ect->InsertKeys(ukeys.size(), &ukeys[0]);
}

ECT* ECT::Default(size_t key_len, size_t n, const Slice* keys) {
  ECT* ect = new ECTIndex(key_len);
  ECT::InitTrie(ect, n, keys);
  return ect;
}

}  // namespace pdlfs
