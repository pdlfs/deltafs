/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "ectindex.h"

#include "ectrie/bit_vector.h"
#include "ectrie/trie.h"
#include "ectrie/twolevel_bucketing.h"

#include <vector>

namespace pdlfs {

ECT::~ECT() {}

namespace {

class EntropyCodedTrie : public ECT {
 private:
  typedef ectrie::bit_vector<> bitvec_t;
  bitvec_t bitvector_;
  typedef ectrie::trie<> trie_t;
  trie_t trie_;
  const size_t key_len_;
  size_t n_;

 public:
  EntropyCodedTrie(size_t l) : key_len_(l), n_(0) {}
  virtual ~EntropyCodedTrie() {}

  virtual size_t MemUsage() const { return bitvector_.size(); }

  virtual size_t Locate(const uint8_t* key) const {
    size_t iter = 0;
    size_t rank = trie_.locate(bitvector_, iter, key, key_len_, 0, n_);
    return rank;
  }

  virtual size_t Find(const Slice& key) const {
    return Locate(reinterpret_cast<const uint8_t*>(key.data()));
  }

  virtual void InsertKeys(size_t n, const uint8_t** keys) {
    assert(n_ == 0);
    trie_.encode(bitvector_, keys, key_len_, 0, n);
    bitvector_.compact();
    n_ = n;
  }
};

class TwoLevelBucketingTrie : public ECT {
 private:
  static const size_t kSkipBits = 0;
  static const size_t kDestBase = 0;
  static const size_t kDestKeysPerBlock = 1;

  struct Builder {
    std::vector<const uint8_t*> pending_keys_;
    size_t last_dest_offset_;
    size_t bucket_count_;
    size_t pending_key_count_;
  };

  typedef ectrie::bit_vector<> bitvec_t;
  bitvec_t bitvector_;
  typedef ectrie::trie<> trie_t;
  trie_t trie_;
  typedef ectrie::twolevel_bucketing<> bucket_t;
  bucket_t bucketing_;
  const size_t default_bucket_size_;
  const size_t key_len_;

  // Initialized lazily
  size_t bucket_count_;
  size_t bucket_bits_;
  size_t bucket_size_;
  size_t n_;

 public:
  TwoLevelBucketingTrie(size_t key_len, size_t bucket_size)
      : default_bucket_size_(bucket_size), key_len_(key_len), n_(0) {}
  virtual ~TwoLevelBucketingTrie() {}

  virtual size_t MemUsage() const {
    return bitvector_.size() + bucketing_.bit_size();
  }

  virtual size_t Locate(const uint8_t* key) const {
    size_t b = FindBucket(key);

    size_t iter = bucketing_.index_offset(b);
    size_t off = bucketing_.dest_offset(b);
    size_t n = bucketing_.dest_offset(b + 1) - off;

    size_t index =
        off +
        trie_.locate(bitvector_, iter, key, key_len_, 0, n, kDestBase + off,
                     kDestKeysPerBlock, kSkipBits + bucket_bits_);

    return index;
  }

  virtual size_t Find(const Slice& key) const {
    return Locate(reinterpret_cast<const uint8_t*>(key.data()));
  }

  virtual void InsertKeys(size_t n, const uint8_t** keys) {
    assert(n_ == 0);
    size_t bucket_size = default_bucket_size_;
    bucket_bits_ = 0;
    while ((static_cast<size_t>(1) << bucket_bits_) < (n / bucket_size)) {
      bucket_bits_++;
    }
    bucket_count_ = static_cast<size_t>(1) << bucket_bits_;
    bucket_size_ = n / bucket_count_;
    if (bucket_size_ == 0) {
      bucket_size_ = 1;
    }

    bucketing_.resize(bucket_count_ + 1, bucket_size_, kDestKeysPerBlock);
    bucketing_.insert(0, 0);

    Builder bu;
    bu.last_dest_offset_ = 0;
    bu.bucket_count_ = 0;
    bu.pending_key_count_ = 0;
    for (size_t i = 0; i < n; i++) AppendKey(keys[i], bu);
    while (bu.bucket_count_ < bucket_count_) {
      Flush(bu);
    }

    bucketing_.finalize();
    bitvector_.compact();
    n_ = n;
  }

 private:
  void AppendKey(const uint8_t* key, Builder& bu) {
    assert(bu.bucket_count_ < bucket_count_);
    size_t b = FindBucket(key);
    assert(b >= bu.bucket_count_);
    while (bu.bucket_count_ < b) {
      Flush(bu);
    }

    bu.pending_key_count_++;
    bu.pending_keys_.push_back(key);
  }

  void Flush(Builder& bu) {
    assert(bu.bucket_count_ < bucket_count_);
    trie_.encode(bitvector_, bu.pending_keys_, key_len_, 0,
                 bu.pending_key_count_, kDestBase + bu.last_dest_offset_,
                 kDestKeysPerBlock, kSkipBits + bucket_bits_);

    bu.bucket_count_ += 1;
    bucketing_.insert(bitvector_.size(),
                      bu.last_dest_offset_ + bu.pending_key_count_);

    bu.last_dest_offset_ += bu.pending_key_count_;
    bu.pending_keys_.clear();
    bu.pending_key_count_ = 0;
  }

  size_t FindBucket(const uint8_t* key) const {
    size_t bits = 0;
    size_t index = 0;
    while (bits < bucket_bits_) {
      index <<= 1;
      if (ectrie::bit_access::get(key, kSkipBits + bits)) {
        index |= 1;
      }
      bits++;
    }
    return index;
  }
};

}  // anonymous namespace

void ECT::InitTrie(ECT* ect, size_t n, const Slice* keys) {
  std::vector<const uint8_t*> ukeys;
  ukeys.resize(n);
  for (size_t i = 0; i < n; i++) {
    ukeys[i] = reinterpret_cast<const uint8_t*>(&keys[i][0]);
  }
  ect->InsertKeys(ukeys.size(), &ukeys[0]);
}

ECT* ECT::Default(size_t key_len, size_t n, const Slice* keys) {
  ECT* ect = new EntropyCodedTrie(key_len);
  ECT::InitTrie(ect, n, keys);
  return ect;
}

ECT* ECT::TwoLevelBucketing(size_t key_len, size_t n, const Slice* keys,
                            size_t bucket_size) {
  ECT* ect = new TwoLevelBucketingTrie(key_len, bucket_size);
  ECT::InitTrie(ect, n, keys);
  return ect;
}

}  // namespace pdlfs
