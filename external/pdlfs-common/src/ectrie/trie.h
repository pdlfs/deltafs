#pragma once

/*
 * Copyright (c) 2015 The SILT Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>

#include "bit_access.h"
#include "exp_golomb.h"
#include "huffman.h"
#include "sign_interleave.h"

namespace pdlfs {
namespace ectrie {

template <typename RefType = uint8_t>
class trie {
 public:
  explicit trie() {
    if (kHuffmanEncoding) {
      for (unsigned int n = 2; n <= kHuffmanCodingLimit; n++) {
        if (!kWeakOrdering) {
          huffman_tree_generator<uint64_t> gen(n + 1);

          uint64_t v = 1;
          gen[0] = v;
          for (unsigned int k = 1; k <= n; k++)
            gen[k] = v = v * (n - k + 1) / k;

          huffman_tree<RefType> t(n + 1);
          gen.generate(t);

          huff_[n - 2] = new huffman<RefType>(t);
        } else {
          huffman_tree_generator<uint64_t> gen(n);

          uint64_t v = 1;
          gen[0] = v * 2;
          for (unsigned int k = 1; k <= n - 1; k++)
            gen[k] = v = v * (n - k + 1) / k;

          huffman_tree<RefType> t(n);
          gen.generate(t);

          huff_[n - 2] = new huffman<RefType>(t);
        }
      }
    }
  }

#if 0
  template <typename DistType>
  void recreate_huffman_from_dist(DistType& dist) {
    assert(!kWeakOrdering);

    for (unsigned int n = 2; n <= kHuffmanCodingLimit; n++) delete huff_[n - 2];
    for (unsigned int n = 2; n <= kHuffmanCodingLimit; n++) {
      huffman_tree_generator<uint64_t> gen(n + 1);
      for (unsigned int k = 0; k <= n; k++) gen[k] = dist[n][k];
      huffman_tree<RefType> t(n + 1);
      gen.generate(t);

      huff_[n - 2] = new huffman<RefType>(t);
    }
  }
#endif

  virtual ~trie() {
    if (kHuffmanEncoding) {
      for (unsigned int n = 2; n <= kHuffmanCodingLimit; n++) {
        delete huff_[n - 2];
        huff_[n - 2] = NULL;
      }
    }
  }

  template <typename Buffer, typename KeyArrayType>
  void encode(Buffer& out_buf, const KeyArrayType& arr, size_t key_len,
              size_t off, size_t n, size_t dest_base = 0,
              size_t dest_keys_per_block = 1, size_t skip_bits = 0) const {
    encode_rec(out_buf, arr, key_len, off, n, dest_base, dest_keys_per_block,
               skip_bits);
  }

  template <typename Buffer>
  size_t locate(const Buffer& in_buf, size_t& in_out_buf_iter,
                const uint8_t* key, size_t key_len, size_t off, size_t n,
                size_t dest_base = 0, size_t dest_keys_per_block = 1,
                size_t skip_bits = 0) const {
    return locate_rec(in_buf, in_out_buf_iter, key, key_len, off, n, dest_base,
                      dest_keys_per_block, skip_bits);
  }

 private:
  template <typename Buffer, typename KeyArrayType>
  void encode_rec(Buffer& out_buf, const KeyArrayType& arr, size_t key_len,
                  size_t off, size_t n, size_t dest_base,
                  size_t dest_keys_per_block, size_t depth) const {
    // do not encode 0- or 1-sized trees
    if (n <= 1) return;

    // k-perfect hashing
    if (n <= dest_keys_per_block &&
        (dest_base + off) / dest_keys_per_block ==
            (dest_base + off + n - 1) / dest_keys_per_block)
      return;

    assert(depth < key_len * 8);  // duplicate key?

    // find the number of keys on the left tree
    size_t left = 0;
    for (; left < n; left++) {
      if (bit_access::get(arr[off + left], depth))  // assume sorted keys
        break;
    }

    // replace (n, 0) split with (0, n) split if weak ordering is used
    if (kWeakOrdering && left == n) left = 0;

    // encode the left tree size
    if (kHuffmanEncoding && n <= kHuffmanCodingLimit) {
      huff_[n - 2]->encode(out_buf, left);
    } else {
      exp_golomb<>::encode<size_t>(
          out_buf, sign_interleave::encode<size_t>(left - n / 2));
    }

    encode_rec(out_buf, arr, key_len, off, left, dest_base, dest_keys_per_block,
               depth + 1);
    encode_rec(out_buf, arr, key_len, off + left, n - left, dest_base,
               dest_keys_per_block, depth + 1);
  }

  template <typename Buffer>
  size_t locate_rec(const Buffer& in_buf, size_t& in_out_buf_iter,
                    const uint8_t* key, size_t key_len, size_t off, size_t n,
                    size_t dest_base, size_t dest_keys_per_block,
                    size_t depth) const {
    // do not encode 0- or 1-sized trees
    if (n <= 1) return 0;

    // k-perfect hashing
    if (n <= dest_keys_per_block &&
        (dest_base + off) / dest_keys_per_block ==
            (dest_base + off + n - 1) / dest_keys_per_block)
      return 0;

    assert(depth < key_len * 8);  // invalid code?

    // decode the left tree size
    size_t left;
    if (kHuffmanEncoding && n <= kHuffmanCodingLimit) {
      left = huff_[n - 2]->decode(in_buf, in_out_buf_iter);
    } else {
      left = sign_interleave::decode<size_t>(
                 exp_golomb<>::decode<size_t>(in_buf, in_out_buf_iter)) +
             n / 2;
    }

    assert(left <= n);

    // find the number of keys on the left to the key (considering weak
    // ordering)
    if (!bit_access::get(key, depth) &&
        (!kWeakOrdering || (kWeakOrdering && left != 0))) {
      return locate_rec(in_buf, in_out_buf_iter, key, key_len, off, left,
                        dest_base, dest_keys_per_block, depth + 1);
    } else {
      skip_rec(in_buf, in_out_buf_iter, key, key_len, off, left, dest_base,
               dest_keys_per_block, depth + 1);
      return left + locate_rec(in_buf, in_out_buf_iter, key, key_len,
                               off + left, n - left, dest_base,
                               dest_keys_per_block, depth + 1);
    }
  }

  template <typename Buffer>
  void skip_rec(const Buffer& in_buf, size_t& in_out_buf_iter,
                const uint8_t* key, size_t key_len, size_t off, size_t n,
                size_t dest_base, size_t dest_keys_per_block,
                size_t depth) const {
    // do not encode 0- or 1-sized trees
    if (n <= 1) return;

    // k-perfect hashing
    if (n <= dest_keys_per_block &&
        (dest_base + off) / dest_keys_per_block ==
            (dest_base + off + n - 1) / dest_keys_per_block)
      return;

    // decode the left tree size
    size_t left;
    if (kHuffmanEncoding && n <= kHuffmanCodingLimit) {
      left = huff_[n - 2]->decode(in_buf, in_out_buf_iter);
    } else {
      left = sign_interleave::decode<size_t>(
                 exp_golomb<>::decode<size_t>(in_buf, in_out_buf_iter)) +
             n / 2;
    }

    assert(left <= n);

    skip_rec(in_buf, in_out_buf_iter, key, key_len, off, left, dest_base,
             dest_keys_per_block, depth + 1);
    skip_rec(in_buf, in_out_buf_iter, key, key_len, off + left, n - left,
             dest_base, dest_keys_per_block, depth + 1);
  }

  static const unsigned int kHuffmanCodingLimit = 16;
  huffman<RefType>* huff_[kHuffmanCodingLimit - 1];

  static const bool kHuffmanEncoding = false;
  static const bool kWeakOrdering = false;
};

}  // namespace ectrie
}  // namespace pdlfs
