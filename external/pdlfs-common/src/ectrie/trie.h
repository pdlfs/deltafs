#pragma once

/*
 * Copyright (c) 2015 The SILT Authors.
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
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
  explicit trie(huffman_buffer<RefType>* huff_buf = NULL)
      : huff_buf_(huff_buf) {}

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

    // encode the left tree size
    if (huff_buf_ != NULL && n <= huff_buf_->encoding_limit()) {
      (*huff_buf_)[n - 2]->encode(out_buf, left);
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
    if (huff_buf_ != NULL && n <= huff_buf_->encoding_limit()) {
      left = (*huff_buf_)[n - 2]->decode(in_buf, in_out_buf_iter);
    } else {
      left = sign_interleave::decode<size_t>(
                 exp_golomb<>::decode<size_t>(in_buf, in_out_buf_iter)) +
             n / 2;
    }

    assert(left <= n);

    // find the number of keys on the left to the key (considering weak
    // ordering)
    if (!bit_access::get(key, depth)) {
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
    if (huff_buf_ != NULL && n <= huff_buf_->encoding_limit()) {
      left = (*huff_buf_)[n - 2]->decode(in_buf, in_out_buf_iter);
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

  // non-NULL if huffman encoding has been enabled
  huffman_buffer<RefType>* huff_buf_;
};

}  // namespace ectrie
}  // namespace pdlfs
