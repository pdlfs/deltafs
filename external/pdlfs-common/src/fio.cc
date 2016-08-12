/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/fio.h"
#include "pdlfs-common/coding.h"

namespace pdlfs {

Fio::~Fio() {}

Slice Fentry::ExtractUntypedKeyPrefix(const Slice& encoding) {
  Slice key_prefix;
  Slice input = encoding;
  GetLengthPrefixedSlice(&input, &key_prefix);
  assert(!key_prefix.empty());
  return key_prefix;
}

bool Fentry::DecodeFrom(Slice* input) {
  Slice key_prefix;
  Slice sli;
  uint32_t u32;
  if (!GetLengthPrefixedSlice(input, &key_prefix)) {
    return false;
  }
#if defined(DELTAFS)
  if (!GetVarint64(input, &pid.reg) || !GetVarint64(input, &pid.snap)) {
    return false;
  }
#endif
  if (!GetVarint64(input, &pid.ino) || !GetLengthPrefixedSlice(input, &sli) ||
      !GetVarint32(input, &u32)) {
    return false;
  } else {
    Key key(key_prefix);
    stat.SetRegId(key.reg_id());
    stat.SetSnapId(key.snap_id());
    stat.SetInodeNo(key.inode());
    nhash = sli.ToString();
    zserver = u32;
    return true;
  }
}

// The encoding has the following format:
//
//   key_prefix_length      varint32
//   key_prefix             char[key_prefix_length]
//   reg_id of parent dir   varint64
//   snap_id of parent dir  varint64
//   ino_no of parent dir   varint64
//   nhash_length           varint32
//   nhash                  char[nhash_length]
//   zserver of parent dir  varint32
Slice Fentry::EncodeTo(char* scratch) const {
  char* p = scratch;

  KeyType dummy = static_cast<KeyType>(0);
  Key key(stat, dummy);
  p = EncodeLengthPrefixedSlice(p, key.prefix());
#if defined(DELTAFS)
  p = EncodeVarint64(p, pid.reg);
  p = EncodeVarint64(p, pid.snap);
#endif
  p = EncodeVarint64(p, pid.ino);
  p = EncodeLengthPrefixedSlice(p, nhash);
  p = EncodeVarint32(p, zserver);

  return Slice(scratch, p - scratch);
}

}  // namespace pdlfs
