/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>

#include "pdlfs-common/fstypes.h"
#include "pdlfs-common/gigaplus.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

void Key::SetName(const Slice& name) {
  Slice r = DirIndex::Hash(name, rep_ + 8);
  assert(r.size() == 8);
  (void)r;
}

void Key::SetHash(const Slice& hash) {
  assert(hash.size() == 8);
  memcpy(rep_ + 8, hash.data(), 8);
}

Key::Key(uint64_t dir_id, KeyType type) {
  uint64_t pack = (dir_id << 8) | (type & 0xff);
  pack = htobe64(pack);
  memcpy(rep_, &pack, 8);
}

Key::Key(uint64_t dir_id, KeyType type, const Slice& name) {
  uint64_t pack = (dir_id << 8) | (type & 0xff);
  pack = htobe64(pack);
  memcpy(rep_, &pack, 8);
  SetName(name);
}

uint64_t Key::dir_id() const {
  uint64_t pack;
  memcpy(&pack, rep_, 8);
  uint64_t r = be64toh(pack) >> 8;
  return r;
}

KeyType Key::type() const {
  uint64_t pack;
  memcpy(&pack, rep_, 8);
  KeyType type = static_cast<KeyType>(be64toh(pack) & 0xff);
  return type;
}

Slice Key::hash() const {
  Slice r = Slice(rep_ + 8, 8);
  return r;
}

Slice Key::prefix() const {
  Slice r = Slice(rep_, 8);
  return r;
}

}  // namespace pdlfs
