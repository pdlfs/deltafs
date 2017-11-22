/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/random.h"

namespace pdlfs {

static uint32_t Permute(uint32_t x) {
  const uint32_t prime = 4294967291u;
  if (x >= prime) {
    return x;  // The 5 integers out of range are mapped to themselves.
  }
  uint32_t residue = static_cast<uint32_t>(
      (static_cast<uint64_t>(x) * static_cast<uint64_t>(x)) % prime);
  return (x <= prime / 2) ? residue : prime - residue;
}

RandomSequence::RandomSequence(uint32_t seed, uint32_t base, uint32_t offset) {
  index_ = Permute(Permute(base) + 0x682f0161) + offset;
  mid_offset_ = Permute(Permute(seed) + 0x46790905);
}

uint32_t RandomSequence::Next() {
  return Permute((Permute(index_++) + mid_offset_) ^ 0x5bf03635);
}

}  // namespace pdlfs
