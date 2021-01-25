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
#include "index_block.h"

namespace pdlfs {

void IndexBlockBuilder::AddIndexEntry(std::string* last_key,
                                      const Slice* next_key,
                                      const BlockHandle& block_handle) {
  const Comparator* const cmp = builder_.comparator();
  if (next_key != NULL) {
    cmp->FindShortestSeparator(last_key, *next_key);
  } else {
    cmp->FindShortSuccessor(last_key);
  }

  std::string encoding;
  block_handle.EncodeTo(&encoding);
  builder_.Add(*last_key, encoding);
}

}  // namespace pdlfs
