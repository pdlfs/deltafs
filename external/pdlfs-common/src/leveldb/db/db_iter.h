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

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */
#pragma once

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/internal_types.h"

#include <stdint.h>

namespace pdlfs {

class DBImpl;

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number into
// appropriate user keys.
extern Iterator* NewDBIterator(  ///
    DBImpl* db, const Comparator* user_key_comparator, Iterator* internal_iter,
    SequenceNumber sequence, uint32_t seed);

}  // namespace pdlfs
