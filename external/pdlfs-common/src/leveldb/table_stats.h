#pragma once

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

#include "pdlfs-common/leveldb/table.h"
#include "pdlfs-common/leveldb/table_properties.h"
#include "pdlfs-common/slice.h"
#include "pdlfs-common/status.h"

#include <assert.h>

namespace pdlfs {

class TableStats {
 public:
  // Return true iff the table has all stats loaded.
  static bool HasStats(const Table* table) {
    return table->GetProperties() != NULL;
  }

  // Return the first key in the table.
  // REQUIRES: TableStats::HasStats(table) == true.
  static Slice FirstKey(const Table* table) {
    const TableProperties* props = table->GetProperties();
    assert(props != NULL);
    return props->first_key();
  }

  // Return the last key in the table.
  // REQUIRES: TableStats::HasStats(table) == true.
  static Slice LastKey(const Table* table) {
    const TableProperties* props = table->GetProperties();
    assert(props != NULL);
    return props->last_key();
  }

  // Return the maximum sequence number among all keys in the table.
  // REQUIRES: TableStats::HasStats(table) == true.
  static uint64_t MaxSeq(const Table* table) {
    const TableProperties* props = table->GetProperties();
    assert(props != NULL);
    return props->max_seq();
  }

  // Return the minimum sequence number among all keys in the table.
  // REQUIRES: TableStats::HasStats(table) == true.
  static uint64_t MinSeq(const Table* table) {
    const TableProperties* props = table->GetProperties();
    assert(props != NULL);
    return props->min_seq();
  }
};

}  // namespace pdlfs
