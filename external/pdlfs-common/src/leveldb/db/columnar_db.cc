/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/leveldb/db/columnar_db.h"

namespace pdlfs {

ColumnarDB::~ColumnarDB() {}

Status ColumnarDB::AddL0Tables(const InsertOptions& options,
                               const std::string& dir) {
  return Status::NotSupported(Slice());
}

Status ColumnarDB::Dump(const DumpOptions& options, const Range& range,
                        const std::string& dir, SequenceNumber* min_seq,
                        SequenceNumber* max_seq) {
  return Status::NotSupported(Slice());
}

bool ColumnarDB::GetProperty(const Slice& property, std::string* value) {
  return false;
}

void ColumnarDB::GetApproximateSizes(const Range* range, int n,
                                     uint64_t* sizes) {
  // Empty
}

void ColumnarDB::CompactRange(const Slice* begin, const Slice* end) {
  // Empty
}

}  // namespace pdlfs
