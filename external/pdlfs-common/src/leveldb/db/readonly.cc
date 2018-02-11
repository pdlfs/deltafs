/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/leveldb/db/readonly.h"

namespace pdlfs {

ReadonlyDB::~ReadonlyDB() {}

Status ReadonlyDB::SyncWAL() { return Status::OK(); }

void ReadonlyDB::CompactRange(const Slice* begin, const Slice* end) {}

Status ReadonlyDB::WaitForCompactions() { return Status::OK(); }

Status ReadonlyDB::FlushMemTable(const FlushOptions&) { return Status::OK(); }

Status ReadonlyDB::Put(const WriteOptions&, const Slice& k, const Slice& v) {
  return Status::ReadOnly(Slice());
}

Status ReadonlyDB::Delete(const WriteOptions&, const Slice& k) {
  return Status::ReadOnly(Slice());
}

Status ReadonlyDB::Write(const WriteOptions&, WriteBatch* updates) {
  return Status::ReadOnly(Slice());
}

Status ReadonlyDB::AddL0Tables(const InsertOptions&, const std::string& dir) {
  return Status::ReadOnly(Slice());
}

}  // namespace pdlfs
