/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/leveldb/table.h"
#include "pdlfs-common/coding.h"
#include "pdlfs-common/leveldb/db/dbformat.h"

#include "table_stats.h"

namespace pdlfs {

TableStats::~TableStats() {}

bool TableStats::HasStats(const Table* table) {
  return table->ObtainStats() != NULL;
}

void TableStats::Clear() {
  min_seq_ = kMaxSequenceNumber;
  max_seq_ = 0;
  first_key_.clear();
  last_key_.clear();
}

void TableStats::EncodeTo(std::string* dst) const {
  PutVarint64(dst, min_seq_);
  PutVarint64(dst, max_seq_);
  PutLengthPrefixedSlice(dst, first_key_);
  PutLengthPrefixedSlice(dst, last_key_);
}

Status TableStats::DecodeFrom(const Slice& src) {
  Clear();
  Slice input = src;
  Slice first_key;
  Slice last_key;
  if (!GetVarint64(&input, &min_seq_) || !GetVarint64(&input, &max_seq_) ||
      !GetLengthPrefixedSlice(&input, &first_key) ||
      !GetLengthPrefixedSlice(&input, &last_key)) {
    return Status::Corruption(Slice());
  }
  SetFirstKey(first_key);
  SetLastKey(last_key);
  return Status::OK();
}

uint64_t TableStats::MinSeq(const Table* table) {
  const TableStats* stats = table->ObtainStats();
  assert(stats != NULL);
  return stats->min_seq_;
}

uint64_t TableStats::MaxSeq(const Table* table) {
  const TableStats* stats = table->ObtainStats();
  assert(stats != NULL);
  return stats->max_seq_;
}

Slice TableStats::FirstKey(const Table* table) {
  const TableStats* stats = table->ObtainStats();
  assert(stats != NULL);
  return stats->first_key_;
}

Slice TableStats::LastKey(const Table* table) {
  const TableStats* stats = table->ObtainStats();
  assert(stats != NULL);
  return stats->last_key_;
}

}  // namespace pdlfs
