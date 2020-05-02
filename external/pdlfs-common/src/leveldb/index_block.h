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
#pragma once

#include "pdlfs-common/leveldb/block.h"
#include "pdlfs-common/leveldb/block_builder.h"
#include "pdlfs-common/leveldb/comparator.h"
#include "pdlfs-common/leveldb/format.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

class IndexBlockBuilder {
 public:
  IndexBlockBuilder(int restart_interval, const Comparator* cmp)
      : builder_(restart_interval, cmp) {}

  void AddIndexEntry(std::string* last_key, const Slice* next_key,
                     const BlockHandle& block_handle);

  Slice Finish() { return builder_.Finish(); }

  size_t CurrentSizeEstimate() const { return builder_.CurrentSizeEstimate(); }

  void ChangeRestartInterval(int interval) {
    builder_.ChangeRestartInterval(interval);
  }

  void OnKeyAdded(const Slice& key) {
    // Empty
  }

 private:
  BlockBuilder builder_;
};

class IndexBlockReader {
 public:
  explicit IndexBlockReader(const BlockContents& contents) : block_(contents) {}

  size_t ApproximateMemoryUsage() const { return block_.size(); }

  Iterator* NewIterator(const Comparator* cmp) {
    return block_.NewIterator(cmp);
  }

 private:
  Block block_;
};

}  // namespace pdlfs
