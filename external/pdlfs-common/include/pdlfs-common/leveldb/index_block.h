/*
 * Copyright (c) 2013 The RocksDB Authors.
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

#include "pdlfs-common/slice.h"
#include "pdlfs-common/status.h"

#include <stddef.h>
#include <stdint.h>
#include <string>

namespace pdlfs {

struct BlockContents;
struct DBOptions;

class BlockHandle;
class Iterator;

// An internal interface used to build the index block for each Table file.
class IndexBuilder {
 public:
  IndexBuilder() {}
  virtual ~IndexBuilder();

  typedef DBOptions Options;

  // Create an index builder according to the specified options.
  static IndexBuilder* Create(const Options* options);

  // This method will be called whenever a new data block
  // is generated.
  // REQUIRES: Finish() has not been called.
  virtual void AddIndexEntry(std::string* last_key, const Slice* next_key,
                             const BlockHandle& block) = 0;

  // This method will be called whenever a new key/value pair
  // has been inserted into the table.
  // REQUIRES: Finish() has not been called.
  virtual void OnKeyAdded(const Slice& key) = 0;

  // Update options.
  virtual Status ChangeOptions(const Options* options) = 0;

  // Returns an estimate of the current size of the index
  // block we are building.
  virtual size_t CurrentSizeEstimate() const = 0;

  // Finish building the block and return a slice that refers to the
  // block contents.
  virtual Slice Finish() = 0;
};

// An internal interface for accessing the index block.
class IndexReader {
 public:
  IndexReader() {}
  virtual ~IndexReader();

  typedef DBOptions Options;

  // Create an index reader according to the specified options.
  static IndexReader* Create(const BlockContents& contents,
                             const Options* options);

  // Return the amount of memory used to hold the index.
  virtual size_t ApproximateMemoryUsage() const = 0;

  // Return an iterator that maps keys to their data blocks.
  virtual Iterator* NewIterator() = 0;
};

}  // namespace pdlfs
