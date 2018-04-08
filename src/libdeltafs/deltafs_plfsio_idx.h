/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "deltafs_plfsio.h"
#include "deltafs_plfsio_format.h"
#include "deltafs_plfsio_nio.h"
#include "deltafs_plfsio_recov.h"

namespace pdlfs {
namespace plfsio {

// Stats for indexed directory data.
struct DirOutputStats {  // All final sizes include padding and block trailers
  DirOutputStats();

  // Total size of data blocks
  size_t final_data_size;
  size_t data_size;

  // Total size of meta index blocks and the root meta index block
  size_t final_meta_index_size;
  size_t meta_index_size;

  // Total size of index blocks
  size_t final_index_size;
  size_t index_size;

  // Total size of filter blocks
  size_t final_filter_size;
  size_t filter_size;

  // Total size of user data compacted
  size_t value_size;
  size_t key_size;
};

// Index streaming directory data and write the results into a pair of log
// files.
class DirIndexer {
 public:
  explicit DirIndexer(const DirOptions& options);
  virtual ~DirIndexer();

  virtual void Add(const Slice& key, const Slice& value) = 0;

  // Finish building the current table. Optionally, a filter can be specified
  // that is associated with the table.
  // REQUIRES: Finish() has not been called.
  virtual void EndTable(void* filter, ChunkType filter_type) = 0;

  // Force the start of a new epoch.
  // REQUIRES: Finish() has not been called.
  virtual void MakeEpoch() = 0;

  // Finalize directory contents.
  // No further writes.
  virtual Status Finish() = 0;

 protected:
  const DirOptions& options_;
  DirOutputStats output_stats_;
  Status status_;

  bool ok() const { return status_.ok(); }
  Status status() const { return status_; }

  template <typename T>
  friend class DirBuilder;

 private:
  // No copying allowed
  void operator=(const DirIndexer&);
  DirIndexer(const DirIndexer&);
};

}  // namespace plfsio
}  // namespace pdlfs
