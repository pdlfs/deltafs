/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_idx.h"

namespace pdlfs {
namespace plfsio {

DirOutputStats::DirOutputStats()
    : final_data_size(0),
      data_size(0),
      final_meta_index_size(0),
      meta_index_size(0),
      final_index_size(0),
      index_size(0),
      final_filter_size(0),
      filter_size(0),
      value_size(0),
      key_size(0) {}

DirIndexer::DirIndexer(const DirOptions& options) : options_(options) {}

DirIndexer::~DirIndexer() {}

}  // namespace plfsio
}  // namespace pdlfs
