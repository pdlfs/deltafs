#pragma once

/*
 * Copyright (c) 2013 The RocksDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <string>

#include "pdlfs-common/slice.h"

namespace pdlfs {

// Interface for specifying user-defined functions that perform a
// transformation on a slice.
class SliceTransform {
 public:
  SliceTransform() {}
  virtual ~SliceTransform();

  virtual Slice Transform(const Slice& input, std::string* scratch) const = 0;

  // The name of the transformation.
  virtual const char* Name() const = 0;
};

extern const SliceTransform* NewFixedPrefixTransform(size_t prefix_len);

extern const SliceTransform* NewEchoTransform();

}  // namespace pdlfs
