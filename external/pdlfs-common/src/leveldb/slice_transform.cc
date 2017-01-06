/*
 * Copyright (c) 2013 The RocksDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/leveldb/slice_transform.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

SliceTransform::~SliceTransform() {}

namespace {
class EchoTransform : public SliceTransform {
 public:
  virtual const char* Name() const { return "leveldb.EchoTransform"; }

  virtual Slice Transform(const Slice& input, std::string* scratch) const {
    return input;
  }
};

class FixedPrefixTransform : public SliceTransform {
 public:
  FixedPrefixTransform(size_t s) : prefix_len_(s) {}

  virtual const char* Name() const { return "leveldb.FixedPrefixTransform"; }

  virtual Slice Transform(const Slice& input, std::string* scratch) const {
    assert(input.size() >= prefix_len_);
    return Slice(input.data(), prefix_len_);
  }

 private:
  size_t prefix_len_;
};
}  // namespace

const SliceTransform* NewEchoTransform() { return new EchoTransform(); }

const SliceTransform* NewFixedPrefixTransform(size_t prefix_len) {
  return new FixedPrefixTransform(prefix_len);
}

}  // namespace pdlfs
