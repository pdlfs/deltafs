/*
 * Copyright (c) 2018-2019 Carnegie Mellon University and
 *         Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/coding.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/status.h"

#include <stdint.h>
#include <string>

namespace pdlfs {
namespace plfsio {

// Write filter contents into a log file.
class FilterWriter {
 public:
  explicit FilterWriter(WritableFile* dst);

  Status EpochFlush(uint32_t epoch, const Slice& filter_contents);

  Status Finish();

 private:
  WritableFile* const dst_;
  bool finished_;  // If Finish() has been called
  std::string indexes_;
  uint32_t num_ep_written_;
  uint64_t off_;
};

// Read filter contents from a log file.
class FilterReader {
 public:
  FilterReader(RandomAccessFile* src, uint64_t src_sz);

  Status Read(uint32_t epoch, Slice* result, std::string* scratch);

  uint32_t TEST_NumEpochs();

 private:
  Status LoadFilterIndexes(Slice* footer);
  Status MaybeLoadCache();

  RandomAccessFile* const src_;
  const uint64_t src_sz_;
  uint32_t n_;  // Total number of filters
  Status cache_status_;
  std::string cache_storage_;
  Slice indexes_;  // This includes the footer entry
};

}  // namespace plfsio
}  // namespace pdlfs
