/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_filterio.h"

#include <assert.h>

//            =============== <---+
// filters > |   filter 1    |    |
//            --------------- <---+---------+
//           |   filter 2    |    |         |
//            ---------------     |         |
//           |     ...       |    |         |
//            ---------------     |         |
//           |   filter n    |    |         |
//            --------------- <---+---+     |
// indexes > | ftl 1's epoch |    |   |     |
//           | ftl 1's start |----+   |     |
//            ---------------         |     |
//           | ftl 2's epoch |        |     |
//           | ftl 2's start |--------+-----+
//            ---------------         |
//           |     ...       |        |
//            ---------------         |
//           | ftl n's epoch |        |
//           | ftl n's start |        |
//            ---------------         |
//  footer > | total num ftl |        |
//           | ftl n's limit |--------+
//            ===============
namespace pdlfs {
namespace plfsio {

FilterWriter::FilterWriter(WritableFile* dst)
    : dst_(dst),
      finished_(false),
      num_ep_written_(0),
      off_(0)

{}

Status FilterWriter::EpochFlush(uint32_t epoch, const Slice& filter_contents) {
  if (finished_) {
    return Status::AssertionFailed("Already finished");
  }
  assert(dst_);
  Status status;
  PutFixed32(&indexes_, epoch);
  PutFixed64(&indexes_, off_);
  ++num_ep_written_;
  if (!filter_contents.empty()) {
    status = dst_->Append(filter_contents);
  }
  if (status.ok()) {
    off_ += filter_contents.size();
    status = dst_->Flush();
  }
  return status;
}

Status FilterWriter::Finish() {
  if (finished_) {
    return Status::AssertionFailed("Already finished");
  }
  assert(dst_);
  PutFixed32(&indexes_, num_ep_written_);
  PutFixed64(&indexes_, off_);
  Status status = dst_->Append(indexes_);
  if (status.ok()) {
    status = dst_->Sync();
  }
  finished_ = true;
  return status;
}

FilterReader::FilterReader(RandomAccessFile* src, uint64_t src_sz)
    : src_(src),
      src_sz_(src_sz),
      n_(0)

{}

// Retrieve the filter corresponding to a specific epoch. Return OK on success,
// or a non-OK status on errors.
Status FilterReader::Read(uint32_t const ep, Slice* const result,
                          std::string* scratch) {
  Status status = MaybeLoadCache();
  if (!status.ok()) {
    return status;
  }
  assert(indexes_.size() >= 12);
  if (n_ == 0) {
    return status;
  }

  uint32_t left = 0;
  uint32_t right = n_ - 1;
  while (left < right) {
    uint32_t mid = (left + right + 1) / 2;
    uint32_t midepoch = DecodeFixed32(&indexes_[mid * 12]);
    if (midepoch < ep) {
      // Mid is smaller than target
      left = mid;
    } else {
      // Mid >= target
      right = mid - 1;
    }
  }
  uint32_t start = left * 12;
  uint32_t filterepoch = DecodeFixed32(&indexes_[start]);
  uint64_t offset = DecodeFixed64(&indexes_[start + 4]);
  uint32_t next_filterepoch;
  uint64_t next_offset;
  const size_t limit = indexes_.size();
  size_t off = start + 12;
  for (; off + 11 < limit; off += 12) {
    next_filterepoch = DecodeFixed32(&indexes_[off]);
    next_offset = DecodeFixed64(&indexes_[off + 4]);
    if (filterepoch == ep) {
      scratch->resize(next_offset - offset);
      status = src_->Read(offset, next_offset - offset, result, &(*scratch)[0]);
      break;
    }

    filterepoch = next_filterepoch;
    offset = next_offset;
  }

  return status;
}

uint32_t FilterReader::TEST_NumEpochs() {
  Status status = MaybeLoadCache();
  if (status.ok()) return n_;
  return 0;
}

Status FilterReader::LoadFilterIndexes(Slice* footer) {
  n_ = DecodeFixed32(footer->data());

  if (n_ == 0) {
    cache_storage_.assign(footer->data(), footer->size());
    indexes_ = cache_storage_;
  } else {
    const size_t indexbytes = 12 * (n_ + 1);
    cache_storage_.resize(indexbytes);
    if (src_sz_ < indexbytes) {
      cache_status_ = Status::Corruption("Input file is too short for indexes");
    } else {
      cache_status_ = src_->Read(src_sz_ - indexbytes, indexbytes, &indexes_,
                                 &cache_storage_[0]);
      if (cache_status_.ok()) {
        if (indexes_.size() != indexbytes) {
          cache_status_ = Status::IOError("Read ret partial data");
        }
      }
    }
  }

  return cache_status_;
}

// Read and cache filter indexes and the footer.
// Return OK on success, or a non-OK status on errors.
Status FilterReader::MaybeLoadCache() {
  // Do not repeat prev efforts
  if (!cache_status_.ok() || !indexes_.empty()) {
    return cache_status_;
  }

  char footer_tmp[12];
  Slice footer;
  if (src_sz_ < sizeof(footer_tmp)) {
    cache_status_ = Status::Corruption("Input file too short for a footer");
  } else {
    cache_status_ = src_->Read(src_sz_ - sizeof(footer_tmp), sizeof(footer_tmp),
                               &footer, footer_tmp);
    if (cache_status_.ok()) {
      if (footer.size() != sizeof(footer_tmp)) {
        cache_status_ = Status::IOError("Read ret partial data");
      }
    }
  }

  if (cache_status_.ok()) {
    return LoadFilterIndexes(&footer);
  } else {
    return cache_status_;
  }
}

}  // namespace plfsio
}  // namespace pdlfs
