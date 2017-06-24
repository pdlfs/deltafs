/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

// XXX: Reuse formats defined by leveldb

#include "../../external/pdlfs-common/src/leveldb/block.h"
#include "../../external/pdlfs-common/src/leveldb/block_builder.h"
#include "../../external/pdlfs-common/src/leveldb/format.h"

#include "pdlfs-common/leveldb/comparator.h"
#include "pdlfs-common/leveldb/iterator.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/crc32c.h"
#include "pdlfs-common/env.h"

namespace pdlfs {
namespace plfsio {
static const uint32_t kMaxTableNo = 9999;
static const uint32_t kMaxEpochNo = 9999;

// Formats used by keys in the meta index blocks.
extern std::string EpochKey(uint32_t epoch);
extern std::string EpochTableKey(uint32_t epoch, uint32_t table);
extern Status ParseEpochKey(const Slice& input, uint32_t* epoch,
                            uint32_t* table);

// Type definition for write ahead log chunks
enum ChunkType {
  kUnknown = 0x00,  // Useless padding that should be ignored

  // Standard block types for data indexing
  kIdxChunk = 0x01,
  kSbfChunk = 0x02,   // Standard bloom filters
  kMetaChunk = 0x03,  // Meta indexes for each epoch
  kRtChunk = 0x04,    // One per directory

  // Special types for durability
  kEpochSeal = 0x11,
  kFooter = 0xff
};

// Table handle is a pointer to extends of a file that store the index and
// filter data of a table. In addition, table handle also stores
// the key range.
class TableHandle {
 public:
  TableHandle();

  // The offset of the filter block in a file.
  uint64_t filter_offset() const { return filter_offset_; }
  void set_filter_offset(uint64_t offset) { filter_offset_ = offset; }

  // The size of the filter block.
  uint64_t filter_size() const { return filter_size_; }
  void set_filter_size(uint64_t size) { filter_size_ = size; }

  // The offset of the index block in a file.
  uint64_t index_offset() const { return index_offset_; }
  void set_index_offset(uint64_t offset) { index_offset_ = offset; }

  // The size of the index block.
  uint64_t index_size() const { return index_size_; }
  void set_index_size(uint64_t size) { index_size_ = size; }

  // The smallest key within the table.
  Slice smallest_key() const { return smallest_key_; }
  void set_smallest_key(const Slice& key) { smallest_key_ = key.ToString(); }

  // The largest key within the table.
  Slice largest_key() const { return largest_key_; }
  void set_largest_key(const Slice& key) { largest_key_ = key.ToString(); }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

 private:
  // Key range of the table
  std::string smallest_key_;
  std::string largest_key_;
  // Handle to the filter and the index block
  uint64_t filter_offset_;
  uint64_t filter_size_;
  uint64_t index_offset_;
  uint64_t index_size_;
};

// A special marker representing the completion of an epoch.
class EpochSeal {
 public:
  EpochSeal();

  const BlockHandle& handle() const { return handle_; }
  void set_handle(const BlockHandle& handle) { handle_ = handle; }

  uint32_t id() const { return id_; }
  void set_id(uint32_t id) { id_ = id; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

 private:
  BlockHandle handle_;  // Meta index for the epoch

  uint32_t id_;  // Seal Id
};

// Fixed information stored at the end of every log file.
class Footer {
 public:
  Footer();

  // Important options
  uint32_t lg_parts() const { return lg_parts_; }
  void set_lg_parts(uint32_t lg) { lg_parts_ = lg; }

  unsigned char skip_checksums() const { return skip_checksums_; }
  void set_skip_checksums(unsigned char skip) { skip_checksums_ = skip; }

  unsigned char unique_keys() const { return unique_keys_; }
  void set_unique_keys(unsigned char uni) { unique_keys_ = uni; }

  // The block handle for the root index.
  const BlockHandle& epoch_index_handle() const { return epoch_index_handle_; }
  void set_epoch_index_handle(const BlockHandle& h) { epoch_index_handle_ = h; }

  // Total number of epoches
  uint32_t num_epoches() const { return num_epoches_; }
  void set_num_epoches(uint32_t num) { num_epoches_ = num; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

  // Encoded length of a Footer. It consists of one encoded block
  // handle, a couple of options, and a magic number.
  enum { kEncodedLength = BlockHandle::kMaxEncodedLength + 10 + 8 };

 private:
  BlockHandle epoch_index_handle_;
  uint32_t num_epoches_;
  uint32_t lg_parts_;
  unsigned char skip_checksums_;
  unsigned char unique_keys_;
};

inline TableHandle::TableHandle()
    : filter_offset_(~static_cast<uint64_t>(0) /* Invalid offset */),
      filter_size_(~static_cast<uint64_t>(0) /* Invalid size */),
      index_offset_(~static_cast<uint64_t>(0) /* Invalid offset */),
      index_size_(~static_cast<uint64_t>(0) /* Invalid size */) {
  // Empty
}

inline EpochSeal::EpochSeal()
    : id_(~static_cast<uint32_t>(0) /* Invalid id */) {
  // Empty
}

inline Footer::Footer()
    : num_epoches_(~static_cast<uint32_t>(0) /* Invalid */),
      lg_parts_(~static_cast<uint32_t>(0) /* Invalid */),
      skip_checksums_(~static_cast<unsigned char>(0) /* Invalid */),
      unique_keys_(~static_cast<unsigned char>(0) /* Invalid */) {
  // Empty
}

}  // namespace plfsio
}  // namespace pdlfs
