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

#pragma once

#include "deltafs_plfsio_format.h"
#include "deltafs_plfsio_types.h"

#include <string>

// Logging facilitates for the write-ahead index log.
// Each index log consists of a list of log entries that we call chunks.
// There are different types of chunks. Each chunk is written atomically, with a
// crc32c checksum.

namespace pdlfs {
namespace plfsio {

// Fixed # bytes for each chunk header.
static const size_t kChunkHeaderSize = 9;

class LogSink;

// Write blocks as log chunks that can be repaired and replayed by a future
// reader. Each log chunk has the following format:
//  - header (9 bytes)
//    * chunk type: uint8_t
//    * contents length: uint32_t
//        (not including the header and the block trailer)
//    * crc32c: uint32_t
//  - leveldb compatible block (n+5 bytes)
//    * block contents: char[n]
//    - block trailer (5 bytes)
//      * block type: uint8_t
//      * crc32c: uint32_t
//          (covering block contents and the block type only)
class LogWriter {
 public:
  LogWriter(const DirOptions& options, LogSink* sink);
  ~LogWriter();

  // Append a regular block into the write-ahead log.
  // Return OK on success, or a non-OK status on errors.
  Status Write(ChunkType chunk_type, const Slice& block_contents,
               BlockHandle* handle);

  // Append a special epoch stone into the write-ahead log.
  // Return OK on success, or a non-OK status on errors.
  Status SealEpoch(const Slice& epoch_stone);

  // Append a special footer block into the write-ahead log.
  // Return OK on success, or a non-OK status on error.
  Status Finish(const Slice& footer);

  // Return the underlying buffer space.
  std::string* buffer_store() { return &compressed_; }

 private:
  const DirOptions& options_;

  Status LogRaw(ChunkType chunk_type, CompressionType compre_type,
                const Slice& data, BlockHandle* handle);

  Status LogSpecial(ChunkType type, const Slice& data);

  // No copying allowed
  void operator=(const LogWriter&);
  LogWriter(const LogWriter&);

  uint64_t prev_seal_;  // Log offset of the previous epoch seal
  std::string compressed_;
  LogSink* sink_;
};

}  // namespace plfsio
}  // namespace pdlfs
