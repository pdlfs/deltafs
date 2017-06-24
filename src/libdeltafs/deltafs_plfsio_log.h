/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "deltafs_plfsio.h"
#include "deltafs_plfsio_format.h"

#include <string>

namespace pdlfs {
namespace plfsio {

// Fixed number of bytes for chunk headers
static const size_t kChunkHeaderSize = 9;

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

  Status Write(ChunkType chunk_type, const Slice& block_contents,
               BlockHandle* handle);

  Status SealEpoch(const Slice& epoch_stone);

  Status Finish(const Slice& footer);

  std::string* buffer_store() { return &compressed_; }

 private:
  const DirOptions& options_;

  Status LogRaw(ChunkType chunk_type, CompressionType compre_type,
                const Slice& data, BlockHandle* handle);

  Status LogSpecial(ChunkType type, const Slice& data);

  // No copying allowed
  void operator=(const LogWriter&);
  LogWriter(const LogWriter&);

  std::string compressed_;
  LogSink* sink_;
};

}  // namespace plfsio
}  // namespace pdlfs
