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
static const size_t kChunkHeaderSize = 5;
// Fixed number of bytes for chunk trailers
static const size_t kChunkTrailerSize = 4;

enum ChunkType {
  kUnknown = 0x00,  // Useless padding that should be ignored
  kIndexBlock = 0x01,
  kFilterBlock = 0x02,
  kMetaIndexBlock = 0x03,
  kRootBlock = 0x04,
  kEpochSeal = 0x11,
  kFooter = 0xff
};

// Write blocks as log chunks that can be repaired and replayed
// by a future reader. Each log chunk has the following format:
//  - header (5 bytes)
//    * chunk type: uint8_t
//    * chunk length: uint32_t
//        (not including the header and the block trailer)
//  - leveldb compatible block
//    - block contents (n bytes)
//      * binary data: char[n]
//    - block trailer (5 bytes)
//      * block compression type: uint8_t
//      * crc32c: uint32_t
//          (covering block contents and compression type only)
//  - trailer (4 bytes)
//    * crc32c: uint32_t
//        (also covering the header)
class LogWriter {
 public:
  LogWriter(const DirOptions& options, LogSink* sink);
  ~LogWriter();

  Status Write(ChunkType chunk_type, const Slice& block_contents,
               BlockHandle* handle);

  std::string* buffer_store() { return &compressed_; }

 private:
  const DirOptions& options_;

  Status WriteRaw(ChunkType chunk_type, CompressionType compre_type,
                  const Slice& raw, BlockHandle* handle);

  // No copying allowed
  void operator=(const LogWriter&);
  LogWriter(const LogWriter&);

  std::string compressed_;
  LogSink* sink_;
};

}  // namespace plfsio
}  // namespace pdlfs
