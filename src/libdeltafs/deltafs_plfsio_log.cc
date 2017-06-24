/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_log.h"

namespace pdlfs {
namespace plfsio {

LogWriter::LogWriter(const DirOptions& options, LogSink* sink)
    : options_(options), sink_(sink) {
  assert(sink_ != NULL);
  sink_->Ref();
}

LogWriter::~LogWriter() { sink_->Unref(); }

Status LogWriter::Write(ChunkType chunk_type, const Slice& block_contents,
                        BlockHandle* handle) {
  Status status;
  Slice raw_contents;
  CompressionType compre_type = options_.compression;
  switch (compre_type) {
    case kNoCompression:
      raw_contents = block_contents;
      break;

    case kSnappyCompression:
      if (port::Snappy_Compress(block_contents.data(), block_contents.size(),
                                &compressed_) &&
          compressed_.size() <
              block_contents.size() - (block_contents.size() / 8u)) {
        raw_contents = compressed_;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        raw_contents = block_contents;
        compre_type = kNoCompression;
      }
      break;
  }
  status = LogRaw(chunk_type, compre_type, raw_contents, handle);
  compressed_.clear();
  return status;
}

Status LogWriter::SealEpoch(const Slice &epoch_stone) {
  return LogSpecial(kEpochSeal, epoch_stone);
}

Status LogWriter::Finish(const Slice& footer) {
  Status status;
  const size_t footer_size = footer.size() + kChunkHeaderSize;
  if (options_.tail_padding) {
    // Add enough padding to ensure the final size of the index log
    // is some multiple of the physical write size.
    const size_t total_size = static_cast<size_t>(sink_->Ltell()) + footer_size;
    const size_t overflow = total_size % options_.index_buffer;
    if (overflow != 0) {
      const size_t n = options_.index_buffer - overflow;
      status = sink_->Lwrite(std::string(n, 0));
    } else {
      // No need to pad
    }
  }
  if (status.ok()) {
    status = LogSpecial(kFooter, footer);
  }
  return status;
}

Status LogWriter::LogSpecial(ChunkType chunk_type, const Slice& contents) {
  Status status;
  const size_t contents_size = contents.size();
  char header[kChunkHeaderSize];
  header[0] = chunk_type;
  EncodeFixed32(header + 1, static_cast<uint32_t>(contents_size));
  if (!options_.skip_checksums) {
    uint32_t crc = crc32c::Value(contents.data(), contents_size);
    crc = crc32c::Extend(crc, header, 5);
    EncodeFixed32(header + 5, crc32c::Mask(crc));
  } else {
    EncodeFixed32(header + 5, 0);
  }
  Slice slis[2];
  slis[0] = Slice(header, sizeof(header));
  slis[1] = contents;
  for (size_t i = 0; i < 2; i++) {
    status = sink_->Lwrite(slis[i]);
    if (!status.ok()) {
      break;
    }
  }
  return status;
}

Status LogWriter::LogRaw(ChunkType chunk_type, CompressionType compre_type,
                         const Slice& contents, BlockHandle* handle) {
  Status status;
  const size_t contents_size = contents.size();
  char header[kChunkHeaderSize];
  header[0] = chunk_type;
  EncodeFixed32(header + 1, static_cast<uint32_t>(contents_size));
  char block_trailer[kBlockTrailerSize];
  block_trailer[0] = compre_type;
  if (!options_.skip_checksums) {
    uint32_t crc = crc32c::Value(contents.data(), contents_size);
    crc = crc32c::Extend(crc, block_trailer, 1);
    EncodeFixed32(block_trailer + 1, crc32c::Mask(crc));
    crc = crc32c::Extend(crc, header, 5);
    EncodeFixed32(header + 5, crc32c::Mask(crc));
  } else {
    EncodeFixed32(block_trailer + 1, 0);
    EncodeFixed32(header + 5, 0);
  }
  Slice slis[3];
  slis[0] = Slice(header, sizeof(header));
  slis[1] = contents;
  slis[2] = Slice(block_trailer, sizeof(block_trailer));
  const uint64_t offset = sink_->Ltell();
  for (size_t i = 0; i < 3; i++) {
    status = sink_->Lwrite(slis[i]);
    if (!status.ok()) {
      break;
    }
  }
  if (status.ok()) {
    handle->set_offset(offset + sizeof(header));
    handle->set_size(contents_size);
  }
  return status;
}

}  // namespace plfsio
}  // namespace pdlfs
