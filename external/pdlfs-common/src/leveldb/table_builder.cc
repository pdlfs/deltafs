/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */
#include "filter_block.h"

#include "pdlfs-common/leveldb/block_builder.h"
#include "pdlfs-common/leveldb/comparator.h"
#include "pdlfs-common/leveldb/db/dbformat.h"
#include "pdlfs-common/leveldb/db/options.h"
#include "pdlfs-common/leveldb/filter_policy.h"
#include "pdlfs-common/leveldb/format.h"
#include "pdlfs-common/leveldb/index_block.h"
#include "pdlfs-common/leveldb/table_builder.h"
#include "pdlfs-common/leveldb/table_properties.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/crc32c.h"
#include "pdlfs-common/env.h"

#include <assert.h>

namespace pdlfs {

struct TableBuilder::Rep {
  Options options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  IndexBuilder* index_block;
  std::string last_key;
  int64_t num_entries;
  int64_t num_blocks;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;
  TableProperties props_;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;

  Rep(const Options& options, WritableFile* f)
      : options(options),
        file(f),
        offset(0),
        data_block(options.block_restart_interval, options.comparator),
        index_block(IndexBuilder::Create(&options)),
        num_entries(0),
        num_blocks(0),
        closed(false),
        filter_block(options.filter_policy != NULL
                         ? new FilterBlockBuilder(options.filter_policy)
                         : NULL),
        pending_index_entry(false) {
    assert(options.comparator != NULL);
  }
};

Status TableBuilder::status() const { return rep_->status; }

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::NumBlocks() const { return rep_->num_blocks; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

const TableProperties* TableBuilder::properties() const {
  return &rep_->props_;
}

const IndexBuilder* TableBuilder::index_builder() const {
  return rep_->index_block;
}

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != NULL) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_->index_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  rep_->options = options;
  rep_->data_block.ChangeRestartInterval(rep_->options.block_restart_interval);
  rep_->index_block->ChangeOptions(&rep_->options);
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  } else {
    r->props_.SetFirstKey(key);
  }

  {
    ParsedInternalKey parsed;
    if (ParseInternalKey(key, &parsed)) {
      r->props_.AddSeq(parsed.sequence);
    }
  }

  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    r->index_block->AddIndexEntry(&r->last_key, &key, r->pending_handle);
    r->pending_index_entry = false;
  }

  if (r->filter_block != NULL) {
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->index_block->OnKeyAdded(key);

  r->data_block.Add(key, value);
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  AddBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
    if (ok()) {
      r->num_blocks++;
      if (r->filter_block != NULL) {
        r->filter_block->StartBlock(r->offset);
      }
    }
  }
}

void TableBuilder::AddBlock(BlockBuilder* builder, BlockHandle* handle) {
  WriteBlock(builder->Finish(), handle);
  builder->Reset();
}

void TableBuilder::WriteBlock(const Slice& block_contents,
                              BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw_block_contents;
  CompressionType type = r->options.compression;
  switch (type) {
    case kNoCompression:
      raw_block_contents = block_contents;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(block_contents.data(), block_contents.size(),
                                compressed) &&
          compressed->size() <
              block_contents.size() - (block_contents.size() / 8u)) {
        raw_block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        raw_block_contents = block_contents;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(raw_block_contents, type, handle);
  r->compressed_output.clear();
}

void TableBuilder::WriteRawBlock(const Slice& raw_block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(raw_block_contents.size());
  r->status = r->file->Append(raw_block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc =
        crc32c::Value(raw_block_contents.data(), raw_block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += raw_block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;
  BlockHandle filter_block_handle;
  BlockHandle props_block_handle;
  BlockHandle metaindex_block_handle;
  BlockHandle index_block_handle;

  // Write filter block
  if (ok()) {
    if (r->filter_block != NULL) {
      WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                    &filter_block_handle);
    }
  }

  // Write stats
  if (ok()) {
    r->props_.SetLastKey(r->last_key);
    std::string props_encoding;
    r->props_.EncodeTo(&props_encoding);
    WriteRawBlock(props_encoding, kNoCompression, &props_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(1);

    if (r->filter_block != NULL) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    std::string key = "table.properties";
    std::string handle_encoding;
    props_block_handle.EncodeTo(&handle_encoding);
    meta_index_block.Add(key, handle_encoding);

    WriteRawBlock(meta_index_block.Finish(), kNoCompression,
                  &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->index_block->AddIndexEntry(&r->last_key, NULL, r->pending_handle);
      r->pending_index_entry = false;
    }
    WriteBlock(r->index_block->Finish(), &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

}  // namespace pdlfs
