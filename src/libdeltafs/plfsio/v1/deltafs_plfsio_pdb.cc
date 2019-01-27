/*
 * Copyright (c) 2015-2019 Carnegie Mellon University and
 *         Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_pdb.h"

namespace pdlfs {
namespace plfsio {

BufferedBlockWriter::BufferedBlockWriter(const DirOptions& options,
                                         WritableFile* dst, size_t buf_size)
    : DoubleBuffering(&mu_, &bg_cv_, &bb0_, &bb1_),
      options_(options),
      dst_(dst),  // Not owned by us
      bg_cv_(&mu_),
      buf_threshold_(buf_size),
      buf_reserv_(buf_size),
      offset_(0),
      bb0_(options),
      bb1_(options) {
  bb0_.Reserve(buf_reserv_);
  bb1_.Reserve(buf_reserv_);

  mem_buf_ = &bb0_;
}

// Wait for all outstanding compactions to clear.
BufferedBlockWriter::~BufferedBlockWriter() {
  MutexLock ml(&mu_);
  while (has_bg_compaction_) {
    bg_cv_.Wait();
  }
}

// Insert data into the writer.
// REQUIRES: Finish() has NOT been called.
Status BufferedBlockWriter::Add(const Slice& k, const Slice& v) {
  MutexLock ml(&mu_);
  return __Add<BufferedBlockWriter>(k, v);
}

// Force an epoch flush.
// REQUIRES: Finish() has NOT been called.
Status BufferedBlockWriter::EpochFlush() {
  return Flush();  // TODO
}

// Force a compaction but do NOT wait for the compaction to clear.
// REQUIRES: Finish() has NOT been called.
Status BufferedBlockWriter::Flush() {
  MutexLock ml(&mu_);
  return __Flush<BufferedBlockWriter>(false);
}

// Sync data to storage. Data still buffered in memory is NOT sync'ed.
// REQUIRES: Finish() has NOT been called.
Status BufferedBlockWriter::Sync() {
  MutexLock ml(&mu_);
  return __Sync<BufferedBlockWriter>(false);
}

// Wait until there is no outstanding compactions.
// REQUIRES: Finish() has NOT been called.
Status BufferedBlockWriter::Wait() {
  MutexLock ml(&mu_);  // Wait until !has_bg_compaction_
  return __Wait();
}

// Finalize the writer. Expected to be called ONLY once.
Status BufferedBlockWriter::Finish() {
  MutexLock ml(&mu_);
  return __Finish<BufferedBlockWriter>();
}

// REQUIRES: mu_ has been LOCKed.
Status BufferedBlockWriter::Compact(void* buf) {
  mu_.AssertHeld();
  assert(dst_);
  BlockBuf* const bb = static_cast<BlockBuf*>(buf);
  // Skip empty buffers
  if (bb->empty()) return Status::OK();
  mu_.Unlock();  // Unlock during I/O operations
  const Slice blk_contents = bb->Finish(kNoCompression);
  PutFixed64(&indexes_, offset_);
  Status status = dst_->Append(blk_contents);
  // Does not sync data to storage.
  // Sync() does.
  if (status.ok()) {
    offset_ += blk_contents.size();
    status = dst_->Flush();
  }
  mu_.Lock();
  return status;
}

// REQUIRES: mu_ has been LOCKed.
Status BufferedBlockWriter::DumpIndexesAndFilters() {
  PutFixed64(&indexes_, offset_);
  Status status;

  if (status.ok()) {
    bloomfilter_handle_.set_size(bloomfilter_.size());
    bloomfilter_handle_.set_offset(offset_);
    if (!bloomfilter_.empty()) status = dst_->Append(bloomfilter_);
    if (status.ok()) {
      offset_ += bloomfilter_.size();
    }
  }

  if (status.ok()) {
    index_handle_.set_size(indexes_.size());
    index_handle_.set_offset(offset_);
    if (!indexes_.empty()) status = dst_->Append(indexes_);
    if (status.ok()) {
      offset_ += indexes_.size();
    }
  }

  return status;
}

// REQUIRES: mu_ has been LOCKed.
Status BufferedBlockWriter::Close() {
  Status status = DumpIndexesAndFilters();

  if (status.ok()) {
    std::string footer;
    bloomfilter_handle_.EncodeTo(&footer);
    index_handle_.EncodeTo(&footer);
    footer.resize(2 * BlockHandle::kMaxEncodedLength);
    status = dst_->Append(footer);
  }

  if (status.ok()) {
    status = dst_->Sync();
    dst_->Close();
  }

  return status;
}

// REQUIRES: mu_ has been LOCKed.
Status BufferedBlockWriter::SyncBackend(bool close) {
  mu_.AssertHeld();
  assert(dst_);
  if (!close) {
    return dst_->Sync();
  } else {
    return Close();
  }
}

// REQUIRES: mu_ has been LOCKed.
void BufferedBlockWriter::ScheduleCompaction() {
  mu_.AssertHeld();

  assert(has_bg_compaction_);

  if (options_.compaction_pool) {
    options_.compaction_pool->Schedule(BufferedBlockWriter::BGWork, this);
  } else if (options_.allow_env_threads) {
    Env::Default()->Schedule(BufferedBlockWriter::BGWork, this);
  } else {
    DoCompaction<BufferedBlockWriter>();
  }
}

void BufferedBlockWriter::BGWork(void* arg) {
  BufferedBlockWriter* const ins = reinterpret_cast<BufferedBlockWriter*>(arg);
  MutexLock ml(&ins->mu_);
  ins->DoCompaction<BufferedBlockWriter>();
}

BufferedBlockReader::BufferedBlockReader(const DirOptions& options,
                                         RandomAccessFile* src, uint64_t src_sz)
    : options_(options), src_(src), src_sz_(src_sz) {}

bool BufferedBlockReader::GetFrom(Status* status, const Slice& k,
                                  std::string* result, uint64_t offset,
                                  size_t n) {
  BlockContents contents;
  contents.heap_allocated = false;
  contents.cachable = false;
  std::string buf;
  buf.resize(n);
  *status = src_->Read(offset, n, &contents.data, &buf[0]);
  if (status->ok()) {
    if (contents.data.size() != n) {
      *status = Status::IOError("Read ret partial data");
    }
  }

  if (status->ok()) {
    Block block(contents);
    const Comparator* comp = NULL;  // Force linear search
    IteratorWrapper iter(block.NewIterator(comp));
    iter.Seek(k);
    if (iter.Valid()) {
      *result = iter.value().ToString();
      return true;
    }
  }

  return false;
}

// Get the value for a specific key.
Status BufferedBlockReader::Get(const Slice& k, std::string* result) {
  Status status = MaybeLoadCache();
  if (!status.ok()) {
    return status;
  }

  assert(indexes_.size() >= 8);
  uint64_t offset = DecodeFixed64(&indexes_[0]);
  uint64_t next_offset;
  const size_t limit = indexes_.size();
  size_t off = 8;
  for (; off + 7 < limit; off += 8) {
    next_offset = DecodeFixed64(&indexes_[0] + off);
    if (GetFrom(&status, k, result, offset, next_offset - offset)) {
      break;
    } else if (!status.ok()) {
      break;
    }

    offset = next_offset;
  }

  return status;
}

Status BufferedBlockReader::LoadIndexesAndFilters(Slice* footer) {
  BlockHandle bloomfilter_handle;
  BlockHandle index_handle;
  cache_status_ = bloomfilter_handle.DecodeFrom(footer);
  if (cache_status_.ok()) {
    cache_status_ = index_handle.DecodeFrom(footer);
  }
  if (!cache_status_.ok()) {
    return cache_status_;
  }

  uint64_t start = bloomfilter_handle.offset();
  assert(start + bloomfilter_handle.size() == index_handle.offset());
  size_t totalbytes = bloomfilter_handle.size() + index_handle.size();
  cache_.resize(totalbytes);
  cache_status_ = src_->Read(start, totalbytes, &cache_contents_, &cache_[0]);
  if (cache_status_.ok()) {
    if (cache_contents_.size() != totalbytes) {
      cache_status_ = Status::IOError("Read ret partial data");
    }
  }
  if (!cache_status_.ok()) {
    return cache_status_;
  }

  indexes_ = bloomfilter_ = cache_contents_;
  indexes_.remove_prefix(bloomfilter_handle.size());
  bloomfilter_.remove_suffix(index_handle.size());
  if (indexes_.size() < 8) {
    std::string error = "Indexes are shorter than 8 bytes and are invalid";
    cache_status_ = Status::AssertionFailed(error);
  }

  return cache_status_;
}

// Read and cache all indexes and filters.
// Return OK on success, or a non-OK status on errors.
Status BufferedBlockReader::MaybeLoadCache() {
  if (!cache_status_.ok() ||
      !cache_contents_.empty()) {  // Do not repeat previous efforts
    return cache_status_;
  }

  std::string footer_stor;
  footer_stor.resize(2 * BlockHandle::kMaxEncodedLength);
  Slice footer;
  if (src_sz_ < footer_stor.size()) {
    cache_status_ =
        Status::AssertionFailed("Input file is too short for a footer");
  } else {
    cache_status_ = src_->Read(src_sz_ - footer_stor.size(), footer_stor.size(),
                               &footer, &footer_stor[0]);
    if (cache_status_.ok()) {
      if (footer.size() != footer_stor.size()) {
        cache_status_ = Status::IOError("Read ret partial data");
      }
    }
  }

  if (cache_status_.ok()) {
    return LoadIndexesAndFilters(&footer);
  } else {
    return cache_status_;
  }
}

}  // namespace plfsio
}  // namespace pdlfs
