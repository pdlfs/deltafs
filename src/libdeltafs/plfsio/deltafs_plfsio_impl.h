/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "v1/deltafs_plfsio_internal.h"

#include "deltafs_plfsio.h"

namespace pdlfs {
namespace plfsio {
namespace v2 {

class BufferList;
class Buffer;

class Buffer {
 public:
  class Iter;
  Iter* NewIter() const;
  size_t CurrentBufferSize() const;
  uint32_t NumEntries() const;
  static Buffer* NewBuf();
  void Add(const Slice& key, const Slice& value);
  void Finish(bool skip_sort);
  void Ref() { nrefs_++; }
  void Unref();

 private:
  struct STLLessThan;
  struct Rep;
  Rep* const r_;
  friend class BufferList;
  BufferList* list_;
  void operator=(const Buffer& buffer);
  Buffer(const Buffer&);
  explicit Buffer(Rep* rep);
  ~Buffer();

  Buffer* prev_;
  Buffer* next_;

  int nrefs_;
};

class BufferList {
 public:
  BufferList() : head_(NULL) {}
  bool empty() const { return (head_.next_ == &head_); }
  Buffer* oldest() const { return (empty() ? NULL : head_.next_); }

  Buffer* Add(Buffer* buf);

  ~BufferList();

 private:
  Buffer* Remove(Buffer* buf);
  friend class Buffer;
  void operator=(const BufferList& buflist);
  BufferList(const BufferList&);
  // Dummy head of doubly-linked entries
  Buffer head_;
};

class VariableLengthBlockBuilder : AbstractBlockBuilder {
 public:
  VariableLengthBlockBuilder();

  void Add(const Slice& key, const Slice& value);

  Slice Finish(CompressionType compression);

  size_t CurrentSize() const;

  void Reset();

 private:
  std::vector<uint32_t> offs_;
};

class SimpleBlockBuilder : AbstractBlockBuilder {
 public:
  SimpleBlockBuilder(uint32_t kbytes, uint32_t vbytes);

  void Add(const Slice& key, const Slice& value);

  Slice Finish(CompressionType compression);

  // Return the current block size.
  size_t CurrentSize() const;

  void Reset();

 private:
  uint32_t kbytes_;
  uint32_t vbytes_;
};

class Formatter {
 public:
 private:
};

class Indexer {
 public:
  // key -> rank,  seq is implicit represented by indexes
  Status Add(const Slice& key, uint32_t seq, int rank);

 private:
};

class L1Indexer {
 public:
  bool has_bg_compaction() const;

  // Insert a key-value pair into the write buffer and return the buffer's
  // sequence number. Return OK on success, or a non-OK status on errors.
  // REQUIRES: mu_ has been locked
  Status Add(const Slice& key, const Slice& value, uint32_t* seq);

  // Return OK if compaction can be scheduled without blocking, or a non-OK
  // status otherwise. REQUIRES: mu_ has been locked
  Status CanFlush() const;

  // Force a buffer flush.
  // Return OK on success, or a non-OK status on errors.
  // REQUIRES: mu_ has been locked
  Status Flush();

 private:
  ~L1Indexer();
  typedef SimpleBlockBuilder Block;
  typedef WritableFile File;
  friend class DirWriter;
  void operator=(const L1Indexer& indexer);
  L1Indexer(const L1Indexer&);

  static void BGWork(void*);
  Status Dump(uint32_t seq, const Slice& contents);
  Status Prepare(bool force);
  void Compact(Block* buf);
  void MaybeScheduleCompaction();
  void DoCompaction();

  const DirOptions& options_;
  size_t buf_threshold_;  // Threshold for regular buffer flushes
  Buffer* const index_buf_;
  port::CondVar* const bg_cv_;
  uint32_t* const bg_dump_seqsrc_;
  Status* const bg_status_;
  port::Mutex* const mu_;
  port::Mutex* const io_mu_;
  uint64_t* const io_offset_;
  File* const io_dst_;

  uint32_t membuf_seq_;
  uint32_t immbuf_seq_;
  Block* membuf_;
  Block* immbuf_;
  Block* buf0_;
  Block* buf1_;
  int refs_;
};

}  // namespace v2
}  // namespace plfsio
}  // namespace pdlfs
