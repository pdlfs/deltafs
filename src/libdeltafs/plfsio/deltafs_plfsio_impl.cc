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

#include "deltafs_plfsio_impl.h"

#include "pdlfs-common/logging.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/strutil.h"

#include <algorithm>

namespace pdlfs {
namespace plfsio {
namespace v2 {

BufferList::~BufferList() {}

Buffer::Buffer(Rep* r)
    : r_(r), list_(NULL), prev_(this), next_(this), nrefs_(0) {}

struct Buffer::Rep {
  // Starting offsets of inserted entries
  std::vector<uint32_t> offs_;
  std::string buf_;
  bool fin_;
};

void Buffer::Add(const Slice& key, const Slice& value) {
  Rep* const r = r_;
  assert(r && !r->fin_);
  assert(!key.empty());
  r->offs_.push_back(static_cast<uint32_t>(r->buf_.size()));
  PutLengthPrefixedSlice(&r->buf_, key);
  PutLengthPrefixedSlice(&r->buf_, value);
}

struct Buffer::STLLessThan {
  const Slice buf_;

  explicit STLLessThan(const Slice& buf) : buf_(buf) {}

  bool operator()(uint32_t a, uint32_t b) {
    Slice key_a = GetKey(a);
    Slice key_b = GetKey(b);
    assert(!key_a.empty() && !key_b.empty());
    return (key_a < key_b);
  }

  Slice GetKey(uint32_t off) {
    Slice result;
    assert(buf_.size() >= off);
    Slice input(buf_.data() + off, buf_.size() - off);
    if (GetLengthPrefixedSlice(&input, &result)) {
      return result;
    } else {
      abort();
    }
  }
};

void Buffer::Finish(bool skip_sort) {
  Rep* const r = r_;
  assert(r && !r->fin_);
  r->fin_ = true;
  if (!skip_sort) {
    std::sort(r->offs_.begin(), r->offs_.end(), STLLessThan(r->buf_));
  }
}

size_t Buffer::CurrentBufferSize() const {
  Rep* const r = r_;
  return (r ? r->buf_.size() : 0);
}

uint32_t Buffer::NumEntries() const {
  Rep* const r = r_;
  return static_cast<uint32_t>(r ? r->offs_.size() : 0);
}

class Buffer::Iter : public Iterator {
 public:
  explicit Iter(const Buffer::Rep* r)
      : input_(r->buf_), offs_(&r->offs_[0]), n_(r->offs_.size()) {
    cur_ = n_;
  }

  virtual void Next() {
    cur_++;
    if (Valid()) {
      Get();
    }
  }

  virtual void Prev() {
    cur_--;
    if (Valid()) {
      Get();
    }
  }

  virtual ~Iter() {}
  virtual bool Valid() const { return status_.ok() && cur_ < n_; }
  virtual Status status() const { return status_; }
  virtual void SeekToFirst() {
    cur_ = 0;
    if (Valid()) {
      Get();
    }
  }

  virtual void SeekToLast() {
    cur_ = (n_ != 0 ? n_ - 1 : 0);
    if (Valid()) {
      Get();
    }
  }

  virtual void Seek(const Slice& target) {
    // Not supported
  }

  void Get() {
    const size_t off = offs_[cur_];
    assert(input_.size() >= off);
    Slice in(input_.data() + off, input_.size() - off);
    if (!GetLengthPrefixedSlice(&in, &key_) ||
        !GetLengthPrefixedSlice(&in, &value_)) {
      status_ = Status::Corruption("Bad iter contents");
    }
  }

  virtual Slice key() const { return (Valid() ? key_ : Slice()); }

  virtual Slice value() const { return value_; }

 private:
  Status status_;
  Slice value_;
  Slice key_;
  const Slice input_;
  const uint32_t* offs_;
  const size_t n_;
  size_t cur_;
};

Buffer::Iter* Buffer::NewIter() const {
  Rep* const r = r_;
  assert(r && r->fin_);
  return new Iter(r);
}

Buffer* Buffer::NewBuf() { return new Buffer(new Rep); }

Buffer::~Buffer() {
  assert(nrefs_ == 0);
  delete r_;
}

// Removing the last reference will cause the buffer object
// to be removed from its list and to be deleted.
void Buffer::Unref() {
  assert(nrefs_ > 0);
  nrefs_--;
  if (nrefs_ == 0) {
    if (list_) list_->Remove(this);
    delete this;
  }
}

Buffer* BufferList::Remove(Buffer* buf) {
  assert(buf);
  assert(buf != &head_);
  assert(buf->list_ == this);
  assert(buf->nrefs_ == 0);
  buf->prev_->next_ = buf->next_;
  buf->next_->prev_ = buf->prev_;
  buf->prev_ = buf->next_ = buf;
  buf->list_ = NULL;
  return buf;
}

Buffer* BufferList::Add(Buffer* buf) {
  buf->list_ = this;
  buf->next_ = &head_;
  buf->prev_ = head_.prev_;
  buf->prev_->next_ = buf;
  buf->next_->prev_ = buf;
  return buf;
}

namespace {
enum { kFixedLength = 0, kVarLength = 1 };
enum { kForceCompression = true };
enum { kCrc32c = false };
}  // namespace

VariableLengthBlockBuilder::VariableLengthBlockBuilder()
    : AbstractBlockBuilder(NULL) {
  Reset();
}

void VariableLengthBlockBuilder::Add(const Slice& key, const Slice& value) {
  assert(!finished_);
  offs_.push_back(static_cast<uint32_t>(CurrentSize()));
  PutLengthPrefixedSlice(&buffer_, key);
  PutLengthPrefixedSlice(&buffer_, value);
}

Slice VariableLengthBlockBuilder::Finish(CompressionType compression) {
  assert(!finished_);
  for (size_t i = 0; i < offs_.size(); i++) {
    PutFixed32(&buffer_, offs_[i]);
  }
  uint32_t n = static_cast<uint32_t>(offs_.size());
  PutFixed32(&buffer_, n);

  AbstractBlockBuilder::Finish(compression, kForceCompression);
  buffer_.push_back(kVarLength);

  return AbstractBlockBuilder::Finalize(kCrc32c);
}

size_t VariableLengthBlockBuilder::CurrentSize() const {
  return buffer_.size() - buffer_start_;
}

void VariableLengthBlockBuilder::Reset() {
  AbstractBlockBuilder::Reset();

  offs_.clear();
}

SimpleBlockBuilder::SimpleBlockBuilder(uint32_t k, uint32_t v)
    : AbstractBlockBuilder(NULL), kbytes_(k), vbytes_(v) {
  Reset();
}

void SimpleBlockBuilder::Add(const Slice& key, const Slice& value) {
  assert(!finished_);
  assert(key.size() == kbytes_);
  buffer_.append(key.data(), kbytes_);
  assert(value.size() == vbytes_);
  buffer_.append(value.data(), vbytes_);
}

Slice SimpleBlockBuilder::Finish(CompressionType compression) {
  assert(!finished_);
  PutFixed32(&buffer_, vbytes_);
  PutFixed32(&buffer_, kbytes_);

  AbstractBlockBuilder::Finish(compression, kForceCompression);
  buffer_.push_back(kFixedLength);

  return AbstractBlockBuilder::Finalize(kCrc32c);
}

size_t SimpleBlockBuilder::CurrentSize() const {
  return buffer_.size() - buffer_start_;
}

void SimpleBlockBuilder::Reset() {
  AbstractBlockBuilder::Reset();

  //
}

bool L1Indexer::has_bg_compaction() const {
  mu_->AssertHeld();
  return immbuf_;
}

Status L1Indexer::Add(const Slice& key, const Slice& value, uint32_t* seq) {
  mu_->AssertHeld();
  Status status = Prepare(false);
  if (!status.ok()) return status;
  membuf_->Add(key, value);
  *seq = membuf_seq_;
  return status;
}

Status L1Indexer::CanFlush() const {
  mu_->AssertHeld();

  if (immbuf_) return Status::TryAgain(Slice());

  return *bg_status_;
}

Status L1Indexer::Flush() {
  mu_->AssertHeld();
  return Prepare(true);
}

Status L1Indexer::Prepare(bool force) {
  mu_->AssertHeld();
  Status status;
  while (true) {
    if (!bg_status_->ok()) {
      status = *bg_status_;
      break;
    } else if (!force && membuf_->CurrentSize() < buf_threshold_) {
      // There is room in current write buffer
      break;
    } else if (immbuf_) {
      bg_cv_->Wait();
    } else {
      // Attempt to switch to a new write buffer
      assert(!immbuf_);
      immbuf_seq_ = membuf_seq_;
      immbuf_ = membuf_;
      MaybeScheduleCompaction();
      membuf_seq_ = (*bg_dump_seqsrc_)++;
      membuf_ = (membuf_ != buf0_) ? buf0_ : buf1_;
      force = false;
    }
  }

  return status;
}

void L1Indexer::MaybeScheduleCompaction() {
  mu_->AssertHeld();

  assert(immbuf_);

  if (immbuf_->CurrentSize() == 0) {
    immbuf_ = NULL;

    return;
  }

  if (options_.compaction_pool) {
    options_.compaction_pool->Schedule(L1Indexer::BGWork, this);
  } else {
    DoCompaction();
  }
}

void L1Indexer::BGWork(void* arg) {
  L1Indexer* ins = reinterpret_cast<L1Indexer*>(arg);
  MutexLock ml(ins->mu_);
  ins->DoCompaction();
}

void L1Indexer::DoCompaction() {
  mu_->AssertHeld();
  Compact(immbuf_);
  if (bg_status_->ok()) immbuf_ = NULL;
  bg_cv_->SignalAll();
}

void L1Indexer::Compact(Block* buf) {
  Status status;
  mu_->AssertHeld();
  mu_->Unlock();
  assert(buf);
  Slice contents = buf->Finish(options_.compression);
  status = Dump(immbuf_seq_, contents);
  mu_->Lock();
  if (!status.ok()) {
    *bg_status_ = status;
  } else {
    buf->Reset();
  }
}

Status L1Indexer::Dump(uint32_t seq, const Slice& contents) {
  MutexLock ml(io_mu_);
  const uint64_t io_size = contents.size();
  Status status;

  status = io_dst_->Append(contents);
  if (!status.ok()) {
    return status;
  }

  status = io_dst_->Flush();
  if (!status.ok()) {
    return status;
  }

  std::string key, value;
  PutVarint32(&key, seq);  // Fixme

  BlockHandle handle;
  handle.set_size(io_size);
  handle.set_offset(*io_offset_);
  *io_offset_ += io_size;

  handle.EncodeTo(&value);
  index_buf_->Add(key, value);

  return status;
}

}  // namespace v2
}  // namespace plfsio
}  // namespace pdlfs
