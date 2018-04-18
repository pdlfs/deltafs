/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_internal.h"
#include "deltafs_plfsio_events.h"
#include "deltafs_plfsio_filter.h"

#include "pdlfs-common/logging.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/strutil.h"

#include <assert.h>
#include <math.h>
#include <algorithm>

namespace pdlfs {
extern const char* GetLengthPrefixedSlice(const char* p, const char* limit,
                                          Slice* result);
namespace plfsio {

// Return current time in microseconds.
static inline uint64_t GetCurrentTimeMicros() {
  return Env::Default()->NowMicros();
}

// Create a new write buffer and determine an estimated memory usage per
// entry. For small entries, there is a 2-byte overhead per entry.
// This overhead is necessary for supporting variable length
// key-value pairs.
WriteBuffer::WriteBuffer(const DirOptions& options)
    : num_entries_(0), finished_(false) {
  const size_t entry_size =  // Estimated, actual entry sizes may differ
      options.key_size + options.value_size;
  bytes_per_entry_ =  // Memory usage per entry
      static_cast<size_t>(
          VarintLength(options.key_size)) +  // Varint encoding for key lengths
      static_cast<size_t>(
          VarintLength(options.value_size)) +  // For value lengths
      entry_size;
}

class WriteBuffer::Iter : public Iterator {
 public:
  explicit Iter(const WriteBuffer* write_buffer)
      : cursor_(-1),
        offsets_(&write_buffer->offsets_[0]),
        num_entries_(write_buffer->num_entries_),
        buffer_(write_buffer->buffer_) {}

  virtual ~Iter() {}
  virtual void Next() { cursor_++; }
  virtual void Prev() { cursor_--; }
  virtual Status status() const { return Status::OK(); }
  virtual bool Valid() const { return cursor_ >= 0 && cursor_ < num_entries_; }
  virtual void SeekToFirst() { cursor_ = 0; }
  virtual void SeekToLast() { cursor_ = num_entries_ - 1; }
  virtual void Seek(const Slice& target) {
    // Not supported
  }

  virtual Slice key() const {
    assert(Valid());
    Slice result;
    const char* p = &buffer_[offsets_[cursor_]];
    Slice input = buffer_;
    assert(p - buffer_.data() >= 0);
    input.remove_prefix(p - buffer_.data());
    if (GetLengthPrefixedSlice(&input, &result)) {
      return result;
    } else {
      assert(false);
      result = Slice();
      return result;
    }
  }

  virtual Slice value() const {
    assert(Valid());
    Slice result;
    const char* p = &buffer_[offsets_[cursor_]];
    Slice input = buffer_;
    assert(p - buffer_.data() >= 0);
    input.remove_prefix(p - buffer_.data());
    if (GetLengthPrefixedSlice(&input, &result) &&
        GetLengthPrefixedSlice(&input, &result)) {
      return result;
    } else {
      assert(false);
      result = Slice();
      return result;
    }
  }

 private:
  int cursor_;
  const uint32_t* offsets_;
  int num_entries_;
  Slice buffer_;
};

Iterator* WriteBuffer::NewIterator() const {
  assert(finished_);
  return new Iter(this);
}

struct WriteBuffer::STLLessThan {
  Slice buffer_;

  explicit STLLessThan(const Slice& buffer) : buffer_(buffer) {}

  bool operator()(uint32_t a, uint32_t b) {
    Slice key_a = GetKey(a);
    Slice key_b = GetKey(b);
    assert(!key_a.empty() && !key_b.empty());
    return key_a < key_b;
  }

  Slice GetKey(uint32_t offset) {
    Slice result;
    const char* p = GetLengthPrefixedSlice(
        buffer_.data() + offset, buffer_.data() + buffer_.size(), &result);
    if (p != NULL) {
      return result;
    } else {
      assert(false);
      return result;
    }
  }
};

void WriteBuffer::Finish(bool skip_sort) {
  assert(!finished_);
  finished_ = true;
  // Sort entries if not skipped
  if (!skip_sort) {
    std::vector<uint32_t>::iterator begin = offsets_.begin();
    std::vector<uint32_t>::iterator end = offsets_.end();
    std::sort(begin, end, STLLessThan(buffer_));
  }
}

void WriteBuffer::Reset() {
  num_entries_ = 0;
  finished_ = false;
  offsets_.clear();
  buffer_.clear();
}

void WriteBuffer::Reserve(size_t bytes_to_reserve) {
  // Reserve memory for the write buffer
  buffer_.reserve(bytes_to_reserve);
  const uint32_t num_entries =  // Estimated, actual counts may differ
      static_cast<uint32_t>(ceil(double(bytes_to_reserve) / bytes_per_entry_));
  // Also reserve memory for the offset array
  offsets_.reserve(num_entries);
}

bool WriteBuffer::Add(const Slice& key, const Slice& value) {
  assert(!finished_);       // Finish() has not been called
  assert(key.size() != 0);  // Key cannot be empty
  const size_t offset = buffer_.size();
  PutLengthPrefixedSlice(&buffer_, key);
  PutLengthPrefixedSlice(&buffer_, value);
  offsets_.push_back(static_cast<uint32_t>(offset));
  num_entries_++;
  return true;
}

size_t WriteBuffer::memory_usage() const {
  size_t result = 0;
  result += sizeof(uint32_t) * offsets_.capacity();
  result += buffer_.capacity();
  return result;
}

template <typename T>
DirIndexer<T>::DirIndexer(const DirOptions& options, size_t part,
                          port::Mutex* mu, port::CondVar* cv)
    : options_(options),
      bg_cv_(cv),
      mu_(mu),
      part_(part),
      num_flush_requested_(0),
      num_flush_completed_(0),
      has_bg_compaction_(false),
      filter_(NULL),
      mem_buf_(NULL),
      imm_buf_(NULL),
      imm_buf_is_forced_(false),
      imm_buf_is_epoch_flush_(false),
      imm_buf_is_final_(false),
      buf0_(options),
      buf1_(options),
      bu_(NULL),
      data_(NULL),
      indx_(NULL),
      opened_(false),
      refs_(0) {
  size_t memory =  // Total write buffer memory for each sub-partition
      options_.total_memtable_budget /
          static_cast<uint32_t>(1 << options_.lg_parts) -
      options_.block_batch_size;  // Reserved for compaction

  tb_bytes_ = memory / 2;  // Due to double buffering

  buf_threshold_ =
      static_cast<size_t>(floor(tb_bytes_ * options_.memtable_util));
  buf_reserv_ = static_cast<size_t>(ceil(tb_bytes_ * options_.memtable_reserv));

  // Estimate filter size
  size_t entry_size = options_.key_size + options_.value_size;
  size_t num_keys = tb_bytes_ / entry_size;
  ft_bits_ = options_.filter_bits_per_key * num_keys;

  ft_bytes_ = (ft_bits_ + 7) / 8;
  ft_bits_ = ft_bytes_ * 8;

  if (part == 0) {
#if VERBOSE >= 2
    Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.internal.tb_size -> %s",
            PrettySize(tb_bytes_).c_str());
    Verbose(__LOG_ARGS__, 2, "Dfs.plfsdir.internal.ft_size -> %s",
            PrettySize(ft_bytes_).c_str());
#endif
  }

  // Allocate memory
  buf0_.Reserve(buf_reserv_);
  buf1_.Reserve(buf_reserv_);

  if (options_.filter == kFtNoFilter) {
    // Skip filter
  } else if (options_.filter != kFtBloomFilter ||
             options_.bf_bits_per_key != 0) {
    filter_ = new T(options_, ft_bytes_);
  } else {
    // Skip filter
  }

  mem_buf_ = &buf0_;
}

template <typename T>
DirIndexer<T>::~DirIndexer() {
  mu_->AssertHeld();
  while (has_bg_compaction_) {
    bg_cv_->Wait();
  }
  delete bu_;
  if (data_ != NULL) data_->Unref();
  if (indx_ != NULL) indx_->Unref();
  delete filter_;
}

template <typename T>
Status DirIndexer<T>::Open(LogSink* data, LogSink* indx) {
  // Open() has not been called before
  assert(!opened_);
  opened_ = true;
  data_ = data;
  indx_ = indx;

  data_->Ref();
  indx_->Ref();
  bu_ = DirBuilder::Open(options_, &stats_, data, indx);

  return Status::OK();
}

// True iff there is an on-going background compaction.
template <typename T>
bool DirIndexer<T>::has_bg_compaction() {
  mu_->AssertHeld();
  return has_bg_compaction_;
}

// Report background compaction status.
template <typename T>
Status DirIndexer<T>::bg_status() {
  mu_->AssertHeld();
  return bg_status_;
}

// Sync and pre-close all linked log files.
// By default, log files are reference-counted and are implicitly closed when
// de-referenced by the last opener. Optionally, a caller may force data
// sync and pre-closing all log files.
template <typename T>
Status DirIndexer<T>::SyncAndClose() {
  Status status;
  if (!opened_) return status;
  assert(data_ != NULL);
  data_->Lock();
  status = data_->Lclose(true);
  data_->Unlock();
  assert(indx_ != NULL);
  if (status.ok()) status = indx_->Lclose(true);
  return status;
}

// If dry_run has been set, simply perform status checks and no compaction
// jobs will be scheduled or waited for. Return immediately, and return OK if
// compaction may be scheduled immediately without waiting, or return a special
// status if compaction cannot be scheduled immediately due to lack of buffer
// space, or directly return a status that indicates an I/O error.
// Otherwise, **wait** until a compaction is scheduled unless
// options_.non_blocking is set. After a compaction has been scheduled,
// **wait** until it finishes unless no_wait has been set.
template <typename T>
Status DirIndexer<T>::Flush(const FlushOptions& flush_options) {
  mu_->AssertHeld();
  assert(opened_);
  // Wait for buffer space
  while (imm_buf_ != NULL) {
    if (flush_options.dry_run || options_.non_blocking) {
      return Status::BufferFull(Slice());
    } else {
      bg_cv_->Wait();
    }
  }

  Status status;
  if (flush_options.dry_run) {
    status = bg_status_;  // Status check only
  } else {
    num_flush_requested_++;
    const uint32_t thres = num_flush_requested_;
    const bool force = true;
    status = Prepare(force, flush_options.epoch_flush, flush_options.finalize);
    if (status.ok()) {
      if (!flush_options.no_wait) {
        while (num_flush_completed_ < thres) {
          bg_cv_->Wait();
        }
      }
    }
  }

  return status;
}

template <typename T>
Status DirIndexer<T>::Add(const Slice& key, const Slice& value) {
  mu_->AssertHeld();
  assert(opened_);
  Status status = Prepare();
  while (status.ok()) {
    // Implementation may reject a key-value insertion
    if (!mem_buf_->Add(key, value)) {
      status = Prepare();
    } else {
      break;
    }
  }
  return status;
}

template <typename T>
Status DirIndexer<T>::Prepare(bool force /* force minor memtable flush */,
                              bool epoch_flush /* force epoch flush */,
                              bool finalize) {
  mu_->AssertHeld();
  Status status;
  assert(mem_buf_ != NULL);
  while (true) {
    if (!bg_status_.ok()) {
      status = bg_status_;
      break;
    } else if (!force && !mem_buf_->NeedCompaction() &&
               mem_buf_->CurrentBufferSize() < buf_threshold_) {
      // There is room in current write buffer
      break;
    } else if (imm_buf_ != NULL) {
      if (options_.non_blocking) {
        status = Status::BufferFull(Slice());
        break;
      } else {
        bg_cv_->Wait();
      }
    } else {
      // Attempt to switch to a new write buffer
      assert(imm_buf_ == NULL);
      imm_buf_ = mem_buf_;
      if (force) imm_buf_is_forced_ = true;
      force = false;
      if (epoch_flush) imm_buf_is_epoch_flush_ = true;
      epoch_flush = false;
      if (finalize) imm_buf_is_final_ = true;
      finalize = false;
      WriteBuffer* const current_buf = mem_buf_;
      MaybeScheduleCompaction();
      if (current_buf == &buf0_) {
        mem_buf_ = &buf1_;
      } else {
        mem_buf_ = &buf0_;
      }
    }
  }

  return status;
}

template <typename T>
void DirIndexer<T>::MaybeScheduleCompaction() {
  mu_->AssertHeld();

  // Do not schedule more if we are in error status
  if (!bg_status_.ok()) {
    return;
  }
  // Skip if there is one already scheduled
  if (has_bg_compaction_) {
    return;
  }
  // Nothing to be scheduled
  if (imm_buf_ == NULL) {
    return;
  }

  has_bg_compaction_ = true;

  if (options_.compaction_pool != NULL) {
    options_.compaction_pool->Schedule(DirIndexer::BGWork, this);
  } else if (options_.allow_env_threads) {
    Env::Default()->Schedule(DirIndexer::BGWork, this);
  } else {
    DoCompaction();
  }
}

template <typename T>
void DirIndexer<T>::BGWork(void* arg) {
  DirIndexer* ins = reinterpret_cast<DirIndexer*>(arg);
  MutexLock ml(ins->mu_);
  ins->DoCompaction();
}

template <typename T>
void DirIndexer<T>::DoCompaction() {
  mu_->AssertHeld();
  assert(has_bg_compaction_);
  assert(imm_buf_ != NULL);
  CompactMemtable();
  imm_buf_->Reset();
  imm_buf_is_forced_ = false;
  imm_buf_is_epoch_flush_ = false;
  imm_buf_is_final_ = false;
  imm_buf_ = NULL;
  has_bg_compaction_ = false;
  MaybeScheduleCompaction();
  bg_cv_->SignalAll();
}

template <typename T>
void DirIndexer<T>::CompactMemtable() {
  mu_->AssertHeld();
  WriteBuffer* const buffer = imm_buf_;
  assert(buffer != NULL);
  const bool is_final = imm_buf_is_final_;
  const bool is_epoch_flush = imm_buf_is_epoch_flush_;
  const bool is_forced = imm_buf_is_forced_;
  DirBuilder* const bu = bu_;
  T* const ft = filter_;
  mu_->Unlock();
  const uint64_t start = GetCurrentTimeMicros();
  if (options_.listener != NULL) {
    CompactionEvent event;
    event.type = kCompactionStart;
    event.micros = start;
    event.part = part_;
    options_.listener->OnEvent(kCompactionStart, &event);
  }
#if VERBOSE >= 3
  Verbose(__LOG_ARGS__, 3, "Compacting memtable: %d/%d Bytes (%.2f%%) ...",
          static_cast<int>(buffer->CurrentBufferSize()),
          static_cast<int>(tb_bytes_),
          100.0 * buffer->CurrentBufferSize() / tb_bytes_);
#ifndef NDEBUG
  const DirOutputStats stats(stats_);
#endif
#endif  // VERBOSE
#ifndef NDEBUG
  uint32_t num_keys = 0;
#endif
  bool skip_sort = IsKeyUnOrdered(options_.mode);
  if (options_.skip_sort) {
    skip_sort = true;  // Forced by user
  }
  buffer->Finish(skip_sort);
  Iterator* const iter = buffer->NewIterator();
  iter->SeekToFirst();
  if (ft != NULL) {
    ft->Reset(buffer->NumEntries());
  }
  for (; iter->Valid(); iter->Next()) {
#ifndef NDEBUG
    num_keys++;
#endif
    if (ft != NULL) {
      ft->AddKey(iter->key());
    }
    bu->Add(iter->key(), iter->value());
    if (!bu->ok()) {
      break;
    }
  }

  if (bu->ok()) {
    // Double checks
    assert(num_keys == buffer->NumEntries());
    // Inject the filter into the table
    Slice fc = (ft != NULL) ? ft->Finish() : Slice();
    bu->EndTable(fc, static_cast<ChunkType>(T::chunk_type()));
#if VERBOSE >= 3
#ifndef NDEBUG
    Verbose(
        __LOG_ARGS__, 3, "\t+ D: %s, I: %s, F: %s",
        PrettySize(stats_.final_data_size - stats.final_data_size).c_str(),
        PrettySize(stats_.final_index_size - stats.final_index_size).c_str(),
        PrettySize(stats_.final_filter_size - stats.final_filter_size).c_str());
#endif
#endif  // VERBOSE
    if (is_epoch_flush) {
      bu->MakeEpoch();
    }
    if (is_final) {
      bu->Finish();
    }
  }

  const uint64_t end = GetCurrentTimeMicros();
  if (options_.listener != NULL) {
    CompactionEvent event;
    event.type = kCompactionEnd;
    event.micros = end;
    event.part = part_;
    options_.listener->OnEvent(kCompactionEnd, &event);
  }
#if VERBOSE >= 3
  Verbose(__LOG_ARGS__, 3, "Compaction done: %d kv pairs (%d us)",
          static_cast<int>(buffer->NumEntries()),
          static_cast<int>(end - start));
#endif

  Status status = bu->status();
  delete iter;
  mu_->Lock();
  bg_status_ = status;
  if (is_forced) {
    num_flush_completed_++;
  }
}

template <typename T>
uint32_t DirIndexer<T>::num_epochs() const {
  mu_->AssertHeld();
  if (opened_) {
    assert(bu_ != NULL);
    return bu_->num_epochs_;
  } else {
    return 0;
  }
}

template <typename T>
size_t DirIndexer<T>::memory_usage() const {
  mu_->AssertHeld();
  if (opened_) {
    size_t result = 0;
    result += buf0_.memory_usage();
    result += buf1_.memory_usage();
    if (filter_ != NULL) result += filter_->memory_usage();
    assert(bu_ != NULL);
    result += bu_->memory_usage();
    return result;
  } else {
    return 0;
  }
}

// Initialize all potential filter templates
template class DirIndexer<BloomBlock>;

template class DirIndexer<BitmapBlock<UncompressedFormat> >;
template class DirIndexer<BitmapBlock<FastVbPlusFormat> >;
template class DirIndexer<BitmapBlock<VbPlusFormat> >;
template class DirIndexer<BitmapBlock<VbFormat> >;
template class DirIndexer<BitmapBlock<FastPfDeltaFormat> >;
template class DirIndexer<BitmapBlock<PfDeltaFormat> >;
template class DirIndexer<BitmapBlock<RoaringFormat> >;

template class DirIndexer<EmptyFilterBlock>;

static Status ReadBlock(LogSource* source, const DirOptions& options,
                        const BlockHandle& handle, BlockContents* result,
                        bool cached = false, uint32_t file_index = 0,
                        char* tmp = NULL, size_t tmp_length = 0) {
  result->data = Slice();
  result->heap_allocated = false;
  result->cachable = false;

  assert(source != NULL);
  size_t n = static_cast<size_t>(handle.size());
  size_t m = n + kBlockTrailerSize;
  char* buf = tmp;
  if (cached) {
    buf = NULL;
  } else if (tmp == NULL || tmp_length < m) {
    buf = new char[m];
  }
  Slice contents;
  Status status = source->Read(handle.offset(), m, &contents, buf, file_index);
  if (status.ok()) {
    if (contents.size() != m) {
      status = Status::Corruption("Truncated block read");
    }
  }
  if (!status.ok()) {
    if (buf != tmp) delete[] buf;
    return status;
  }

  // CRC checks
  const char* data = contents.data();  // Pointer to where read put the data
  if (!options.skip_checksums && options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      if (buf != tmp) delete[] buf;
      status = Status::Corruption("Block checksum mismatch");
      return status;
    }
  }

  if (data[n] == kSnappyCompression) {
    size_t ulen = 0;
    if (!port::Snappy_GetUncompressedLength(data, n, &ulen)) {
      if (buf != tmp) delete[] buf;
      status = Status::Corruption("Cannot compress");
      return status;
    }
    char* ubuf = new char[ulen];
    if (!port::Snappy_Uncompress(data, n, ubuf)) {
      if (buf != tmp) delete[] buf;
      delete[] ubuf;
      status = Status::Corruption("Cannot compress");
      return status;
    }
    if (buf != tmp) {
      delete[] buf;
    }
    result->data = Slice(ubuf, ulen);
    result->heap_allocated = true;
    result->cachable = true;
  } else if (data != buf) {
    // File implementation has given us pointer to some other data.
    // Use it directly under the assumption that it will be live
    // while the file is open.
    if (buf != tmp) {
      delete[] buf;
    }
    result->data = Slice(data, n);
    result->heap_allocated = false;
    result->cachable = false;  // Avoid double cache
  } else {
    result->data = Slice(buf, n);
    result->heap_allocated = (buf != tmp);
    result->cachable = true;
  }

  return status;
}

// Retrieve all keys from a given data block.
Status Dir::Iter(const IterOptions& opts, Slice* input) {
  Status status;
  BlockHandle handle;
  status = handle.DecodeFrom(input);
  if (!status.ok()) {
    return status;
  }
  BlockContents contents;
  status = ReadBlock(data_, options_, handle, &contents, false, opts.file_index,
                     opts.tmp, opts.tmp_length);
  if (!status.ok()) {
    return status;
  } else {
    opts.stats->seeks++;
  }

  Iterator* const iter = OpenDirBlock(options_, contents);
  iter->SeekToFirst();
  for (; iter->Valid(); iter->Next()) {
    opts.saver(opts.arg, iter->key(), iter->value());
    opts.stats->n++;
  }
  if (status.ok()) {
    status = iter->status();
  }

  delete iter;
  return status;
}

// Retrieve all keys from a given table and call "opts.saver" to handle the
// results. Return OK on success and a non-OK status on errors.
Status Dir::Iter(const IterOptions& opts, const TableHandle& h) {
  Status status;
  // Load the index block
  BlockContents index_contents;
  BlockHandle index_handle;
  index_handle.set_offset(h.index_offset());
  index_handle.set_size(h.index_size());
  // We always prefetch and cache all index blocks in memory
  // so there is no need to allocate an additional
  // buffer to store the block contents
  const bool cached = true;
  status = ReadBlock(indx_, options_, index_handle, &index_contents, cached);
  if (!status.ok()) {
    return status;
  } else {
    opts.stats->table_seeks++;
  }

  Block* index_block = new Block(index_contents);
  Iterator* const iter = index_block->NewIterator(BytewiseComparator());
  iter->SeekToFirst();
  for (; iter->Valid(); iter->Next()) {
    Slice input = iter->value();
    status = Iter(opts, &input);
    if (!status.ok()) {
      break;
    }
  }
  if (status.ok()) {
    status = iter->status();
  }

  delete iter;
  delete index_block;
  return status;
}

// Retrieve value to a specific key from a given block whose handle is encoded
// as *input. Call "opts.saver" using the value found and set *found to true.
// Set *exhausted to true if a key larger than the target is seen so there is
// no need to check further. *exhausted is always false if keys are not stored
// ordered. Return OK on success and a non-OK status on errors.
Status Dir::Fetch(const FetchOptions& opts, const Slice& key, Slice* input,
                  bool* found, bool* exhausted) {
  *found = *exhausted = false;
  Status status;
  BlockHandle handle;
  status = handle.DecodeFrom(input);
  if (!status.ok()) {
    return status;
  }
  BlockContents contents;
  status = ReadBlock(data_, options_, handle, &contents, false, opts.file_index,
                     opts.tmp, opts.tmp_length);
  if (!status.ok()) {
    return status;
  } else {
    opts.stats->seeks++;
  }

  Iterator* const iter = OpenDirBlock(options_, contents);
  if (IsKeyUniqueAndOrdered(options_.mode)) {
    iter->Seek(key);  // Binary search
  } else {
    // Keys are non-unique or stored out-of-order.
    // Must start from the beginning
    iter->SeekToFirst();
    if (!IsKeyUnOrdered(options_.mode)) {  // In case keys are in-order
      while (iter->Valid() && key > iter->key()) {
        iter->Next();
      }
    }
  }

  // Additional checks when keys are stored in-order
  if (!IsKeyUnOrdered(options_.mode)) {
    // The target key is strictly larger than all keys in the
    // current block, and is also known to be strictly smaller than
    // all keys in the next block...
    if (!iter->Valid()) {
      // Special case for some target keys
      // that happen to locate in the gap between two
      // adjacent data blocks.
      // This happens when the target key is greater than the
      // largest key in the current block but no greater than the
      // separator that separates the current block
      // and the next block. That is:
      // all keys in the current block < target <= separator < all keys in the
      // next block
      *exhausted = true;
    }
  }

  // Collect all results
  for (; iter->Valid(); iter->Next()) {
    if (iter->key() == key) {  // Hit
      opts.saver(opts.arg, key, iter->value());
      if (IsKeyUnique(options_.mode)) {
        *found = true;
        break;  // Done
      }
    } else if (!IsKeyUnOrdered(options_.mode)) {
      // Keys are stored in-order and we come across a larger key.
      // No more search is needed
      *exhausted = true;
      break;
    }
  }

  if (status.ok()) {
    status = iter->status();
  }

  delete iter;
  return status;
}

// Check if a specific key may or must not exist in one or more blocks
// indexed by the given filter.
bool Dir::KeyMayMatch(const Slice& key, const BlockHandle& h) {
  Status status;
  BlockContents contents;
  // We always prefetch and cache all filter blocks in memory
  // so there is no need to allocate an additional
  // buffer to store the block contents
  const bool cached = true;
  status = ReadBlock(indx_, options_, h, &contents, cached);
  if (status.ok()) {
    bool r;  // False if key must not match so no need for further access
    if (options_.filter == kFtBloomFilter) {
      r = BloomKeyMayMatch(key, contents.data);
    } else if (options_.filter == kFtBitmap) {
      r = BitmapKeyMustMatch(key, contents.data);
    } else {  // Unknown filter type
      r = true;
    }
    if (contents.heap_allocated) {
      delete[] contents.data.data();
    }
    return r;
  } else {
    return true;
  }
}

// Retrieve value to a specific key from a given table and call "opts.saver"
// using the value found. Filter will be consulted if available to avoid
// unnecessary reads. Return OK on success and a non-OK status on errors.
Status Dir::Fetch(const FetchOptions& opts, const Slice& key,
                  const TableHandle& h) {
  Status status;
  // Check table key range and the paired filter
  if (key < h.smallest_key() || key > h.largest_key()) {
    return status;
  } else if (!options_.ignore_filters) {
    BlockHandle filter_handle;
    filter_handle.set_offset(h.filter_offset());
    filter_handle.set_size(h.filter_size());
    if (filter_handle.size() != 0) {  // Filter detected
      if (!KeyMayMatch(key, filter_handle)) {
        // Assuming no false negatives
        return status;
      }
    }
  }

  // Load the index block
  BlockContents index_contents;
  BlockHandle index_handle;
  index_handle.set_offset(h.index_offset());
  index_handle.set_size(h.index_size());
  // We always prefetch and cache all index blocks in memory
  // so there is no need to allocate an additional
  // buffer to store the block contents
  const bool cached = true;
  status = ReadBlock(indx_, options_, index_handle, &index_contents, cached);
  if (!status.ok()) {
    return status;
  } else {
    opts.stats->table_seeks++;
  }

  Block* index_block = new Block(index_contents);
  Iterator* const iter = index_block->NewIterator(BytewiseComparator());
  if (IsKeyUniqueAndOrdered(options_.mode)) {
    iter->Seek(key);  // Binary search
  } else {
    // Keys are non-unique or stored out-of-order.
    // Must start from the beginning
    iter->SeekToFirst();
    if (!IsKeyUnOrdered(options_.mode)) {  // In case keys are in-order
      while (iter->Valid() && key > iter->key()) {
        iter->Next();
      }
    }
  }

  bool found = false;  // True if we found the target key
  // True if a key greater than the target is seen
  bool exhausted = false;
  for (; iter->Valid(); iter->Next()) {
    Slice input = iter->value();
    status = Fetch(opts, key, &input, &found, &exhausted);
    if (!status.ok()) {
      break;
    }
    // Unique?  Ordered?  Found?  Exhausted?
    //   Y        Y         *       *       >> Break (Always check 1 block)
    //   Y        N         Y       *       >> Break
    //   Y        N         N       *       >> Continue
    //   N        Y         *       Y       >> Break
    //   N        Y         *       N       >> Continue
    //   N        N         *       *       >> Continue (Always check all)
    if (IsKeyUniqueAndOrdered(options_.mode)) {
      break;
    } else if (exhausted && !IsKeyUnOrdered(options_.mode)) {
      break;
    } else if (found && IsKeyUnique(options_.mode)) {
      break;
    }
  }

  if (status.ok()) {
    status = iter->status();
  }

  delete iter;
  delete index_block;
  return status;
}

// List all keys within a given directory epoch.
// ListContext *ctx may be shared among multiple concurrent lister threads.
// ListStats *stats is dedicated to the current thread.
// User callback is expected to be thread-safe.
Status Dir::DoList(const BlockHandle& h, uint32_t epoch, ListContext* ctx,
                   ListStats* stats) {
  Status status;
  // Load the meta index for the epoch
  BlockContents meta_index_contents;
  // We always prefetch and cache all index blocks in memory
  // so there is no need to allocate an additional
  // buffer to store the block contents
  const bool cached = true;
  status = ReadBlock(indx_, options_, h, &meta_index_contents, cached);
  if (!status.ok()) {
    return status;
  }
  Block* meta_index_block = new Block(meta_index_contents);
  Iterator* const iter = meta_index_block->NewIterator(BytewiseComparator());
  iter->SeekToFirst();
  std::string epoch_table_key;
  for (uint32_t table = 0;; table++) {
    epoch_table_key = EpochTableKey(epoch, table);
    // Try reusing current iterator position if possible
    if (!iter->Valid() || iter->key() != epoch_table_key) {
      iter->Seek(epoch_table_key);
      if (!iter->Valid()) {
        break;  // EOF
      } else if (iter->key() != epoch_table_key) {
        break;  // No such table
      }
    }
    TableHandle table_handle;
    Slice input = iter->value();
    status = table_handle.DecodeFrom(&input);
    iter->Next();
    if (status.ok()) {
      IterOptions opts;
      if (options_.epoch_log_rotation) {
        opts.file_index = epoch;
      } else {
        opts.file_index = 0;
      }
      opts.stats = stats;
      opts.tmp_length = ctx->tmp_length;
      opts.tmp = ctx->tmp;
      opts.saver = reinterpret_cast<Saver>(ctx->usr_cb);
      opts.arg = ctx->arg_cb;
      status = Iter(opts, table_handle);
      if (!status.ok()) {
        break;
      }
    }
  }

  if (status.ok()) {
    status = iter->status();
  }

  delete iter;
  delete meta_index_block;
  return status;
}

namespace {
struct SaverState {
  std::string* dst;
  bool found;
};

void SaveValue(void* arg, const Slice& key, const Slice& value) {
  SaverState* state = reinterpret_cast<SaverState*>(arg);
  state->dst->append(value.data(), value.size());
  state->found = true;
}

struct ParaSaverState : public SaverState {
  uint32_t epoch;
  std::vector<uint32_t>* offsets;
  std::string* buffer;
  port::Mutex* mu;
};

void ParaSaveValue(void* arg, const Slice& key, const Slice& value) {
  ParaSaverState* state = reinterpret_cast<ParaSaverState*>(arg);
  MutexLock ml(state->mu);
  state->offsets->push_back(static_cast<uint32_t>(state->buffer->size()));
  PutVarint32(state->buffer, state->epoch);
  PutLengthPrefixedSlice(state->buffer, value);
  state->found = true;
}

}  // namespace

// Obtain the value to a specific key within a given directory epoch.
// GetContext *ctx may be shared among multiple concurrent getter threads.
// GetStats *stats is dedicated to the current thread.
// User callback is expected to be thread-safe.
Status Dir::DoGet(const Slice& key, const BlockHandle& h, uint32_t epoch,
                  GetContext* ctx, GetStats* stats) {
  Status status;
  // Load the meta index for the epoch
  BlockContents meta_index_contents;
  // We always prefetch and cache all index blocks in memory
  // so there is no need to allocate an additional
  // buffer to store the block contents
  const bool cached = true;
  status = ReadBlock(indx_, options_, h, &meta_index_contents, cached);
  if (!status.ok()) {
    return status;
  }
  Block* meta_index_block = new Block(meta_index_contents);
  Iterator* const iter = meta_index_block->NewIterator(BytewiseComparator());
  iter->SeekToFirst();
  std::string epoch_table_key;
  uint32_t table = 0;
  for (; status.ok(); table++) {
    epoch_table_key = EpochTableKey(epoch, table);
    // Try reusing current iterator position if possible
    if (!iter->Valid() || iter->key() != epoch_table_key) {
      iter->Seek(epoch_table_key);
      if (!iter->Valid()) {
        break;  // EOF
      } else if (iter->key() != epoch_table_key) {
        break;  // No such table
      }
    }
    ParaSaverState arg;
    arg.epoch = epoch;
    arg.offsets = ctx->offsets;
    arg.buffer = ctx->buffer;
    arg.mu = mu_;
    arg.dst = ctx->dst;
    arg.found = false;
    TableHandle table_handle;
    Slice input = iter->value();
    status = table_handle.DecodeFrom(&input);
    iter->Next();
    if (status.ok()) {
      FetchOptions opts;
      if (options_.epoch_log_rotation) {
        opts.file_index = epoch;
      } else {
        opts.file_index = 0;
      }
      opts.stats = stats;
      opts.tmp_length = ctx->tmp_length;
      opts.tmp = ctx->tmp;
      if (options_.parallel_reads) {
        opts.saver = ParaSaveValue;
        opts.arg = &arg;
        status = Fetch(opts, key, table_handle);
      } else {
        opts.saver = SaveValue;
        opts.arg = &arg;
        status = Fetch(opts, key, table_handle);
      }
      // Each epoch is stored as a set of tables. If we find one match and
      // we know keys are unique, we are done.
      if (status.ok() && arg.found) {
        if (IsKeyUnique(options_.mode)) {
          break;
        }
      }
    }
  }

  if (status.ok()) {
    status = iter->status();
  }

  delete iter;
  delete meta_index_block;
  return status;
}

static inline Iterator* NewRtIterator(Block* block) {
  Iterator* iter = block->NewIterator(BytewiseComparator());
  iter->SeekToFirst();
  return iter;
}

// List all keys within a given directory epoch.
// ListContext *ctx may be shared among multiple concurrent lister threads.
// Return OK on success, or a non-OK status on errors.
void Dir::List(uint32_t epoch, ListContext* ctx) {
  mu_->AssertHeld();
  if (!ctx->status->ok()) {
    return;
  }
  Iterator* rt_iter = ctx->rt_iter;
  if (rt_iter == NULL) {
    rt_iter = NewRtIterator(rt_);
  }
  mu_->Unlock();
  ListStats stats;
  stats.table_seeks = 0;  // Number of tables touched
  // Number of data blocks fetched
  stats.seeks = 0;
  stats.n = 0;
  Status status;
  for (uint32_t dummy = epoch; dummy == epoch; dummy++) {
    std::string epoch_key = EpochKey(epoch);
    // Try reusing current iterator position if possible
    if (!rt_iter->Valid() || rt_iter->key() != epoch_key) {
      rt_iter->Seek(epoch_key);
      if (!rt_iter->Valid()) {
        break;  // EOF
      } else if (rt_iter->key() != epoch_key) {
        break;  // No such epoch
      }
    }
    BlockHandle h;  // Handle to the table index block
    Slice input = rt_iter->value();
    status = h.DecodeFrom(&input);
    rt_iter->Next();
    if (status.ok()) {
      status = DoList(h, epoch, ctx, &stats);
    } else {
      // Skip the epoch
    }
    break;
  }

  if (status.ok()) {
    status = rt_iter->status();
  }

  mu_->Lock();
  if (rt_iter != ctx->rt_iter) {
    delete rt_iter;
  }
  // Increase the total seek count
  ctx->num_table_seeks += stats.table_seeks;
  ctx->num_seeks += stats.seeks;
  ctx->n += stats.n;
  assert(ctx->num_open_lists > 0);
  ctx->num_open_lists--;
  bg_cv_->SignalAll();
  if (ctx->status->ok()) {
    *ctx->status = status;
  }
}

// Obtain the value to a specific key at a given directory epoch.
// GetContext *ctx may be shared among multiple concurrent getter threads.
// Return OK on success, or a non-OK status on errors.
void Dir::Get(const Slice& key, uint32_t epoch, GetContext* ctx) {
  mu_->AssertHeld();
  if (!ctx->status->ok()) {
    return;
  }
  Iterator* rt_iter = ctx->rt_iter;
  if (rt_iter == NULL) {
    rt_iter = NewRtIterator(rt_);
  }
  mu_->Unlock();
  GetStats stats;
  stats.table_seeks = 0;  // Number of tables touched
  // Number of data blocks fetched
  stats.seeks = 0;
  Status status;
  for (uint32_t dummy = epoch; dummy == epoch; dummy++) {
    std::string epoch_key = EpochKey(epoch);
    // Try reusing current iterator position if possible
    if (!rt_iter->Valid() || rt_iter->key() != epoch_key) {
      rt_iter->Seek(epoch_key);
      if (!rt_iter->Valid()) {
        break;  // EOF
      } else if (rt_iter->key() != epoch_key) {
        break;  // No such epoch
      }
    }
    BlockHandle h;
    Slice input = rt_iter->value();
    status = h.DecodeFrom(&input);
    rt_iter->Next();
    if (status.ok()) {
      status = DoGet(key, h, epoch, ctx, &stats);
    } else {
      // Skip the epoch
    }
    break;
  }

  if (status.ok()) {
    status = rt_iter->status();
  }

  mu_->Lock();
  if (rt_iter != ctx->rt_iter) {
    delete rt_iter;
  }
  // Increase the total seek count
  ctx->num_table_seeks += stats.table_seeks;
  ctx->num_seeks += stats.seeks;
  assert(ctx->num_open_reads > 0);
  ctx->num_open_reads--;
  bg_cv_->SignalAll();
  if (ctx->status->ok()) {
    *ctx->status = status;
  }
}

struct Dir::STLLessThan {
  Slice buffer_;

  explicit STLLessThan(const Slice& buffer) : buffer_(buffer) {}

  bool operator()(uint32_t a, uint32_t b) {
    const uint32_t epoch_a = GetEpoch(a);
    const uint32_t epoch_b = GetEpoch(b);
    return epoch_a < epoch_b;
  }

  uint32_t GetEpoch(uint32_t off) {
    uint32_t e = 0;
    const char* p = GetVarint32Ptr(  // Decode epoch number
        buffer_.data() + off, buffer_.data() + buffer_.size(), &e);
    if (p != NULL) {
      return e;
    } else {
      assert(false);
      return e;
    }
  }
};

void Dir::Merge(GetContext* ctx) {
  std::vector<uint32_t>::iterator begin;
  begin = ctx->offsets->begin();
  std::vector<uint32_t>::iterator end;
  end = ctx->offsets->end();
  // A key might appear multiple times within each
  // epoch so the sort must be stable.
  std::stable_sort(begin, end, STLLessThan(*ctx->buffer));

  uint32_t ignored;
  Slice value;
  std::vector<uint32_t>::const_iterator it;
  for (it = ctx->offsets->begin(); it != ctx->offsets->end(); ++it) {
    Slice input = *ctx->buffer;
    input.remove_prefix(*it);
    bool r1 = GetVarint32(&input, &ignored);
    bool r2 = GetLengthPrefixedSlice(&input, &value);
    if (r1 && r2) {
      ctx->dst->append(value.data(), value.size());
    } else {
      assert(false);
    }
  }
}

// Iterate through all keys stored within a given epoch range.
// Return OK on success, or a non-OK status on errors.
Status Dir::Scan(const ScanOptions& opts, ScanStats* stats) {
  mu_->AssertHeld();
  Status status;
  assert(rt_ != NULL);

  ListContext ctx;
  ctx.tmp = opts.tmp;  // User-supplied buffer space
  ctx.tmp_length = opts.tmp_length;
  ctx.num_open_lists = 0;  // Number of outstanding list operations
  ctx.status = &status;
  ctx.num_table_seeks = 0;  // Total number of tables touched
  // Total number of data blocks fetched
  ctx.num_seeks = 0;
  ctx.n = 0;
  if (!options_.parallel_reads) {
    // Pre-create the root iterator for serial reads
    ctx.rt_iter = NewRtIterator(rt_);
  } else {
    ctx.rt_iter = NULL;
  }
  ctx.usr_cb = opts.usr_cb;
  ctx.arg_cb = opts.arg_cb;
  if (num_epoches_ != 0) {
    uint32_t epoch = opts.epoch_start;
    uint32_t epoch_end = std::min(num_epoches_, opts.epoch_end);
    for (; epoch < epoch_end; epoch++) {
      ctx.num_open_lists++;
      BGListItem item;
      item.epoch = epoch;
      item.dir = this;
      item.ctx = &ctx;
      if (opts.force_serial_reads || !options_.parallel_reads) {
        List(item.epoch, item.ctx);
      } else if (options_.reader_pool != NULL) {
        options_.reader_pool->Schedule(Dir::BGList, &item);
      } else if (options_.allow_env_threads) {
        Env::Default()->Schedule(Dir::BGList, &item);
      } else {
        List(item.epoch, item.ctx);
      }
      if (!status.ok()) {
        break;
      }
    }
  }

  // Wait for all outstanding list operations to conclude
  while (ctx.num_open_lists > 0) {
    bg_cv_->Wait();
  }

  delete ctx.rt_iter;
  if (status.ok()) {
    if (stats != NULL) {
      stats->total_table_seeks += ctx.num_table_seeks;
      stats->total_seeks += ctx.num_seeks;
      stats->n += ctx.n;
    }
  }

  return status;
}

// Obtain value to a specific key within a given epoch range.
// Return OK on success, or a non-OK status on errors.
Status Dir::Read(const ReadOptions& opts, const Slice& key, std::string* dst,
                 ReadStats* stats) {
  mu_->AssertHeld();
  Status status;
  assert(rt_ != NULL);
  std::vector<uint32_t> offsets;
  std::string buffer;

  GetContext ctx;
  ctx.tmp = opts.tmp;  // User-supplied buffer space
  ctx.tmp_length = opts.tmp_length;
  ctx.num_open_reads = 0;  // Number of outstanding epoch read operations
  ctx.status = &status;
  ctx.offsets = &offsets;
  ctx.buffer = &buffer;
  ctx.num_table_seeks = 0;  // Total number of tables touched
  // Total number of data blocks fetched
  ctx.num_seeks = 0;
  if (!options_.parallel_reads) {
    // Pre-create the root iterator for serial reads
    ctx.rt_iter = NewRtIterator(rt_);
  } else {
    ctx.rt_iter = NULL;
  }
  ctx.dst = dst;
  if (num_epoches_ != 0) {
    uint32_t epoch = opts.epoch_start;
    uint32_t epoch_end = std::min(num_epoches_, opts.epoch_end);
    for (; epoch < epoch_end; epoch++) {
      ctx.num_open_reads++;
      BGGetItem item;
      item.epoch = epoch;
      item.dir = this;
      item.ctx = &ctx;
      item.key = key;
      if (opts.force_serial_reads || !options_.parallel_reads) {
        Get(item.key, item.epoch, item.ctx);
      } else if (options_.reader_pool != NULL) {
        options_.reader_pool->Schedule(Dir::BGGet, &item);
      } else if (options_.allow_env_threads) {
        Env::Default()->Schedule(Dir::BGGet, &item);
      } else {
        Get(item.key, item.epoch, item.ctx);
      }
      if (!status.ok()) {
        break;
      }
    }
  }

  // Wait for all outstanding read operations to conclude
  while (ctx.num_open_reads > 0) {
    bg_cv_->Wait();
  }

  delete ctx.rt_iter;
  // Merge sort read results
  if (status.ok()) {
    if (stats != NULL) {
      stats->total_table_seeks += ctx.num_table_seeks;
      stats->total_seeks += ctx.num_seeks;
    }
    if (options_.parallel_reads) {
      Merge(&ctx);
    }
  }

  return status;
}

void Dir::BGList(void* arg) {
  BGListItem* item = reinterpret_cast<BGListItem*>(arg);
  MutexLock ml(item->dir->mu_);
  item->dir->List(item->epoch, item->ctx);
}

void Dir::BGGet(void* arg) {
  BGGetItem* item = reinterpret_cast<BGGetItem*>(arg);
  MutexLock ml(item->dir->mu_);
  item->dir->Get(item->key, item->epoch, item->ctx);
}

Dir::ScanOptions::ScanOptions()
    : force_serial_reads(false),
      epoch_start(0),
      epoch_end(~static_cast<uint32_t>(0)),
      usr_cb(NULL),
      arg_cb(NULL),
      tmp_length(0),
      tmp(NULL) {}

Dir::ReadOptions::ReadOptions()
    : force_serial_reads(false),
      epoch_start(0),
      epoch_end(~static_cast<uint32_t>(0)),
      tmp_length(0),
      tmp(NULL) {}

Dir::Dir(const DirOptions& options, port::Mutex* mu, port::CondVar* bg_cv)
    : options_(options),
      num_epoches_(0),
      data_(NULL),
      indx_(NULL),
      mu_(mu),
      bg_cv_(bg_cv),
      rt_(NULL),
      refs_(0) {}

Dir::~Dir() {
  mu_->AssertHeld();
  if (data_ != NULL) data_->Unref();
  if (indx_ != NULL) indx_->Unref();
  delete rt_;
}

void Dir::InstallDataSource(LogSource* data) {
  if (data != data_) {
    if (data_ != NULL) data_->Unref();
    data_ = data;
    if (data_ != NULL) {
      data_->Ref();
    }
  }
}

template <typename U, typename V>
static inline bool UnMatch(U a, V b) {
  return a != static_cast<U>(b);
}

// Double-check if the options supplied by user code match the directory footer
// fetched from the storage. Return OK if verification passes.
static Status VerifyOptions(const DirOptions& options, const Footer& footer) {
  if (UnMatch(options.lg_parts, footer.lg_parts()) ||
      UnMatch(options.num_epochs, footer.num_epochs()) ||
      UnMatch(options.key_size, footer.key_size()) ||
      UnMatch(options.value_size, footer.value_size()) ||
      UnMatch(options.fixed_kv_length, footer.fixed_kv_length()) ||
      UnMatch(options.leveldb_compatible, footer.leveldb_compatible()) ||
      UnMatch(options.epoch_log_rotation, footer.epoch_log_rotation()) ||
      UnMatch(options.skip_checksums, footer.skip_checksums()) ||
      UnMatch(options.filter, footer.filter_type()) ||
      UnMatch(options.mode, footer.mode())) {
    return Status::AssertionFailed("Options does not match footer");
  } else {
    return Status::OK();
  }
}

Status Dir::Open(LogSource* indx) {
  Status status;
  char tmp[Footer::kEncodedLength];
  Slice input;
  if (indx->Size() >= sizeof(tmp)) {
    status = indx->Read(indx->Size() - sizeof(tmp), sizeof(tmp), &input, tmp);
  } else {
    status = Status::Corruption("Dir index too short to be valid");
  }

  if (!status.ok()) {
    return status;
  }

  Footer footer;
  status = footer.DecodeFrom(&input);
  if (!status.ok()) {
    return status;
  } else if (options_.paranoid_checks) {
    status = VerifyOptions(options_, footer);
    if (!status.ok()) {
      return status;
    }
  }

  BlockContents contents;
  const BlockHandle& handle = footer.epoch_index_handle();
  status = ReadBlock(indx, options_, handle, &contents, true);
  if (!status.ok()) {
    return status;
  }

  num_epoches_ = footer.num_epochs();
  if (num_epoches_ != options_.num_epochs)
    Warn(__LOG_ARGS__, "Num epochs does not match dir footer: %u != %u",
         options_.num_epochs, num_epoches_);
  rt_ = new Block(contents);
  indx_ = indx;
  indx_->Ref();

  return status;
}

}  // namespace plfsio
}  // namespace pdlfs
