//
// Created by Ankush J on 9/2/20.
//

#include "range_writer.h"

#include "coding_float.h"

#include <assert.h>
#include <limits.h>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include <string>
#include <sys/stat.h>
namespace pdlfs {
namespace plfsio {
PartitionManifestWriter::PartitionManifestWriter(WritableFile* dst)
    : range_min_(FLT_MAX),
      range_max_(FLT_MIN),
      mass_total_(0),
      mass_oob_(0),
      dst_(dst),
      finished_(false),
      num_ep_written_(0),
      off_prev_(0) {}

size_t PartitionManifestWriter::AddItem(uint64_t offset,
                                        OrderedBlockBuilder* sst) {
  assert(sst);

  uint32_t part_count = 0, part_oob = 0;
  sst->GetNumItems(part_count, part_oob);

  if (part_count == 0) return SIZE_MAX;

  size_t item_idx = items_.size();

  PartitionManifestItem item;
  item.epoch = -1;
  item.rank = -1;
  item.offset = offset;
  item.expected = sst->GetExpectedRange();
  item.observed = sst->GetObservedRange();
  item.updcnt = sst->GetUpdateCount();
  item.part_item_count = part_count;
  item.part_item_oob = part_oob;

  items_.push_back(item);

  range_min_ = std::min(range_min_, item.observed.rmin());
  range_max_ = std::max(range_max_, item.observed.rmax());

  mass_total_ += part_count;
  mass_oob_ += part_oob;

  return item_idx;
}

std::string PartitionManifestWriter::FinishEpoch() {
  std::string contents;

  for (size_t i = 0; i < items_.size(); i++) {
    PartitionManifestItem& item = items_[i];
    PutFixed64(&contents, i);
    PutFixed64(&contents, item.offset);
    PutFloat32(&contents, item.expected.rmin());
    PutFloat32(&contents, item.expected.rmax());
    PutFloat32(&contents, item.observed.rmin());
    PutFloat32(&contents, item.observed.rmax());
    PutFixed32(&contents, item.updcnt);
    PutFixed32(&contents, item.part_item_count);
    PutFixed32(&contents, item.part_item_oob);
  }

  items_.clear();
  range_min_ = FLT_MAX;
  range_max_ = FLT_MIN;
  mass_total_ = 0;
  mass_oob_ = 0;

  return contents;
}

Status PartitionManifestWriter::EpochFlush() {
  if (finished_) {
    return Status::AssertionFailed("Already finished");
  }

  std::string manifest_contents = FinishEpoch();
  size_t cur_epoch_sz = manifest_contents.size();

  PutFixed32(&indexes_, num_ep_written_);
  PutFixed64(&indexes_, cur_epoch_sz);

  // 12 = size of prev two Puts
  off_prev_ += cur_epoch_sz + 12;
  ++num_ep_written_;

  if (!manifest_contents.empty()) {
    indexes_.append(manifest_contents);
  }

  return Status::OK();
}

Status PartitionManifestWriter::Finish() {
  if (finished_) {
    return Status::AssertionFailed("Already finished");
  }

  Status status = Status::OK();

  // EpochFlush is called by clients at the beginning of the next epoch
  // so the last one needs to be called manually
  if (Size()) {
    status = EpochFlush();
  }

  if (!status.ok()) return status;

  assert(dst_);
  PutFixed32(&indexes_, num_ep_written_);
  PutFixed64(&indexes_, off_prev_);

  status = dst_->Append(indexes_);
  if (status.ok()) {
    status = dst_->Sync();
  }
  finished_ = true;
  return status;
}

int PartitionManifestWriter::GetOverLappingEntries(
    float point, PartitionManifestMatch& match) {
  for (size_t i = 0; i < items_.size(); i++) {
    if (items_[i].Overlaps(point)) {
      match.items.push_back(items_[i]);
      match.mass_total += items_[i].part_item_count;
      match.mass_oob += items_[i].part_item_oob;
    }
  }

  return 0;
}

int PartitionManifestWriter::GetOverLappingEntries(
    float range_begin, float range_end, PartitionManifestMatch& match) {
  for (size_t i = 0; i < items_.size(); i++) {
    if (items_[i].Overlaps(range_begin, range_end)) {
      match.items.push_back(items_[i]);
      match.mass_total += items_[i].part_item_count;
      match.mass_oob += items_[i].part_item_oob;
    }
  }

  return 0;
}
int PartitionManifestWriter::GetRange(float& range_min,
                                      float& range_max) const {
  range_min = range_min_;
  range_max = range_max_;
  return 0;
}

int PartitionManifestWriter::GetMass(uint64_t& mass_total,
                                     uint64_t mass_oob) const {
  mass_total = mass_total_;
  mass_oob = mass_oob_;
  return 0;
}

size_t PartitionManifestWriter::Size() const { return items_.size(); }

Status RangeWriter::UpdateBounds(const float rmin, const float rmax) {
  MutexLock ml(&mu_);

  Status s = Status::OK();

  uint32_t ignored_compac_seq;
  /* PrepareAll atomically swaps all buffers, but:
   * 1. Their compaction is going on in the background
   * 2. This will block if available double buffers are less than
   * what's needed to swap all
   */
  s = PrepareAll<RangeWriter>(&ignored_compac_seq);

  if (!s.ok()) return s;

#define NUM_SUBPART 1
#define NUM_ACTIVE NUM_SUBPART + 2

#define BUF(i) (reinterpret_cast<BlockBuf*>(bufs_active_[i]))

  /* WARN: i is unsigned, i >= 0 is infinite loop */
  for (size_t i = n_active_ - 1; i >= NUM_SUBPART + 1; i--) {
    BUF(i)->UpdateExpectedRange(BUF(i - 1)->GetExpectedRange());
  }

  Range r0 = BUF(0)->GetExpectedRange();
  Range r1 = BUF(NUM_SUBPART - 1)->GetExpectedRange();

  BUF(NUM_SUBPART)->UpdateExpectedRange(r0.rmin(), r1.rmax());

#define SUBPART(i) (rmin + (i)*rdel)
  float rdel = (rmax - rmin) / NUM_SUBPART;
  for (size_t i = 0; i < NUM_SUBPART; i++) {
    float rx = SUBPART(i);
    float ry = i < NUM_SUBPART ? SUBPART(i + 1) : rmax;
    BUF(i)->UpdateExpectedRange(rx, ry);
  }
  return s;
}

RangeWriter::RangeWriter(const DirOptions& options, WritableFile* dst,
                         size_t buf_size, size_t n)
    : MultiBuffering(&mu_, &bg_cv_, n, NUM_ACTIVE),
      logger_(options.env),
      logging_enabled_(false),
      manifest_(dst),
      options_(options),
      dst_(dst),
      bg_cv_(&mu_),
      buf_threshold_(buf_size),
      buf_reserv_(8 + buf_size),
      offset_(0),
      bbs_(NULL) {
  // XXX : n is ignored for now, fixed subpartitioning
  n_total_ = n_active_ * 2;

  bufs_active_ = new void*[n_active_];
  bbs_ = new BlockBuf*[n_total_];

  for (size_t i = 0; i < n_total_; i++) {
    bbs_[i] = new BlockBuf(options_);
    bbs_[i]->Reserve(buf_reserv_);
    if (i < n_active_) {
      bufs_active_[i] = bbs_[i];
    } else {
      bufs_free_.push_back(bbs_[i]);
    }
  }
}

RangeWriter::~RangeWriter() {
  mu_.Lock();
  while (num_bg_compactions_) {
    bg_cv_.Wait();
  }
  mu_.Unlock();

  delete[] bufs_active_;

  for (size_t i = 0; i < n_total_; i++) {
    delete bbs_[i];
  }
  delete[] bbs_;
}

Status RangeWriter::Add(const Slice& k, const Slice& v) {
  MutexLock ml(&mu_);
  float key_val = DecodeFloat32(k.data());
  void** dest_buf = nullptr;
  for (size_t i = 0; i < n_active_; i++) {
    if (BUF(i)->Inside(key_val)) {
      dest_buf = &bufs_active_[i];
      break;
    }
  }

  if (dest_buf == nullptr) {
    dest_buf = &bufs_active_[n_active_ - 1];
  }

  return __Add<RangeWriter>(reinterpret_cast<void**>(dest_buf), k, v, false);
}

// Wait until there is no outstanding compactions.
// REQUIRES: __Finish() has NOT been called.
// REQUIRES: mu_ has been LOCKed.
Status RangeWriter::Wait() {
  MutexLock ml(&mu_);
  WaitForAny();  // Wait until !num_bg_compactions_
  return bg_status_;
}

Status RangeWriter::EpochFlush() {
  MutexLock ml(&mu_);

  // flush all data, block until over
  // this needs to be synchronous to ensure
  // that the manifest has data
  // XXX: consider making things async wrt manifest
  // metadata if performance bottleneck
  Status s = __Flush<RangeWriter>(true);

  // flush manifest
  manifest_.EpochFlush();

  return s;
}

Status RangeWriter::Sync() {
  MutexLock ml(&mu_);
  return __Sync<RangeWriter>();
}

Status RangeWriter::Flush() {
  MutexLock ml(&mu_);
  return __Flush<RangeWriter>(false);
}

Status RangeWriter::Finish() {
  MutexLock ml(&mu_);
  return __Finish<RangeWriter>();
}

Status RangeWriter::Compact(uint32_t const compac_seq, void* immbuf) {
  mu_.AssertHeld();

  if (logging_enabled_) logger_.RegisterCompacBegin(compac_seq);

  assert(dst_);
  BlockBuf* const bb = static_cast<BlockBuf*>(immbuf);
  // Skip empty buffers
  if (bb->empty() && compac_seq == num_compac_completed_ + 1) {
    return Status::OK();
  }
  mu_.Unlock();  // Unlock as compaction is expensive

  Slice block_contents;
  if (!bb->empty()) {
    block_contents = bb->Finish();
  }

  if (logging_enabled_) logger_.RegisterCompacPreprocess(compac_seq);

  uint64_t offset = 0;

  mu_.Lock();  // All writes are serialized through compac_seq
  assert(num_compac_completed_ < compac_seq);
  while (compac_seq != num_compac_completed_ + 1) {
    bg_cv_.Wait();
  }
  offset = offset_;
  mu_.Unlock();

  manifest_.AddItem(offset, bb);

  //    printf("Compacted: %p @ %u (%.3f to %.3f), %u-%u\n", immbuf, offset_,
  //           buf_range.rmin(), buf_range.rmax(), num_items, num_oob);

  Status status;
  if (!block_contents.empty()) {
    status = dst_->Append(block_contents);
  }

  if (logging_enabled_) logger_.RegisterCompacPostprocess(compac_seq);

  mu_.Lock();

  if (status.ok()) {
    offset_ += block_contents.size();
  }

  if (status.ok()) {
    status = dst_->Flush();
  }

  if (logging_enabled_) logger_.RegisterCompacEnd(compac_seq);

  return status;
}

// REQUIRES: no outstanding background compactions.
// REQUIRES: mu_ has been LOCKed.
Status RangeWriter::Close() {
  assert(!num_bg_compactions_);
  mu_.AssertHeld();
  assert(dst_);

  Status status = manifest_.Finish();
  if (!status.ok()) return status;

  std::string footer;
  PutFixed64(&footer, options_.key_size);
  PutFixed64(&footer, options_.value_size);

  status = dst_->Append(footer);
  if (!status.ok()) return status;

  status = dst_->Sync();
  if (!status.ok()) return status;

  status = dst_->Close();
  if (!status.ok()) return status;

  if (logging_enabled_) logger_.Report();

  return status;
}

// REQUIRES: no outstanding background compactions.
// REQUIRES: mu_ has been LOCKed.
Status RangeWriter::SyncBackend() {
  assert(!num_bg_compactions_);
  mu_.AssertHeld();
  assert(dst_);
  return dst_->Sync();
}

namespace {  // State for each compaction
struct State {
  RangeWriter* writer;
  uint32_t compac_seq;
  void* immbuf;
};
}  // namespace

// REQUIRES: mu_ has been LOCKed.
void RangeWriter::ScheduleCompaction(uint32_t const compac_seq, void* immbuf) {
  mu_.AssertHeld();

  assert(num_bg_compactions_);

  State* s = new State;
  s->compac_seq = compac_seq;
  s->immbuf = immbuf;
  s->writer = this;

  if (options_.compaction_pool) {
    options_.compaction_pool->Schedule(RangeWriter::BGWork, s);
  } else if (options_.allow_env_threads) {
    Env::Default()->Schedule(RangeWriter::BGWork, s);
  } else {
    DoCompaction<RangeWriter>(compac_seq, immbuf);
    delete s;
  }
}

void RangeWriter::BGWork(void* arg) {
  State* const s = reinterpret_cast<State*>(arg);
  MutexLock ml(&s->writer->mu_);
  s->writer->DoCompaction<RangeWriter>(s->compac_seq, s->immbuf);
  delete s;
}

}  // namespace plfsio
}  // namespace pdlfs
