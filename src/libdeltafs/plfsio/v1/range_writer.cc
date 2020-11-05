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

size_t PartitionManifestWriter::AddItem(uint64_t offset, float range_begin,
                                        float range_end, uint32_t part_count,
                                        uint32_t part_oob) {
  if (part_count == 0) return SIZE_MAX;

  size_t item_idx = items_.size();

  items_.push_back(
      {offset, range_begin, range_end, part_count, part_oob, (int)item_idx});

  range_min_ = std::min(range_min_, range_begin);
  range_max_ = std::max(range_max_, range_end);

  mass_total_ += part_count;
  mass_oob_ += part_oob;

  return item_idx;
}

Slice PartitionManifestWriter::FinishEpoch() {
  std::string contents;

  for (size_t i = 0; i < items_.size(); i++) {
    PartitionManifestItem& item = items_[i];
    PutFixed64(&contents, i);
    PutFixed64(&contents, item.offset);
    PutFloat32(&contents, item.part_range_begin);
    PutFloat32(&contents, item.part_range_end);
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

  PutFixed32(&indexes_, num_ep_written_);
  PutFixed64(&indexes_, off_prev_);
  ++num_ep_written_;

  Slice manifest_contents = FinishEpoch();
  if (!manifest_contents.empty()) {
    indexes_.append(manifest_contents.data(), manifest_contents.size());
  }

  off_prev_ += manifest_contents.size() + 12;

  return Status::OK();
}

Status PartitionManifestWriter::Finish() {
  if (finished_) {
    return Status::AssertionFailed("Already finished");
  }
  assert(dst_);
  PutFixed32(&indexes_, num_ep_written_);
  PutFixed64(&indexes_, off_prev_);
  Status status = dst_->Append(indexes_);
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

int RangeWriter::UpdateBounds(const float bound_start, const float bound_end) {
  MutexLock ml(&mu_);

  /* Strictly, should lock before updating, but this is only for measuring
   * "pollution" - who cares if it's a couple of counters off */
  membuf_prev_->UpdateExpectedRange(membuf_cur_->GetExpectedRange());
  membuf_cur_->UpdateExpectedRange(bound_start, bound_end);

  uint32_t ignored_compac_seq;
  // XXX: disabled flushing prev_, assuming that it will reduce metadata items
  // without significantly affecting overlaps
  //  Prepare<RangeWriter>(reinterpret_cast<void**>(&membuf_prev_),
  //  &ignored_compac_seq);
  Prepare<RangeWriter>(reinterpret_cast<void**>(&membuf_cur_),
                       &ignored_compac_seq);

  return 0;
}

RangeWriter::RangeWriter(const DirOptions& options, WritableFile* dst,
                         size_t buf_size, size_t n)
    : MultiBuffering(&mu_, &bg_cv_),
      logger_(options.env),
      logging_enabled_(true),
      manifest_(dst),
      membuf_cur_(NULL),
      membuf_prev_(NULL),
      options_(options),
      dst_(dst),
      bg_cv_(&mu_),
      buf_threshold_(buf_size),
      buf_reserv_(8 + buf_size),
      offset_(0),
      bbs_(NULL),
      n_(n) {
  if (n_ < 3) n_ = 3;

  bbs_ = new BlockBuf*[n_];
  for (size_t i = 0; i < n_; i++) {
    bbs_[i] = new BlockBuf(options_);
    bbs_[i]->Reserve(buf_reserv_);
    if (i > 1) bufs_.push_back(bbs_[i]);
  }

  membuf_cur_ = bbs_[0];
  membuf_prev_ = bbs_[1];
}

RangeWriter::~RangeWriter() {
  mu_.Lock();
  while (num_bg_compactions_) {
    bg_cv_.Wait();
  }
  mu_.Unlock();

  for (size_t i = 0; i < n_; i++) {
    delete bbs_[i];
  }
  delete[] bbs_;
}

Status RangeWriter::Add(const Slice& k, const Slice& v) {
  MutexLock ml(&mu_);
  float key_val = DecodeFloat32(k.data());
  BlockBuf** dest_buf =
      membuf_cur_->Inside(key_val) ? &membuf_cur_ : &membuf_prev_;
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
  Status s = __Flush<RangeWriter>(false);

  // flush manifest
  manifest_.EpochFlush();

  return s;
}

Status RangeWriter::Sync() {
  MutexLock ml(&mu_);
  return __Sync<RangeWriter>(false);
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

  mu_.Lock();  // All writes are serialized through compac_seq
  assert(num_compac_completed_ < compac_seq);
  while (compac_seq != num_compac_completed_ + 1) {
    bg_cv_.Wait();
  }

  // append the data and metadata atomically, under lock
  Range buf_range = bb->GetObservedRange();
  uint32_t num_items = 0, num_oob = 0;
  bb->GetWriteStats(num_items, num_oob);
  manifest_.AddItem(offset_, buf_range.range_min, buf_range.range_max,
                    num_items, num_oob);

//  printf("Compacted: %p @ %u (%.3f to %.3f), %u-%u\n", immbuf, offset_,
//         buf_range.range_min, buf_range.range_max, num_items, num_oob);


  if (logging_enabled_) logger_.RegisterCompacPreprocess(compac_seq);

  Status status;
  if (!block_contents.empty()) {
    status = dst_->Append(block_contents);
  }

  if (logging_enabled_) logger_.RegisterCompacPostprocess(compac_seq);

  if (status.ok()) {
    offset_ += block_contents.size();
  }

  mu_.Unlock();
  if (status.ok()) {
    status = dst_->Flush();
  }
  mu_.Lock();

  if (logging_enabled_) logger_.RegisterCompacEnd(compac_seq);

  return status;
}

// REQUIRES: no outstanding background compactions.
// REQUIRES: mu_ has been LOCKed.
Status RangeWriter::Close() {
  assert(!num_bg_compactions_);
  mu_.AssertHeld();
  assert(dst_);

  // Manifest has the footer
  Status status = manifest_.Finish();

  if (status.ok()) {
    status = dst_->Sync();
    dst_->Close();
  }

  if (logging_enabled_) logger_.Report();

  return status;
}

// REQUIRES: no outstanding background compactions.
// REQUIRES: mu_ has been LOCKed.
Status RangeWriter::SyncBackend(bool close) {
  assert(!num_bg_compactions_);
  mu_.AssertHeld();
  assert(dst_);
  if (!close) {
    return dst_->Sync();
  } else {
    return Close();
  }
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

  uint64_t sched_begin = options_.env->NowMicros();

  if (options_.compaction_pool) {
    options_.compaction_pool->Schedule(RangeWriter::BGWork, s);
  } else if (options_.allow_env_threads) {
    Env::Default()->Schedule(RangeWriter::BGWork, s);
  } else {
    DoCompaction<RangeWriter>(compac_seq, immbuf);
    delete s;
  }

  uint64_t sched_end = options_.env->NowMicros();
  printf("\nCompaction Time: %fms\n", (sched_end - sched_begin) * 1e-3);
}

void RangeWriter::BGWork(void* arg) {
  State* const s = reinterpret_cast<State*>(arg);
  MutexLock ml(&s->writer->mu_);
  s->writer->DoCompaction<RangeWriter>(s->compac_seq, s->immbuf);
  delete s;
}

}  // namespace plfsio
}  // namespace pdlfs
