//
// Created by Ankush J on 9/2/20.
//

#pragma once

//#include "builder.h"
#include "coding_float.h"
#include "multibuf.h"
#include "ordered_builder.h"

#include <algorithm>
#include <float.h>
#include <numeric>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include <vector>

namespace pdlfs {
namespace plfsio {
class PartitionManifestWriter;

typedef struct PartitionManifestItem {
  int epoch;
  int rank;
  uint64_t offset;
  Range expected;
  Range observed;
  uint32_t updcnt;
  uint32_t part_item_count;
  uint32_t part_item_oob;

  bool Overlaps(float point) const {
    return expected.Inside(point);
  }

  bool Overlaps(float range_begin, float range_end) const {
    return expected.Overlaps(range_begin, range_end);
  }
} PartitionManifestItem;

typedef struct PartitionManifestMatch {
  std::vector<PartitionManifestItem> items;
  uint64_t mass_total = 0;
  uint64_t mass_oob = 0;
} PartitionManifestMatch;

class PartitionManifestWriter {
 private:
  std::vector<PartitionManifestItem> items_;
  float range_min_;
  float range_max_;

  uint64_t mass_total_;
  uint64_t mass_oob_;

  WritableFile* const dst_;
  bool finished_;
  std::string indexes_;
  uint32_t num_ep_written_;
  // Offset of the previous manifest entry, relative to the Metadata region
  uint64_t off_prev_;

  std::string FinishEpoch();

 public:
  explicit PartitionManifestWriter(WritableFile* dst);
  size_t AddItem(uint64_t offset, OrderedBlockBuilder<float>* sst);
  Status EpochFlush();
  Status Finish();
  int GetRange(float& range_min, float& range_max) const;
  int GetMass(uint64_t& mass_total, uint64_t mass_oob) const;
  size_t Size() const;
  int GetOverLappingEntries(float point, PartitionManifestMatch& match);
  int GetOverLappingEntries(float range_begin, float range_end,
                            PartitionManifestMatch& match);
};

class RangeWriterPerfLogger {
 private:
  typedef struct {
    uint64_t begin;
    uint64_t pre;
    uint64_t post;
    uint64_t end;
  } Elem;
  Env* env_;
  std::vector<uint32_t> compac_hist_;
  std::map<uint32_t, Elem> compac_table_;

 private:
 public:
  RangeWriterPerfLogger(Env* env) : env_(env){};
  void RegisterCompacBegin(uint32_t seq) {
    compac_table_[seq].begin = env_->NowMicros();
  }

  void RegisterCompacPreprocess(uint32_t seq) {
    compac_table_[seq].pre = env_->NowMicros();
  }

  void RegisterCompacPostprocess(uint32_t seq) {
    compac_table_[seq].post = env_->NowMicros();
  }

  void RegisterCompacEnd(uint32_t seq) {
    Elem& e = compac_table_[seq];
    e.end = env_->NowMicros();

    uint64_t preprocess_time = e.pre - e.begin;
    uint64_t wait_time = e.post - e.pre;
    uint64_t postprocess_time = e.end - e.post;
    uint64_t total_time = e.end - e.begin;

    fprintf(stderr, "\nTime: %.2fms %.2fms %.2fms (TID: %p)\n",
            preprocess_time * 1e-3, wait_time * 1e-3, postprocess_time * 1e-3,
            pthread_self());

    compac_hist_.push_back(total_time);
  }

  void Report() {
    uint64_t max_time =
        *std::max_element(compac_hist_.begin(), compac_hist_.end());
    uint64_t min_time =
        *std::min_element(compac_hist_.begin(), compac_hist_.end());
    uint64_t sum_time =
        std::accumulate(compac_hist_.begin(), compac_hist_.end(), 0ull);

    fprintf(stderr, "Stats: %zu items, mean: %.2fms (range: %.2fms-%.2fms)\n",
            compac_hist_.size(), sum_time * 1e-3 / compac_hist_.size(),
            min_time * 1e-3, max_time * 1e-3);
  }
};

class RangeWriter : public MultiBuffering {
 public:
  RangeWriter(const DirOptions& options, WritableFile* dst, size_t buf_size,
              size_t n);

  Status Add(const Slice& k, const Slice& v);

  Status Wait();

  Status EpochFlush();

  Status Flush();

  Status Sync();

  Status Finish();

  ~RangeWriter();

  Status UpdateBounds(const float rmin, const float rmax);

 private:
  RangeWriterPerfLogger logger_;
  bool logging_enabled_;

  typedef OrderedBlockBuilder<float> BlockBuf;
  PartitionManifestWriter manifest_;

  const DirOptions& options_;
  WritableFile* const dst_;
  port::Mutex mu_;
  port::CondVar bg_cv_;
  const size_t buf_threshold_;  // Threshold for write buffer flush
  // Memory pre-reserved for each write buffer
  size_t buf_reserv_;
  uint64_t offset_;  // Current write offset

  BlockBuf** bbs_;  // Array of *ALL* blockbufs, mainly for garbage collection

  friend class MultiBuffering;
  Status Compact(uint32_t seq, void* buf);
  Status SyncBackend(bool close = false);
  Status Close();
  void ScheduleCompaction(uint32_t seq, void* buf);
  void Clear(void* buf) { static_cast<BlockBuf*>(buf)->Reset(); }
  void AddToBuffer(void** buf, const Slice& k, const Slice& v) {
    static_cast<BlockBuf*>(*buf)->Add(k, v);
  }
  bool HasRoom(const void* buf, const Slice& k, const Slice& v) const {
    return (static_cast<const BlockBuf*>(buf)->CurrentSizeEstimate() +
                k.size() + v.size() <=
            buf_threshold_);
  }
  bool IsEmpty(const void* buf) {
    return static_cast<const BlockBuf*>(buf)->empty();
  }

  void CopyBufState(void* buf_prev, void* buf_next) {
    mu_.AssertHeld();
    BlockBuf* bb_prev = static_cast<BlockBuf*>(buf_prev);
    BlockBuf* bb_next = static_cast<BlockBuf*>(buf_next);
    bb_next->CopyFrom(bb_prev);
  }

  static void BGWork(void*);
};
}  // namespace plfsio
}  // namespace pdlfs
