//
// Created by Ankush J on 9/2/20.
//

#pragma once

//#include "builder.h"
#include "multibuf.h"
#include "ordered_builder.h"

#include <float.h>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include <vector>

namespace pdlfs {
namespace plfsio {
class PartitionManifestWriter;


class Bucket {
 private:
  Range expected_;
  Range observed_;
  const uint32_t max_size_;
  const uint32_t size_per_item_;
  const uint32_t max_items_;
  uint32_t num_items_ = 0;
  uint32_t num_items_oob_ = 0;

  char* data_buffer_;
  size_t data_buffer_idx_ = 0;

  std::string bucket_dir_;
  const int rank_;

 public:
  Bucket(int rank, const char* bucket_dir, const uint32_t max_size,
         const uint32_t size_per_item);

  bool Inside(float prop);

  int Insert(float prop, const char* fname, int fname_len, const char* data,
             int data_len);

  Range GetExpectedRange();

  void UpdateExpectedRange(Range expected);

  void UpdateExpectedRange(float bmin, float bmax);

  void Reset();

  int FlushAndReset(PartitionManifestWriter& manifest);

  ~Bucket();
};

typedef struct PartitionManifestItem {
  uint64_t offset;
  float part_range_begin;
  float part_range_end;
  uint32_t part_item_count;
  uint32_t part_item_oob;
  int bucket_idx;

  bool Overlaps(float point) const {
    return point >= part_range_begin and point <= part_range_end;
  }
  bool Overlaps(float range_begin, float range_end) const {
    return Overlaps(range_begin) or Overlaps(range_end) or
        ((range_begin < part_range_begin) and (range_end > part_range_end));
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

  Slice FinishEpoch();

 public:
  explicit PartitionManifestWriter(WritableFile* dst);
  size_t AddItem(uint64_t offset, float range_begin, float range_end, uint32_t part_count,
                 uint32_t part_oob);
  Status EpochFlush();
  Status Finish();
  int GetRange(float& range_min, float& range_max) const;
  int GetMass(uint64_t& mass_total, uint64_t mass_oob) const;
  size_t Size() const;
  int GetOverLappingEntries(float point, PartitionManifestMatch& match);
  int GetOverLappingEntries(float range_begin, float range_end,
                            PartitionManifestMatch& match);
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

  int UpdateBounds(const float bound_start, const float bound_end);

 private:
  typedef OrderedBlockBuilder<float> BlockBuf;
  PartitionManifestWriter manifest_;

  BlockBuf* membuf_cur_;
  BlockBuf* membuf_prev_;

  /* XXX: needs to be atomic if writing is multithreaded */
  uint32_t bucket_idx_ = 0;

  typedef ArrayBlock Block;
  const DirOptions& options_;
  WritableFile* const dst_;
  port::Mutex mu_;
  port::CondVar bg_cv_;
  const size_t buf_threshold_;  // Threshold for write buffer flush
  // Memory pre-reserved for each write buffer
  size_t buf_reserv_;
  uint64_t offset_;  // Current write offset

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

  static void BGWork(void*);

  BlockBuf** bbs_;
  size_t n_;
};
}  // namespace plfsio
}  // namespace pdlfs
