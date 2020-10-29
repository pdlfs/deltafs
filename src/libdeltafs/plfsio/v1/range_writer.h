//
// Created by Ankush J on 9/2/20.
//

#pragma once

#include "builder.h"
#include "doublebuf.h"
//#include "ordered_builder.h"

#include <float.h>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include <vector>

namespace pdlfs {
namespace plfsio {
class PartitionManifest;

struct Range {
  float range_min = FLT_MAX;
  float range_max = FLT_MIN;

  Range& operator=(Range& r) {
    range_min = r.range_min;
    range_max = r.range_max;
    return *this;
  }

  void Reset() {
    range_min = FLT_MAX;
    range_max = FLT_MIN;
  }

  bool Inside(float f) const { return (f >= range_min && f <= range_max); }

  void Extend(float f) {
    range_min = std::min(range_min, f);
    range_max = std::max(range_min, f);
  }
};

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

  int FlushAndReset(PartitionManifest& manifest);

  ~Bucket();
};

typedef struct PartitionManifestItem {
  float part_range_begin;
  float part_range_end;
  uint32_t part_item_count;
  uint32_t part_item_oob;
  int bucket_idx;
  int rank;

  bool Overlaps(float point) const;
  bool Overlaps(float range_begin, float range_end) const;
} PartitionManifestItem;

typedef struct PartitionManifestMatch {
  std::vector<PartitionManifestItem> items;
  uint64_t mass_total = 0;
  uint64_t mass_oob = 0;
} PartitionManifestMatch;

class PartitionManifest {
 private:
  std::vector<PartitionManifestItem> items_;
  float range_min_ = FLT_MAX;
  float range_max_ = FLT_MIN;

  uint64_t mass_total_ = 0;
  uint64_t mass_oob_ = 0;

 public:
  PartitionManifest();
  size_t AddItem(float range_begin, float range_end, uint32_t part_count,
                 uint32_t part_oob, int rank = -1);
  int WriteToDisk(FILE* out_file);
  int GetRange(float& range_min, float& range_max) const;
  int GetMass(uint64_t& mass_total, uint64_t mass_oob) const;
  size_t Size() const;
  int PopulateFromDisk(const std::string& disk_path, int rank);
  int GetOverLappingEntries(float point, PartitionManifestMatch& match);
  int GetOverLappingEntries(float range_begin, float range_end,
                            PartitionManifestMatch& match);
};

class RangeWriter : public DoubleBuffering {
 public:
  RangeWriter(int rank, const char* dirpath, uint32_t memtable_size,
              uint32_t key_size);
  int UpdateBounds(float bound_start, float bound_end);
  std::string GetManifestDir();
  int Write(const char* fname, int fname_len, const char* data, int data_len);
  //  int Finish();

  /* DeltaFS-compliant interface */
  RangeWriter(const DirOptions& options, WritableFile* dst, size_t buf_size,
              size_t n);

  Status Add(const Slice& k, const Slice& v);

  Status Wait();

  Status EpochFlush();

  Status Flush();

  Status Sync();

  Status Finish();

  ~RangeWriter();

 private:
//  const int rank_;
  std::string dirpath_;
  uint32_t memtable_size_;
  uint32_t key_size_;
  uint32_t items_per_flush_;

//  Bucket current_;
//  Bucket prev_;

  std::string manifest_path_;
  std::string manifest_bin_path_;
  PartitionManifest manifest_;

  /* XXX: needs to be atomic if writing is multithreaded */
  uint32_t bucket_idx_ = 0;

//  typedef OrderedBlockBuilder<float> BlockBuf;
  typedef ArrayBlockBuilder BlockBuf;
    typedef ArrayBlock Block;
  const DirOptions& options_;
  WritableFile* const dst_;
  port::Mutex mu_;
  port::CondVar bg_cv_;
  const size_t buf_threshold_;  // Threshold for write buffer flush
  // Memory pre-reserved for each write buffer
  size_t buf_reserv_;
  uint64_t offset_;  // Current write offset

  friend class DoubleBuffering;
  Status Compact(uint32_t seq, void* buf);
  Status SyncBackend(bool close = false);
  Status Close();
  void ScheduleCompaction(uint32_t seq, void* buf);
  void Clear(void* buf) {
    printf("calling reset: %p\n", buf);
    static_cast<BlockBuf*>(buf)->Reset();
  }
  void AddToBuffer(void* buf, const Slice& k, const Slice& v) {
    static_cast<BlockBuf*>(buf)->Add(k, v);
  }
  bool HasRoom(const void* buf, const Slice& k, const Slice& v) {
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

  int WriteManifestToDisk(const char* path);
  int FlushAndReset(Bucket& bucket);
};
}  // namespace plfsio
}  // namespace pdlfs
