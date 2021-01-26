//
// Created by Ankush J on 1/22/21.
//

#pragma once

#include "builder.h"
#include "range_writer.h"
#include "types.h"

#define LOG_ERRO 4
#define LOG_WARN 3
#define LOG_INFO 2
#define LOG_DBUG 1

int logf(int lvl, const char* fmt, ...) {
  const char* prefix;
  va_list ap;
  switch (lvl) {
    case 4:
      prefix = "!!! ERROR !!! ";
      break;
    case 3:
      prefix = "-WARNING- ";
      break;
    case 2:
      prefix = "-INFO- ";
      break;
    case 1:
      prefix = "-DEBUG- ";
      break;
    default:
      prefix = "";
      break;
  }
  fprintf(stderr, "%s", prefix);

  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);

  fprintf(stderr, "\n");
  return 0;
}

namespace pdlfs {
namespace plfsio {
class PartitionManifestReader {
 public:
  PartitionManifestReader() = default;

  Status ReadManifest(int rank, Slice& footer_data, uint64_t footer_sz);

  int GetOverLappingEntries(float point, PartitionManifestMatch& match);

  int GetOverLappingEntries(float range_begin, float range_end,
                            PartitionManifestMatch& match);

 private:
  void ReadFooterEpoch(int rank, Slice& data, uint64_t epoch_offset,
                       uint64_t epoch_sz);

  static void ComputeInternalOffsets(const size_t entry_sizes[],
                                     size_t offsets[], int num_entries,
                                     size_t& item_sz) {
    item_sz = 0;
    for (int i = 0; i < num_entries; i++) {
      offsets[i] = item_sz;
      item_sz += entry_sizes[i];
    }
  }

 private:
  std::vector<PartitionManifestItem> items_;
};

struct FileCacheEntry {
  int rank;
  bool is_open;
  RandomAccessFile* fh;
  uint64_t fsz;
};

class CachingDirReader {
 public:
  CachingDirReader(Env* env, int max_cache_size = 512)
      : env_(env), kMaxCacheSz(max_cache_size) {}

  Status ReadDirectory(std::string dir, int& num_ranks);

  Status GetFileHandle(int rank, RandomAccessFile** fh, uint64_t* fsz) {
    if (!cache_[rank].is_open) return Status::NotFound("File not open");

    *fh = cache_[rank].fh;
    *fsz = cache_[rank].fsz;

    return Status::OK();
  }

 private:
  std::string RdbName(const std::string& parent, int rank) {
    char tmp[20];
    snprintf(tmp, sizeof(tmp), "RDB-%08x.tbl", rank);
    return parent + "/" + tmp;
  }

  Env* const env_;
  std::string dir_;
  std::map<int, FileCacheEntry> cache_;
  const int kMaxCacheSz;
};

struct ParsedFooter {
  Slice manifest_data;
  uint32_t num_epochs;
  uint64_t manifest_sz;
  uint64_t key_sz;
  uint64_t val_sz;
};

struct KeyPair {
  float key;
  std::string value;
};

struct KeyPairComparator {
  inline bool operator()(const KeyPair& lhs, const KeyPair& rhs) {
    return lhs.key < rhs.key;
  }
};

class RangeReaderPerfLogger {
 public:
  explicit RangeReaderPerfLogger(Env* env) : env_(env){};

  void RegisterBegin(const char* key) { ts_begin_[key] = env_->NowMicros(); }

  void RegisterEnd(const char* key) { ts_end_[key] = env_->NowMicros(); }

  void PrintStats() {
    std::map<const char*, uint64_t>::iterator it = ts_begin_.begin();

    uint64_t intvl_total = 0;

    for (; it != ts_begin_.end(); it++) {
      const char* key = it->first;
      uint64_t intvl_us = ts_end_[key] - ts_begin_[key];
      intvl_total += intvl_us;

      logf(LOG_INFO, "Event %s: %.2lf ms\n", it->first, intvl_us * 1e-3);
    }
    logf(LOG_INFO, "Event TOTAL: %.2lf ms\n", intvl_total * 1e-3);
  }

 private:
  Env* const env_;
  std::map<const char*, uint64_t> ts_begin_;
  std::map<const char*, uint64_t> ts_end_;
};

class RangeReader {
 public:
  RangeReader(const DirOptions& options)
      : options_(options),
        dir_path_(""),
        reader_(options.env),
        num_ranks_(0),
        logger_(options.env) {}

  Status Read(std::string dir_path);

  Status Query(float rbegin, float rend) {
    logger_.RegisterBegin("SSTREAD");

    PartitionManifestMatch match_obj;
    manifest_reader_.GetOverLappingEntries(rbegin, rend, match_obj);
    logf(LOG_INFO, "Query Match: %llu SSTs found (%llu items)",
         match_obj.items.size(), match_obj.mass_total);

    Slice slice;
    std::string scratch;
    for (uint32_t i = 0; i < match_obj.items.size(); i++) {
      PartitionManifestItem& item = match_obj.items[i];
      logf(LOG_DBUG, "Item Rank: %d, Offset: %llu\n", item.rank, item.offset);
      ReadBlock(item.rank, item.offset, item.part_item_count * 60, slice,
                scratch);
    }

    logger_.RegisterEnd("SSTREAD");

    logger_.RegisterBegin("SORT");
    std::sort(query_results_.begin(), query_results_.end(),
              KeyPairComparator());
    logger_.RegisterEnd("SORT");

    logf(LOG_INFO, "Query Results: %zu elements found\n",
         query_results_.size());

    logger_.PrintStats();

    return Status::OK();
  }

  Status ReadFooter(RandomAccessFile* fh, uint64_t fsz, ParsedFooter& pf);

  void ReadBlock(int rank, uint64_t offset, uint64_t size, Slice& slice,
                 std::string& scratch, bool preview = true) {
    scratch.resize(size);
    RandomAccessFile* src;
    uint64_t src_sz;
    reader_.GetFileHandle(rank, &src, &src_sz);
    src->Read(offset, size, &slice, &scratch[0]);

    uint64_t num_items = size / 60;
    uint64_t vec_off = query_results_.size();

    uint64_t block_offset = 0;
    while (block_offset < size) {
      KeyPair kp;
      kp.key = DecodeFloat32(&slice[block_offset]);
      //      kp.value = std::string(&slice[block_offset + 4], 56);
      kp.value = "";
      query_results_.push_back(kp);

      block_offset += 60;
    }
  }

 private:
  const DirOptions& options_;
  std::string dir_path_;
  CachingDirReader reader_;
  PartitionManifestReader manifest_reader_;
  int num_ranks_;
  std::vector<KeyPair> query_results_;

  RangeReaderPerfLogger logger_;
};
}  // namespace plfsio
}  // namespace pdlfs