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

class RangeReader {
 public:
  RangeReader(const DirOptions& options)
      : options_(options), dir_path_(""), reader_(options.env), num_ranks_(0) {}

  void Read(std::string dir_path);

  Status ReadFooter(RandomAccessFile* fh, uint64_t fsz, ParsedFooter& pf);

  void ReadFirstBlock() {
    Slice s;
    std::string scratch;

    ReadBlock(0, 4194248, s, scratch);

    for (int i = 0; i < 10; i++) {
      printf("data-> %f\n", DecodeFloat32(&s[60 * i]));
    }
  }

  void ReadBlock(uint64_t offset, uint64_t size, Slice& slice,
                 std::string& scratch) {
    scratch.resize(size);
    RandomAccessFile* src;
    uint64_t src_sz;
    reader_.GetFileHandle(0, &src, &src_sz);
    src->Read(offset, size, &slice, &scratch[0]);
  }

 private:
  const DirOptions& options_;
  std::string dir_path_;
  CachingDirReader reader_;
  PartitionManifestReader manifest_reader_;
  int num_ranks_;
};
}  // namespace plfsio
}  // namespace pdlfs