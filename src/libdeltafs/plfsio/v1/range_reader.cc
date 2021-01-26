//
// Created by Ankush J on 1/22/21.
//

#include "range_reader.h"

namespace pdlfs {
namespace plfsio {

void PartitionManifestReader::ReadFooterEpoch(int rank, Slice& data,
                                              const uint64_t epoch_offset,
                                              const uint64_t epoch_sz) {
  /* | ENTRY | ENTRY | ENTRY | ...
   * ENTRY = [IDX:8B | OFFSET:8B | RBEG:4B | REND:4B | ICNT: 4B | IOOB: 4B]
   */

  static const size_t entry_sizes[] = {sizeof(uint64_t), sizeof(uint64_t),
                                       sizeof(float),    sizeof(float),
                                       sizeof(uint32_t), sizeof(uint32_t)};

  static const int num_entries = sizeof(entry_sizes) / sizeof(size_t);

  static size_t offsets[num_entries];
  static size_t item_sz;

  ComputeInternalOffsets(entry_sizes, offsets, num_entries, item_sz);

  int num_items = epoch_sz / item_sz;

  uint64_t cur_offset = epoch_offset;

  for (int i = 0; i < num_items; i++) {
    PartitionManifestItem item;
    assert(i == DecodeFixed64(&data[cur_offset + offsets[0]]));

    item.rank = rank;
    item.offset = DecodeFixed64(&data[cur_offset + offsets[1]]);
    item.part_range_begin = DecodeFloat32(&data[cur_offset + offsets[2]]);
    item.part_range_end = DecodeFloat32(&data[cur_offset + offsets[3]]);
    item.part_item_count = DecodeFixed32(&data[cur_offset + offsets[4]]);
    item.part_item_oob = DecodeFixed32(&data[cur_offset + offsets[5]]);

    printf("%llu %.3f %.3f %u %u\n", item.offset, item.part_range_begin,
           item.part_range_end, item.part_item_count, item.part_item_oob);

    items_.push_back(item);

    mass_total_ += item.part_item_count;

    cur_offset += item_sz;
  }
}
Status PartitionManifestReader::ReadManifest(int rank, Slice& footer_data,
                                             const uint64_t footer_sz) {
  uint64_t epoch_offset = 0;
  Status s;

  while (epoch_offset < footer_sz) {
    uint32_t num_ep_written = DecodeFixed32(&footer_data[epoch_offset]);
    uint64_t off_prev =
        DecodeFixed64(&footer_data[epoch_offset + sizeof(uint32_t)]);
    printf("%u %llu\n", num_ep_written, off_prev);

    ReadFooterEpoch(rank, footer_data, epoch_offset + 12, off_prev);

    epoch_offset += off_prev + 12;
  }

  return s;
}

int PartitionManifestReader::GetOverLappingEntries(
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

int PartitionManifestReader::GetOverLappingEntries(
    float range_begin, float range_end, PartitionManifestMatch& match) {
  for (size_t i = 0; i < items_.size(); i++) {
    if (items_[i].Overlaps(range_begin, range_end)) {
      match.items.push_back(items_[i]);
      match.mass_total += items_[i].part_item_count;
      match.mass_oob += items_[i].part_item_oob;
    }
  }

  logf(LOG_INFO, "Query Selectivity: %.4f %%\n", match.mass_total * 1.0 / mass_total_);

  return 0;
}

Status CachingDirReader::ReadDirectory(std::string dir, int& num_ranks) {
  dir_ = dir;
  uint64_t fsz;

  for (int rank = 0;; rank++) {
    std::string fname = RdbName(dir_, rank);
    bool file_exists = env_->FileExists(fname.c_str());
    if (!file_exists) break;

    Status s = env_->GetFileSize(fname.c_str(), &fsz);
    if (!s.ok()) continue;

    logf(LOG_DBUG, "File: %s, size: %u\n", fname.c_str(), fsz);

    RandomAccessFile* fp;
    s = env_->NewRandomAccessFile(fname.c_str(), &fp);
    if (!s.ok()) continue;

    cache_[rank] = {rank, true, fp, fsz};
  }

  logf(LOG_INFO, "%u files found.\n", cache_.size());
  num_ranks = cache_.size();

  return Status::OK();
}

Status RangeReader::Read(std::string dir_path) {
  logger_.RegisterBegin("MFREAD");

  dir_path_ = dir_path;
  reader_.ReadDirectory(dir_path_, num_ranks_);

  RandomAccessFile* src;
  uint64_t src_sz;
  ParsedFooter pf;

  for (int rank = 0; rank < num_ranks_; rank++) {
    reader_.GetFileHandle(rank, &src, &src_sz);
    ReadFooter(src, src_sz, pf);
    manifest_reader_.ReadManifest(rank, pf.manifest_data, pf.manifest_sz);
  }

  logger_.RegisterEnd("MFREAD");

  return Status::OK();
}

Status RangeReader::ReadFooter(RandomAccessFile* fh, uint64_t fsz,
                               ParsedFooter& pf) {
  static const uint64_t footer_sz = 28;
  Slice s;
  std::string scratch;
  scratch.resize(footer_sz);
  Status status = fh->Read(fsz - footer_sz, footer_sz, &s, &scratch[0]);

  pf.num_epochs = DecodeFixed32(&s[0]);
  pf.manifest_sz = DecodeFixed64(&s[4]);
  pf.key_sz = DecodeFixed64(&s[12]);
  pf.val_sz = DecodeFixed64(&s[20]);

  logf(LOG_DBUG, "Footer: %u %llu %llu %llu\n", pf.num_epochs, pf.manifest_sz,
       pf.key_sz, pf.val_sz);

  scratch.resize(pf.manifest_sz);
  status = fh->Read(fsz - pf.manifest_sz - footer_sz, pf.manifest_sz,
                    &pf.manifest_data, &scratch[0]);

  return status;
}
}  // namespace plfsio
}  // namespace pdlfs
