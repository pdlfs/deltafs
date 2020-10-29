//
// Created by Ankush J on 9/2/20.
//

#include "range_writer.h"

#include <assert.h>
#include <limits.h>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include <string>
#include <sys/stat.h>

namespace {
float get_indexable_property(const char* data_buf) {
  const float* prop = reinterpret_cast<const float*>(data_buf);
  return prop[0];
}

}  // namespace

namespace pdlfs {
namespace plfsio {
Bucket::Bucket(int rank, const char* bucket_dir, const uint32_t max_size,
               const uint32_t size_per_item)
    : rank_(rank),
      max_size_(max_size),
      size_per_item_(size_per_item),
      max_items_(max_size_ / size_per_item_) {
  bucket_dir_ = bucket_dir;
  bucket_dir_ += "/buckets";
  data_buffer_ = new char[max_size_];
};

bool Bucket::Inside(float prop) { return expected_.Inside(prop); }

int Bucket::Insert(float prop, const char* fname, int fname_len,
                   const char* data, int data_len) {
  int rv = 0;

  assert(fname_len + data_len == size_per_item_);
  if (num_items_ >= max_items_) return -1;

  observed_.Extend(prop);
  memcpy(&data_buffer_[data_buffer_idx_], fname, fname_len);
  memcpy(&data_buffer_[data_buffer_idx_ + fname_len], data, data_len);
  data_buffer_idx_ += fname_len + data_len;
  num_items_++;

  if (not expected_.Inside(prop)) {
    num_items_oob_++;
  }

  return 0;
}

Range Bucket::GetExpectedRange() { return expected_; }

void Bucket::UpdateExpectedRange(Range expected) { expected_ = expected; }

void Bucket::UpdateExpectedRange(float bmin, float bmax) {
  assert(bmin <= bmax);

  expected_.range_min = bmin;
  expected_.range_max = bmax;
}

void Bucket::Reset() {
  num_items_ = 0;
  num_items_oob_ = 0;
  data_buffer_idx_ = 0;
  observed_.Reset();
}

int Bucket::FlushAndReset(PartitionManifest& manifest) {
  int rv = 0;

  size_t bidx = manifest.AddItem(observed_.range_min, observed_.range_max,
                                 num_items_, num_items_oob_);

  if (bidx < SIZE_MAX) {
    std::stringstream bucket_path;
    bucket_path << bucket_dir_ << "/bucket." << rank_ << '.' << bidx;
    FILE* bfile = fopen(bucket_path.str().c_str(), "wb+");
    fwrite(data_buffer_, size_per_item_, num_items_, bfile);
    fclose(bfile);
  }

  Reset();

  return rv;
}

Bucket::~Bucket() { delete[] data_buffer_; }

bool PartitionManifestItem::Overlaps(float point) const {
  return point >= part_range_begin and point <= part_range_end;
}

bool PartitionManifestItem::Overlaps(float range_begin, float range_end) const {
  return Overlaps(range_begin) or Overlaps(range_end) or
         ((range_begin < part_range_begin) and (range_end > part_range_end));
}

PartitionManifest::PartitionManifest() = default;

size_t PartitionManifest::AddItem(float range_begin, float range_end,
                                  uint32_t part_count, uint32_t part_oob,
                                  int rank) {
  if (part_count == 0) return SIZE_MAX;

  size_t item_idx = items_.size();

  items_.push_back(
      {range_begin, range_end, part_count, part_oob, (int)item_idx, rank});

  range_min_ = std::min(range_min_, range_begin);
  range_max_ = std::max(range_max_, range_end);

  mass_total_ += part_count;
  mass_oob_ += part_oob;

  return item_idx;
}

int PartitionManifest::WriteToDisk(FILE* out_file) {
  int rv = 0;

  for (size_t idx = 0; idx < items_.size(); idx++) {
    PartitionManifestItem& item = items_[idx];
    fprintf(out_file, "%.4f %.4f - %u %u\n", item.part_range_begin,
            item.part_range_end, item.part_item_count, item.part_item_oob);
  }
  return rv;
}

int PartitionManifest::PopulateFromDisk(const std::string& disk_path,
                                        int rank) {
  FILE* f = fopen(disk_path.c_str(), "r");
  if (!f) return -1;

  float range_begin;
  float range_end;
  uint32_t size;
  uint32_t size_oob;

  size_t count_total = 0;
  size_t count_returned = 0;

  while (fscanf(f, "%f %f - %d %d\n", &range_begin, &range_end, &size,
                &size_oob) != EOF) {
    if (size) {
      count_returned = AddItem(range_begin, range_end, size, size_oob, rank);
      count_total++;
    }
  }

  fclose(f);

  return (int)count_total;
}

int PartitionManifest::GetOverLappingEntries(float point,
                                             PartitionManifestMatch& match) {
  for (size_t i = 0; i < items_.size(); i++) {
    if (items_[i].Overlaps(point)) {
      match.items.push_back(items_[i]);
      match.mass_total += items_[i].part_item_count;
      match.mass_oob += items_[i].part_item_oob;
    }
  }

  return 0;
}

int PartitionManifest::GetOverLappingEntries(float range_begin, float range_end,
                                             PartitionManifestMatch& match) {
  for (size_t i = 0; i < items_.size(); i++) {
    if (items_[i].Overlaps(range_begin, range_end)) {
      match.items.push_back(items_[i]);
      match.mass_total += items_[i].part_item_count;
      match.mass_oob += items_[i].part_item_oob;
    }
  }

  return 0;
}
int PartitionManifest::GetRange(float& range_min, float& range_max) const {
  range_min = range_min_;
  range_max = range_max_;
  return 0;
}

int PartitionManifest::GetMass(uint64_t& mass_total, uint64_t mass_oob) const {
  mass_total = mass_total_;
  mass_oob = mass_oob_;
  return 0;
}

size_t PartitionManifest::Size() const { return items_.size(); }

// RangeWriter::RangeWriter(int rank, const char* dirpath,
//                         uint32_t memtable_size_bytes, uint32_t
//                         key_size_bytes)
//    : rank_(rank),
//      dirpath_(dirpath),
//      memtable_size_(memtable_size_bytes),
//      key_size_(key_size_bytes),
//      items_per_flush_(memtable_size_ / key_size_),
//      current_(rank, dirpath, memtable_size_, key_size_),
//      prev_(rank, dirpath, memtable_size_, key_size_) {
//  std::string man_dirpath = dirpath_ + "/manifests";
//  mkdir(man_dirpath.c_str(), S_IRWXU);
//
//  std::string bucket_dirpath = dirpath_ + "/buckets";
//  mkdir(bucket_dirpath.c_str(), S_IRWXU);
//
//  std::stringstream man_path;
//  man_path << man_dirpath << '/' << "vpic-manifest." << rank;
//  manifest_path_ = man_path.str();
//
//  std::stringstream man_bin_path;
//  man_bin_path << man_dirpath << '/' << "vpic-manifest.bin." << rank;
//  manifest_bin_path_ = man_bin_path.str();
//}

//int RangeWriter::Write(const char* fname, int fname_len, const char* data,
//                       int data_len) {
//  int rv = 0;
//
//  float indexed_prop = ::get_indexable_property(data);
//  if (current_.Inside(indexed_prop)) {
//    rv = current_.Insert(indexed_prop, fname, fname_len, data, data_len);
//
//    if (rv) rv = FlushAndReset(current_);
//  } else {
//    rv = prev_.Insert(indexed_prop, fname, fname_len, data, data_len);
//
//    if (rv) rv = FlushAndReset(prev_);
//  }
//
//  return rv;
//}

int RangeWriter::FlushAndReset(Bucket& bucket) {
  int rv = 0;

  rv = bucket.FlushAndReset(manifest_);

  return rv;
}

//int RangeWriter::UpdateBounds(const float bound_start, const float bound_end) {
//  /* Strictly, should lock before updating, but this is only for measuring
//   * "pollution" - who cares if it's a couple of counters off */
//  prev_.UpdateExpectedRange(current_.GetExpectedRange());
//  current_.UpdateExpectedRange(bound_start, bound_end);
//
//  /* XXX: disabled flushing prev_, assuming that it will reduce bucket count
//   * without significantly affecting overlaps */
//  // FlushAndReset(prev_);
//  FlushAndReset(current_);
//
//  return 0;
//}

int RangeWriter::WriteManifestToDisk(const char* path) {
  int rv = 0;
  FILE* out_file = fopen(path, "w+");
  manifest_.WriteToDisk(out_file);
  fclose(out_file);
  return rv;
}

std::string RangeWriter::GetManifestDir() {
  return manifest_path_.substr(0, manifest_path_.find_last_of('/'));
}

RangeWriter::RangeWriter(const DirOptions& options, WritableFile* dst,
                         size_t buf_size, size_t n)
    : DoubleBuffering(&mu_, &bg_cv_),
      options_(options),
      dst_(dst),
      bg_cv_(&mu_),
      buf_threshold_(buf_size),
      buf_reserv_(8 + buf_size),
      offset_(0),
      bbs_(NULL),
      n_(n) {
  if (n_ < 2) n_ = 2;

  bbs_ = new BlockBuf*[n_];
  for (size_t i = 0; i < n_; i++) {
    bbs_[i] = new BlockBuf(options_);
    bbs_[i]->Reserve(buf_reserv_);
    if (i != 0) bufs_.push_back(bbs_[i]);
  }

  membuf_ = bufs_[0];
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
  return __Add<RangeWriter>(k, v, false);
}

Status RangeWriter::Wait() {
  MutexLock ml(&mu_);
  return __Wait();
}

Status RangeWriter::EpochFlush() { return Flush(); }

Status RangeWriter::Sync() {
  MutexLock ml(&mu_);
  return __Sync<RangeWriter>(false);
}

Status RangeWriter::Flush() {
  MutexLock ml(&mu_);
  return __Flush<RangeWriter>(false);
}

Status RangeWriter::Finish() {
//  FlushAndReset(prev_);
//  FlushAndReset(current_);

//  WriteManifestToDisk(manifest_path_.c_str());
  MutexLock ml(&mu_);
  return __Finish<RangeWriter>();
}

Status RangeWriter::Compact(uint32_t const compac_seq, void* immbuf) {
  mu_.AssertHeld();
  assert(dst_);
  BlockBuf* const bb = static_cast<BlockBuf*>(immbuf);
  // Skip empty buffers
  if (bb->empty() && compac_seq == num_compac_completed_ + 1)
    return Status::OK();
  mu_.Unlock();  // Unlock as compaction is expensive
  Slice block_contents;
  if (!bb->empty()) {
    printf("calling finish %p\n", bb);
    block_contents = bb->Finish();
  }
  mu_.Lock();  // All writes are serialized through compac_seq
  assert(num_compac_completed_ < compac_seq);
  while (compac_seq != num_compac_completed_ + 1) {
    bg_cv_.Wait();
  }
  mu_.Unlock();
  Status status;
  if (!block_contents.empty()) {
    status = dst_->Append(block_contents);
  }
  if (status.ok()) {
    offset_ += block_contents.size();
    status = dst_->Flush();
  }
  mu_.Lock();
  printf("compacted: %d\n", compac_seq);
  return status;
}

// REQUIRES: no outstanding background compactions.
// REQUIRES: mu_ has been LOCKed.
Status RangeWriter::Close() {
  assert(!num_bg_compactions_);
  mu_.AssertHeld();
  assert(dst_);

  // TODO: Dump Manifest
  Status status = Status::OK();
  //
  //  if (status.ok()) {
  //    std::string footer;
  //    bloomfilter_handle_.EncodeTo(&footer);
  //    index_handle_.EncodeTo(&footer);
  //    footer.resize(2 * BlockHandle::kMaxEncodedLength);
  //    status = dst_->Append(footer);
  //  }

  if (status.ok()) {
    status = dst_->Sync();
    dst_->Close();
  }

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

  printf("schedule: %p\n", s->immbuf);

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
  printf("bgwork: %p\n", s->immbuf);
  s->writer->DoCompaction<RangeWriter>(s->compac_seq, s->immbuf);
  delete s;
}
}  // namespace plfsio
}  // namespace pdlfs
