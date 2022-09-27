/*
 * Copyright (c) 2020 Carnegie Mellon University,
 * Copyright (c) 2020 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

//
// Created by Ankush J on 9/2/20.
//
#include <assert.h>
#include <algorithm>
#include <numeric>

#include "range_writer.h"
#include "coding_float.h"

namespace pdlfs {
namespace plfsio {

//
// partition manifest writer.  handles manifests for range writer.
// caller should be holding its lock for all calls to this object
// and should ensure that our writes to backing store are properly
// serialized.
//
class PartitionManifestWriter {
 public:
  //
  // ctor.  we cache our own copy of dst for writing to backing store.
  //
  explicit PartitionManifestWriter(WritableFile* dst)
      : dst_(dst),
        range_min_(FLT_MAX),
        range_max_(FLT_MIN),
        mass_total_(0),
        mass_oob_(0),
        finished_(false),
        num_ep_written_(0) {}


  //
  // add metadata from a blk to our in memory manifest (at compaction time).
  //
  void AddItem(uint64_t offset, OrderedBlockBuilder* blk) {
    PartitionManifestItem item;
    uint32_t part_count = 0, part_oob = 0;

    assert(!finished_ && blk != NULL);

    // get number of k/v pairs added to the block
    blk->GetNumItems(part_count, part_oob);
    if (part_count == 0)     // empty block, no need to save metadata
      return;

    // load in manifest data and append an item to items_
    item.offset = offset;
    item.expected = blk->GetExpectedRange();
    item.observed = blk->GetObservedRange();
    item.updcnt = blk->GetUpdateCount();
    item.part_item_count = part_count;
    item.part_item_oob = part_oob;
    items_.push_back(item);

    // update overall stats
    range_min_ = std::min(range_min_, item.observed.rmin());
    range_max_ = std::max(range_max_, item.observed.rmax());
    mass_total_ += part_count;
    mass_oob_ += part_oob;

    return;
  }

  //
  // serialize all our manifest data in items_ and append it to
  // our in-memory indexes_ buffer.
  //
  void EpochFlushManifest() {
    std::string seritems;
    assert(!finished_);

    // serialize items_
    for (size_t i = 0; i < items_.size(); i++) {
      PartitionManifestItem& item = items_[i];
      PutFixed64(&seritems, i);
      PutFixed64(&seritems, item.offset);
      PutFloat32(&seritems, item.expected.rmin());
      PutFloat32(&seritems, item.expected.rmax());
      PutFloat32(&seritems, item.observed.rmin());
      PutFloat32(&seritems, item.observed.rmax());
      PutFixed32(&seritems, item.updcnt);
      PutFixed32(&seritems, item.part_item_count);
      PutFixed32(&seritems, item.part_item_oob);
    }

    // reset items_ and associated metadata
    items_.clear();
    range_min_ = FLT_MAX;
    range_max_ = FLT_MIN;
    mass_total_ = 0;
    mass_oob_ = 0;

    // put in the header first, then the data (if any)
    PutFixed32(&indexes_, num_ep_written_);   // current epoch number
    PutFixed64(&indexes_, seritems.size());
    if (!seritems.empty()) {
      indexes_.append(seritems);
    }
    num_ep_written_++;          // update epoch counter
  }


  //
  // finish by putting a footer on indexes_ and appending its contents
  // to dst.  caller must ensure that we are allowed to write to backing
  // store.
  //
  Status Finish() {
    Status status;
    assert(!finished_ && dst_);

    // flush final set of tables (client only flushes at start of epoch)
    if (items_.size()) {
      this->EpochFlushManifest();
    }

    //
    // append a footer to the indexes_ buf to finish it.  readers can read
    // this footer to know how how many epochs we had and how far back to
    // seek to find manifest info for the first epoch.
    //
    uint64_t manifest_size = indexes_.size();  // before footer add
    PutFixed32(&indexes_, num_ep_written_);    // number of epochs
    PutFixed64(&indexes_, manifest_size);      // size of manifest data
    finished_ = true;

    // write it and sync it, then we are finished
    status = dst_->Append(indexes_);
    if (status.ok()) {
      status = dst_->Sync();
    }
    return status;
  }

  //
  // get current observed range of the manifest
  //
  void GetRange(float& range_min, float& range_max) const {
    range_min = range_min_;
    range_max = range_max_;
  }

  //
  // get current mass values of the manifest
  //
  void GetMass(uint64_t& mass_total, uint64_t mass_oob) const {
    mass_total = mass_total_;
    mass_oob = mass_oob_;
  }

  //
  // get number of blocks in current epoch
  //
  size_t Size() const { return items_.size(); }

 private:
  //
  // we generate a PartitionManifestItem every time we add a block
  // to the manifest.
  //
  typedef struct {
    uint64_t offset;            // offset of block in dst
    Range expected;             // block's expected range
    Range observed;             // block's observed range
    uint32_t updcnt;            // value of updcnt (expected range updates)
    uint32_t part_item_count;   // total k/v pairs in block
    uint32_t part_item_oob;     // subset of k/v pairs that were oob
  } PartitionManifestItem;

  WritableFile* const dst_;                   // cached copy from range writer
  std::vector<PartitionManifestItem> items_;  // blk metadata (in memory)
  float range_min_;                           // observed min for manifest
  float range_max_;                           // observed max for manifest
  uint64_t mass_total_;                       // total cnt for manifest
  uint64_t mass_oob_;                         // just oob for manifest
  std::string indexes_;                       // serialized manifest (so far)
  bool finished_;                             // manifest complete
  uint32_t num_ep_written_;                   // #epochs written
};

//
// RangeWriterPerfLogger: timestamps all compactions.  only active
// if logging is turned on.  we use env to get current microseconds.
//
class RangeWriterPerfLogger {
 public:
  RangeWriterPerfLogger() {};

  void RegisterCompacBegin(uint32_t cseq) {       // compaction start
    compac_table_[cseq].begin = CurrentMicros();
  }
  void RegisterCompacPreprocess(uint32_t cseq) {  // ready for I/O
    compac_table_[cseq].pre = CurrentMicros();
  }
  void RegisterCompacPostprocess(uint32_t cseq) { // I/O complete
    compac_table_[cseq].post = CurrentMicros();
  }
  void RegisterCompacEnd(uint32_t cseq) {        // compaction end
    uint64_t total_time;
    Elem& e = compac_table_[cseq];
    e.end = CurrentMicros();
    total_time = e.end - e.begin;
    compac_hist_.push_back(total_time);

    // prints: preproc/compute time, wait/io time, postproc time
    fprintf(stderr, "\nTime: %.2fms %.2fms %.2fms (TID: %p)\n",
            (e.pre - e.begin) * 1e-3, (e.post - e.pre) * 1e-3,
            (e.end - e.post) * 1e-3, (void*)pthread_self());
  }
  // print final report
  void Report() {
    fprintf(stderr, "Stats: %zu items, mean: %.2fms (range: %.2fms-%.2fms)\n",
            compac_hist_.size(),
            std::accumulate(compac_hist_.begin(), compac_hist_.end(), 0ull)
                                               * 1e-3 / compac_hist_.size(),
            *std::min_element(compac_hist_.begin(), compac_hist_.end()) * 1e-3,
            *std::max_element(compac_hist_.begin(), compac_hist_.end()) * 1e-3);
  }
 private:
  typedef struct {  // microsecond timestamps for compaction phases
    uint64_t begin;
    uint64_t pre;
    uint64_t post;
    uint64_t end;
  } Elem;
  std::map<uint32_t, Elem> compac_table_;   // maps cseq to its timestamps
  std::vector<uint32_t> compac_hist_;       // per-compact hist of total time
};

//
// main RangeWriter code
//

//
// constructor.  general options come in via "options" while range
// writer specific options are parameters.  we allocate all the
// block here.   we set nstore_ to allocate two sets of active
// blocks, thus allowing us to have one full set active and one
// full set in compaction.
//
RangeWriter::RangeWriter(const DirOptions& options, WritableFile* dst,
                         int nsubpart, int nprevious,
                         size_t compact_threshold)
    : options_(options),
      nsubpart_(nsubpart),
      nprevious_(nprevious),
      nactive_(nsubpart+nprevious),
      nstore_(nactive_*2),
      compact_threshold_(compact_threshold),
      value_size_(options.value_size),
      bg_cv_(&mu_),
      finished_(false),
      dst_(dst),
      dst_offset_(0),
      manifest_(NULL),
      cmgr_(&mu_, &bg_cv_, options.compaction_pool, options.allow_env_threads),
      logger_(NULL),
      block_store_(NULL),
      bactive_(NULL) {
  bool logging_enabled = false;      // hardwired, just for debugging

  // assert some sanity checks....
  assert(nsubpart_ > 0 && nprevious_ > 0);
  assert(compact_threshold_ > options.key_size + options.value_size);
  assert(dst_);

  manifest_ = new PartitionManifestWriter(dst);

  if (logging_enabled)
    logger_ = new RangeWriterPerfLogger();

  block_store_ = new OrderedBlockBuilder*[nstore_];
  memset(block_store_, 0, nstore_*sizeof(*block_store_));
  bactive_ = new OrderedBlockBuilder*[nsubpart_+nprevious_];

  // the first nactive_ go in bactive_[], the rest on the free list
  for (int lcv = 0 ; lcv < nstore_ ; lcv++) {
    block_store_[lcv] = new OrderedBlockBuilder(options);

    // XXXCDC: is the +8 needed?  the Reserve() call does a c++ string
    // reserve on abstract block builder buffer_.   we call blk finish
    // if we would cross the threshold (or we are flushing).  the ordered
    // block builder finish fn sorts the k/v data into buffer_ but does
    // not add any additional data to it (e.g. no header or footer), so
    // it should not grow past compact_threshold_.  (the metadata ends
    // up in the manifest, not in buffer_ ...)
    block_store_[lcv]->Reserve(compact_threshold_+8);

    if (lcv < nactive_)
      bactive_[lcv] = block_store_[lcv];
    else
      bfree_.push_back(block_store_[lcv]);
  }
}

//
// destructor...  we expect the caller to have called Finish(), but
// we make sure all I/O has stopped anyway.  once that is done we
// can delete allocated memory.
//
RangeWriter::~RangeWriter() {
  mu_.Lock();
  cmgr_.WaitForAll();
  if (logger_) {
    delete logger_;
    logger_ = NULL;
  }
  mu_.Unlock();
  delete manifest_;
  delete[] bactive_;
  for (int lcv = 0 ; lcv < nstore_ ; lcv++) {
    delete block_store_[lcv];
  }
  delete[] block_store_;
}


//
// add a key/value part.   key must be a float.  we aquire the lock.
//
Status RangeWriter::Add(const Slice& k, const Slice& v) {
  MutexLock ml(&mu_);                    // dtor will unlock for us
  int bidx;
  OrderedBlockBuilder *blk, *newblk;
  float key;

  if (k.size() != sizeof(float) || v.size() != value_size_)
    return Status::InvalidArgument("bad key/value size");

  key = DecodeFloat32(k.data());

 retry:
  if (finished_)
    return Status::AssertionFailed("invalid add on finished range writer");
  bidx = this->FindBlock(key);
  blk = bactive_[bidx];
  if (blk->CurrentSizeEstimate() + k.size() + v.size() < compact_threshold_) {
    blk->Add(k, v);
    return Status::OK();
  }

  //
  // our block was full and needs to be compacted.   we can start
  // a compaction if we have a free block to replace ours with.
  // if not, we need to wait for some I/O to clear and try again.
  //
  if (bfree_.empty()) {
    bg_cv_.Wait();           // drops lock while waiting, so state may change
    goto retry;
  }

  newblk = bfree_.back();
  bfree_.pop_back();
  newblk->CopyFrom(blk);     // copies range from old block
  bactive_[bidx] = newblk;   // install empty block we just got
  newblk->Add(k, v);         // now add the data, user's request is done

  //
  // now we need to compact the old block
  //
  return cmgr_.ScheduleCompaction(blk, false, this, CompactCB, NULL);
}

//
// internal fn to start flushing all blocks to storage via compaction.
// lock should be held.
//
Status RangeWriter::StartAllCompactions() {
  mu_.AssertHeld();
  std::vector<OrderedBlockBuilder*> to_be_compacted;
  Status rv;

  // wait until we have enough free blks to replace all the active ones
  while (!finished_ && bfree_.size() < nactive_) {
    bg_cv_.Wait();
  }
  if (finished_)
    return Status::AssertionFailed("StartAllCompactions on finished writer");

  for (int lcv = 0 ; lcv < nactive_ ; lcv++) {
    OrderedBlockBuilder *blk = bactive_[lcv];
    OrderedBlockBuilder *newblk = bfree_.back();
    bfree_.pop_back();
    newblk->CopyFrom(blk);     // copies range from old block
    bactive_[lcv] = newblk;    // install empty block we just got
    to_be_compacted.push_back(blk);
  }

  for (int lcv = 0 ; lcv < nactive_ ; lcv++) {
    OrderedBlockBuilder *blk = to_be_compacted[lcv];
    Status s = cmgr_.ScheduleCompaction(blk, blk->empty(), this,
                                        CompactCB, NULL);
    if (!s.ok()) {
      //
      // this shouldn't happen, but if it does we are going to drop the
      // block and lose its data...  complain about it so the user knows.
      //
      if (rv.ok())     // return first error we hit
        rv = s;
      fprintf(stderr, "RangeWriter::StartAllCompactions: %s %s\n",
              "data block dropped due to compaction error",
              s.ToString().c_str());
      blk->Reset();
      bfree_.push_back(blk);
    }
  }

  return rv;
}

//
// start all compactions, wait for them to finish, then
// EpochFlush the manifest.
//
Status RangeWriter::EpochFlush() {
  MutexLock ml(&mu_);                    // dtor will unlock for us
  Status status;

  status = StartAllCompactions();
  if (!status.ok()) return status;
  status = cmgr_.WaitForAll();
  if (!status.ok()) return status;

  // in memory flush of manifest
  manifest_->EpochFlushManifest();

  return Status::OK();
}

//
// compact all block data to disk and sync the backend
//
Status RangeWriter::Sync() {
  MutexLock ml(&mu_);                    // dtor will unlock for us
  Status status;

  status = StartAllCompactions();
  if (!status.ok()) return status;
  status = cmgr_.WaitForAll();
  if (!status.ok()) return status;

  return dst_->Sync();                  // fsync data to backend
}

//
// compact out the active blocks and update all the ranges
//
Status RangeWriter::UpdateBounds(const float rmin, const float rmax) {
  MutexLock ml(&mu_);                    // dtor will unlock for us
  Status status;
  float oldmin, oldmax, rdel;

  status = StartAllCompactions();
  if (!status.ok()) return status;

  // shift previous ranges, discarding the oldest previous range
  for (int lcv = nactive_ - 1 ; lcv >= nsubpart_ + 1 ; lcv--) {
    bactive_[lcv]->UpdateExpectedRange( bactive_[lcv-1]->GetExpectedRange() );
  }

  // the most recent previous range gets the current range over all subparts
  oldmin = bactive_[0]->GetExpectedRange().rmin();
  oldmax = bactive_[nsubpart_-1]->GetExpectedRange().rmax();
  bactive_[nsubpart_]->UpdateExpectedRange(oldmin, oldmax);

  // now divide the new range up into subparts and install it
  rdel = (rmax - rmin) / nsubpart_;
  for (int lcv = 0 ; lcv < nsubpart_ ; lcv++) {
    float newmin = rmin + (lcv*rdel);
    float newmax = (lcv == nsubpart_ - 1) ? rmax : rmin+rdel;
    bactive_[lcv]->UpdateExpectedRange(newmin, newmax);
  }

  return Status::OK();
}

//
// finished adding data.  flush everything out, sync, and close dst_.
//
Status RangeWriter::Finish() {
  MutexLock ml(&mu_);                    // dtor will unlock for us
  Status status, rv;

  if (finished_)
    return Status::AssertionFailed("Finish: already finished");

  status = StartAllCompactions();
  if (!status.ok() && rv.ok()) rv = status;

  status = cmgr_.WaitForAll();
  if (!status.ok() && rv.ok()) rv = status;

  // append the manifest to the end of the backing file
  status = manifest_->Finish();
  if (!status.ok() && rv.ok()) rv = status;

  // append ending footer to file
  std::string footer;
  PutFixed64(&footer, options_.key_size);
  PutFixed64(&footer, options_.value_size);
  status = dst_->Append(footer);
  if (!status.ok() && rv.ok()) rv = status;

  status = dst_->Sync();
  if (!status.ok() && rv.ok()) rv = status;

  status = dst_->Close();
  dst_ = NULL;
  if (!status.ok() && rv.ok()) rv = status;

  finished_ = 1;
  if (logger_)
    logger_->Report();

  return rv;
}

//
// compact a block.  called from compaction manager with lock held
// (but we are allowed to drop the lock during slow operations as
// long as we return with the lock held).  we do compaction processing,
// get the write token, write the compacted data to backing store,
// then free the block.
//
Status RangeWriter::Compact(uint32_t const cseq, OrderedBlockBuilder *blk) {
  Status status;
  Slice contents;

  assert(dst_);
  mu_.AssertHeld();

  // do a quick return if block is empty and we already have write token
  if (blk->empty() && cmgr_.IsWriter(cseq)) {
    goto done;
  }

  if (finished_) {
    status = Status::AssertionFailed("Compact on finished writer");
    goto done;
  }

  if (logger_)
    logger_->RegisterCompacBegin(cseq);             // compaction started

  mu_.Unlock();                    // unlock during compaction computation
  contents = blk->Finish();
  mu_.Lock();                      // relock

  if (logger_)
    logger_->RegisterCompacPreprocess(cseq);        // block ready for i/o

  if (finished_) {
    status = Status::AssertionFailed("Writer finished mid-compaction!");
    goto done;
  }

  status = cmgr_.AquireWriteToken(cseq);  // may unlock and relock
  if (!status.ok()) {
    goto done;
  }

  if (finished_) {
    status = Status::AssertionFailed("Writer finished when gaining token!");
    goto done;
  }

  mu_.Unlock();                    // unlock during compaction I/O
  if (!contents.empty()) {
    status = dst_->Append(contents);
    if (status.ok())
      status = dst_->Flush();
  }
  mu_.Lock();                      // relock

  if (logger_)
    logger_->RegisterCompacPostprocess(cseq);      // block I/O complete

  // only update manifest and offset if I/O was a success
  if (status.ok()) {
    manifest_->AddItem(dst_offset_, blk);
    dst_offset_ += contents.size();
  }

  if (logger_)
    logger_->RegisterCompacEnd(cseq);              // all done

 done:
  blk->Reset();
  bfree_.push_back(blk);
  return status;
}

}  // namespace plfsio
}  // namespace pdlfs
