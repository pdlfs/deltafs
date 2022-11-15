/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "bufio.h"

namespace pdlfs {
namespace plfsio {

DirectWriter::DirectWriter(const DirOptions& options, WritableFile* dst,
                           size_t buf_size)
    : options_(options),
      dst_(dst),  // Not owned by us
      bg_cv_(&mu_),
      buf_threshold_(buf_size),
      buf_reserv_(buf_size),
      finished_(false),
      cmgr_(&mu_, &bg_cv_, options.compaction_pool, options.allow_env_threads),
      membuf_(NULL) {
  int nbuf = sizeof(str_) / sizeof(str_[0]);

  for (int lcv = 0 ; lcv < nbuf ; lcv++) {
    str_[lcv].reserve(buf_reserv_);  // reserve memory for our write buffers
    if (lcv > 0)
      bufs_.push_back(&str_[lcv]);   // str_[0] goes in membuf_
  }

  membuf_ = &str_[0];
}

// Wait for all outstanding compactions to clear.
DirectWriter::~DirectWriter() {
  MutexLock ml(&mu_);
  cmgr_.WaitForAll();
}

// Insert data into the writer.
// REQUIRES: Finish() has NOT been called.
Status DirectWriter::Append(const Slice& dat) {
  MutexLock ml(&mu_);

retry:
  if (finished_)
    return Status::AssertionFailed("invalid add on finished direct writer");
  assert(membuf_);
  if (membuf_->size() + dat.size() <= buf_threshold_) {
    membuf_->append(dat.data(), dat.size());
    return Status::OK();
  }

  //
  // our block was full and needs to be compacted.   we can start
  // a compaction if we have a free block to replace ours with.
  // if not, we need to wait for some I/O to clear and try again.
  //
  if (bufs_.empty()) {
    bg_cv_.Wait();           // drops lock while waiting, so state may change
    goto retry;
  }

  std::string* tocompact = membuf_;
  membuf_ = bufs_.back();
  bufs_.pop_back();
  membuf_->assign(dat.data(), dat.size());

  return cmgr_.ScheduleCompaction(tocompact, false, this, CompactCB, NULL);
}

// Sync data to storage. Data still buffered in memory is not sync'ed.
// REQUIRES: Finish() has NOT been called.
Status DirectWriter::Sync() {
  MutexLock ml(&mu_);                    // dtor will unlock for us
  Status status;

  status = StartAllCompactions();
  if (!status.ok()) return status;
  status = cmgr_.WaitForAll();
  if (!status.ok()) return status;

  return dst_->Sync();                  // fsync data to backend
}

// Finalize the writer. Expected to be called ONLY once.
Status DirectWriter::Finish() {
  MutexLock ml(&mu_);                    // dtor will unlock for us
  Status status, rv;

  if (finished_)
    return Status::AssertionFailed("Finish: already finished");

  status = StartAllCompactions();
  if (!status.ok() && rv.ok()) rv = status;

  status = cmgr_.WaitForAll();
  if (!status.ok() && rv.ok()) rv = status;

  status = dst_->Sync();
  if (!status.ok() && rv.ok()) rv = status;

  status = dst_->Close();
  if (!status.ok() && rv.ok()) rv = status;

  finished_ = 1;  // stop us using dst_ since it is now closed
  return rv;
}

// REQUIRES: mu_ has been LOCKed.
Status DirectWriter::Compact(uint32_t const cseq, std::string* const immbuf) {
  Status status;

  assert(dst_);
  mu_.AssertHeld();

  // do a quick return if block is empty and we already have write token
  if (immbuf->empty() && cmgr_.IsWriter(cseq)) {
    goto done;
  }

  if (finished_) {
    status = Status::AssertionFailed("Compact on finished writer");
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
  if (!immbuf->empty()) {
    status = dst_->Append(*immbuf);
    if (status.ok())
      status = dst_->Flush();
  }
  mu_.Lock();                      // relock

 done:
  immbuf->resize(0);
  bufs_.push_back(immbuf);
  return status;
}

Status DirectWriter::StartAllCompactions() {
  mu_.AssertHeld();

retry:
  if (membuf_->empty())
    return Status::OK();     // nothing to compact, so we are done!

  //
  // we can start compaction if we have a free block to replace ours with.
  // if not, we need to wait for some I/O to clear and try again.
  //
  if (bufs_.empty()) {
    bg_cv_.Wait();           // drops lock while waiting, so state may change
    goto retry;
  }

  std::string* tocompact = membuf_;
  membuf_ = bufs_.back();
  bufs_.pop_back();
  membuf_->resize(0);
  return cmgr_.ScheduleCompaction(tocompact, false, this, CompactCB, NULL);
}

DirectReader::DirectReader(const DirOptions& options, RandomAccessFile* src)
    : options_(options), src_(src) {  // src_ is not owned by us
}
// Directly read data from the source.
Status DirectReader::Read(uint64_t off, size_t n, Slice* result,
                          char* scratch) const {
  return src_->Read(off, n, result, scratch);
}

}  // namespace plfsio
}  // namespace pdlfs
