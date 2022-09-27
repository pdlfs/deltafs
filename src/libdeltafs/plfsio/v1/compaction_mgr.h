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

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */

#pragma once

#include <pdlfs-common/mutexlock.h>
#include "types.h"

#include <assert.h>

namespace pdlfs {
namespace plfsio {

//
// a compaction manager manages generic compaction state for objects
// that serially write their data through compaction to backing store.
// we ensure that compactions complete in sequence number order.
// we use our parent's mutex lock/cv for locking and sync.
//
class CompactionMgr {
 public:
  explicit CompactionMgr(port::Mutex* parent_mu, port::CondVar* parent_bgcv,
                         ThreadPool* compactpool, bool use_env_sched)
      : parent_mu_(parent_mu),
        parent_bg_cv_(parent_bgcv),
        next_cseq_(1),             // we start at seq# 1
        last_completed_(0),
        npending_(0),
        finished_(0),
        compactpool_(compactpool),
        use_env_sched_(use_env_sched) {
          nothreads_ = (compactpool_ == NULL && !use_env_sched_);
        }

  //
  // get token that says we are allowed to write to the underlying storage
  // (e.g. posix write).  compaction callback functions call this after
  // processing their compaction data in order to ensure that it is their
  // turn to write to backing storage (we will block until this is true
  // or there is an error).  may be called more than once (if your cseq
  // is already the writer, then this just returns OK).
  // caller must be holding the mutex.
  //
  Status AquireWriteToken(uint32_t cseq) {
    parent_mu_->AssertHeld();
    if (last_completed_ >= cseq)    // shouldn't ever happen
      return Status::InvalidArgument("cseq already complete");

    // wait for the seq before us to complete so that we go in order
    while (bg_status_.ok() && !IsWriter(cseq)) {
      parent_bg_cv_->Wait();
    }
    return bg_status_;
  }

  //
  // does the given compaction sequence number have the write token?
  // caller must be holding the mutex.
  //
  bool IsWriter(uint32_t cseq) {
    parent_mu_->AssertHeld();
    return (cseq == last_completed_ + 1);
  }

  //
  // wait for all pending compactions to complete.  must be holding lock.
  //
  Status WaitForAll() {
    parent_mu_->AssertHeld();
    while (bg_status_.ok() && npending_ > 0) {
      parent_bg_cv_->Wait();
    }
    return bg_status_;
  }

  //
  // wait for a given compaction to complete.  must be holding lock.
  //
  Status WaitFor(uint32_t cseq) {
    parent_mu_->AssertHeld();
    while (bg_status_.ok() && last_completed_ < cseq) {
      parent_bg_cv_->Wait();
    }
    return bg_status_;
  }

  //
  // finish compaction (waits for all compactions to complete).
  // must be holding lock.
  //
  Status Finish() {
    Status rv;
    parent_mu_->AssertHeld();
    if (finished_)
      return bg_status_;           // already finished?  cannot be ok.

    rv = this->WaitForAll();
    finished_ = 1;
    if (rv.ok()) {
      // set bg_status_ to already finished to block further calls to us
      bg_status_ = Status::AssertionFailed("Already finished", rv.ToString());
    }
    return rv;
  }

  //
  // owner-provided compaction callback function type.  the owner
  // should compact the item (e.g. close and compress the buffer),
  // aquire the write token from us, write the compacted data to
  // backing store, and then free the item.  when the callback returns
  // we will release the compaction sequence number for the owner.
  // called with lock held.   we expect that the callback will want
  // to drop the lock during computation and/or I/O operations
  // to allow for some parallelism (but it should regain the lock
  // before returning).
  //
  typedef Status (*callback)(uint32_t cseq, void *item, void *owner);

  //
  // schedule a compaction (may actually do the compaction if we
  // don't have worker threads or the compaction is trivial).
  // caller should be holding lock.
  //
  Status ScheduleCompaction(void *item, bool item_is_empty,
                            void *owner, callback compact_fn,
                            uint32_t *cseq_ret) {
    uint32_t cseq;
    Status rv;

    parent_mu_->AssertHeld();
    if (!bg_status_.ok())
      return bg_status_;

    // allocate a cseq and copy back to caller (if requested)
    cseq = next_cseq_++;
    npending_++;
    if (cseq_ret) *cseq_ret = cseq;

    // compact now if no threads or trivial compaction
    if (nothreads_ || (item_is_empty && IsWriter(cseq)) ) {
      rv = (*compact_fn)(cseq, item, owner);
      parent_mu_->AssertHeld();  // compact_fn may unlock, make sure it relocks
      if (rv.ok()) {
        assert(IsWriter(cseq));
        last_completed_++;
        npending_--;
      } else {
        if (bg_status_.ok())
          bg_status_ = rv;
      }
      parent_bg_cv_->SignalAll();
      return bg_status_;
    }

    //
    // send compaction to the thread pool
    //
    struct cbstate *cbs = new cbstate;
    cbs->cseq = cseq;
    cbs->cmgr = this;
    cbs->item = item;
    cbs->owner = owner;
    cbs->compact_fn = compact_fn;

    if (this->compactpool_) {
      this->compactpool_->Schedule(DoCompaction, cbs);
    } else {
      Env::Default()->Schedule(DoCompaction, cbs);
    }

    return bg_status_;
  }

  //
  // static member function for the callback scheduler to call.
  // we will grab the lock and call the user callback function.
  // the callback may unlock while processing, but will return
  // to us with the lock held.
  //
  static void DoCompaction(void *arg) {
    struct cbstate *cbs = reinterpret_cast<struct cbstate*>(arg);
    Status rv;
    MutexLock ml(cbs->cmgr->parent_mu_);

    rv = cbs->compact_fn(cbs->cseq, cbs->item, cbs->owner);  // may block
    cbs->cmgr->parent_mu_->AssertHeld();

    if (rv.ok()) {
        assert(cbs->cmgr->IsWriter(cbs->cseq));
        cbs->cmgr->last_completed_++;
        cbs->cmgr->npending_--;
    } else {
      if (cbs->cmgr->bg_status_.ok())
        cbs->cmgr->bg_status_ = rv;
    }
    cbs->cmgr->parent_bg_cv_->SignalAll();
  }

 private:
  port::Mutex* parent_mu_;         // protects us and our parent
  port::CondVar* parent_bg_cv_;    // should be internally tied to parent_mu_
  uint32_t next_cseq_;             // next compaction seq# to allocate
  uint32_t last_completed_;        // last compaction seq# completed
  uint32_t npending_;              // number of compactions pending
  bool finished_;                  // true if we are done with all I/O
  Status bg_status_;               // background compaction status
  // threading options.  if neither is available, we compact in calling thread!
  ThreadPool* compactpool_;        // optional compaction thread pool
  bool use_env_sched_;             // use Env Schedule() if !compactpool_
  bool nothreads_;                 // not using threads for compaction

  // No copying allowed
  void operator=(const CompactionMgr&);
  CompactionMgr(const CompactionMgr&);

  //
  // internal callback state struct passed to worker threads
  //
  struct cbstate {
    uint32_t cseq;
    CompactionMgr* cmgr;
    void *item;
    void *owner;
    callback compact_fn;
  };
};

}  // namespace plfsio
}  // namespace pdlfs
