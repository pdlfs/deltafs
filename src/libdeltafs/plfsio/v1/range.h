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

#include <float.h>

namespace pdlfs {
namespace plfsio {

//
// range of floating point numbers.  the range is defined by a min and
// max value (inclusive and exclusive, respectively).  the default/reset
// range is an undefined one (where we set rmin=FLT_MAX, rmax=FLT_MIN).
// note that a range with rmin==rmax is a zero-width range (nothing can
// be inside it).  ranges are safe to copy.  owner must provide locking.
//
class Range {
 public:
  Range() : rmin_(FLT_MAX), rmax_(FLT_MIN) {}

  /* accessor functions */
  float rmin()          const { return rmin_; }
  float rmax()          const { return rmax_; }

  void Reset() {                /* reset everything to clean state */
    rmin_ = FLT_MAX;
    rmax_ = FLT_MIN;
  }

  bool IsSet() const {          /* is range set? */
    return (rmin_ != FLT_MAX) and (rmax_ != FLT_MIN);
  }

  bool IsValid() const {        /* is range valid?  (i.e. reset or set) */
    return !IsSet() || (rmin_ < rmax_);
  }

  void Set(float rmin, float rmax) {      /* set a new range */
    if (rmin > rmax) return;              /* XXX: invalid, shouldn't happen */
    rmin_ = rmin;
    rmax_ = rmax;
  }

  void Extend(float emin, float emax) {  /* extend range if needed */
    rmin_ = std::min(rmin_, emin);
    rmax_ = std::max(rmax_, emax);
  }

  /* is "f" inside our range? */
  bool Inside(float f) const { return (f >= rmin_ && f < rmax_); }

 private:
  float rmin_;              /* start of range (inclusive) */
  float rmax_;              /* end of range (exclusive) */
};

//
// an observed range of floating point numbers is a range (see above)
// in which we track metadata on the keys inserted into the range.
// current metadata tracked: smallest key, largest key, number of
// keys observed, and number of those keys that were out of bounds.
// observed ranges are safe to copy.  owner must provide locking.
//
class ObservedRange : public Range {
 public:
  ObservedRange() : smallest_(FLT_MAX), largest_(FLT_MIN),
                    num_items_(0), num_items_oob_(0) {}

  /* accessor functions */
  float smallest()         const { return smallest_; }
  float largest()          const { return largest_; }
  uint32_t num_items()     const { return num_items_; }
  uint32_t num_items_oob() const { return num_items_oob_; }

  void ClearObservations() {    /* clear observations, but keep range */
    smallest_ = FLT_MAX;
    largest_ = FLT_MIN;
    num_items_ = num_items_oob_ = 0;
  }

  void Reset() {                /* reset everything to clean state */
    this->ClearObservations();
    Range::Reset();
  }

  /* track a new key being added to a range */
  void Observe(float key) {
    smallest_ = std::min(smallest_, key);
    largest_ = std::max(largest_, key);
    num_items_++;
    if (!Range::Inside(key))
      num_items_oob_++;
  }

  /* merge range data from another range into our range */
  void MergeRangeObservations(ObservedRange *toadd) {
    Range::Extend(toadd->rmin(), toadd->rmax());
    smallest_ = std::min(smallest_, toadd->smallest_);
    largest_ = std::max(largest_, toadd->largest_);
    num_items_ += toadd->num_items_;
    num_items_oob_ += toadd->num_items_oob_;
  }

  void Set(float rmin, float rmax) {  /* set a new range */
    ClearObservations();          /* new range invalidates old observations */
    Range::Set(rmin, rmax);
  }

 private:
  float smallest_;          /* smallest key seen */
  float largest_;           /* largest key seen */
  uint32_t num_items_;      /* number of items added */
  uint32_t num_items_oob_;  /* number of items added that were out of bounds */
};

}  // namespace plfsio
}  // namespace pdlfs
