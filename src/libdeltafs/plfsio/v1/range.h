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
// range of floating point numbers defined by a min and max value (inclusive).
// the default/reset range is an empty one (rmin=FLT_MAX, rmax=FLT_MIN).
//
class Range {
 public:
  Range() : rmin_(FLT_MAX), rmax_(FLT_MIN) {}
  Range(float rmin, float rmax) : rmin_(rmin), rmax_(rmax) {}

  float rmin() { return rmin_; }
  float rmax() { return rmax_; }

  Range& operator=(const Range& r) {   // copy assignment operator
    rmin_ = r.rmin_;
    rmax_ = r.rmax_;
    return *this;
  }

  void Reset() {
    rmin_ = FLT_MAX;
    rmax_ = FLT_MIN;
  }

  bool IsSet() const {
    return (rmin_ != FLT_MAX) and (rmax_ != FLT_MIN);
  }

  // valid if in reset state or rmin < rmax  (XXX: should be "rmin <= rmax" ?)
  bool IsValid() const {
    return ((rmin_ == FLT_MAX && rmax_ == FLT_MIN) or
            (rmin_ < rmax_));
  }

  // set the range.  no change made if new range is invalid.
  void Set(float qr_min, float qr_max) {
    if (qr_min > qr_max) return;        // not valid
    rmin_ = qr_min;
    rmax_ = qr_max;
  }

  // is "f" inside our range?  (inclusive)
  bool Inside(float f) const { return (f >= rmin_ && f <= rmax_); }

  bool Overlaps(float qr_min, float qr_max) const {
    if (qr_min > qr_max) return false;  // params not valid

    bool qr_envelops =
        (qr_min < rmin_) and (qr_max > rmax_) and IsSet();

    return Inside(qr_min) or Inside(qr_max) or qr_envelops;
  }

  // extend range to include 'f'.  if range is in the reset state, this
  // will set both rmin and rmax to 'f'
  void Extend(float f) {
    rmin_ = std::min(rmin_, f);
    rmax_ = std::max(rmax_, f);
  }

 private:
  float rmin_;
  float rmax_;
};

}  // namespace plfsio
}  // namespace pdlfs
