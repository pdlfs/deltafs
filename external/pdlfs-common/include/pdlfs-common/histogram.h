/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <string>

namespace pdlfs {

class Histogram {
 public:
  Histogram() {}
  ~Histogram() {}

  void Clear();
  void Add(double value);
  void Merge(const Histogram& other);

  std::string ToString() const;

  double Median() const;
  double Percentile(double p) const;
  double Average() const;
  double StandardDeviation() const;

 private:
  double min_;
  double max_;
  double num_;
  double sum_;
  double sum_squares_;

  enum { kNumBuckets = 154 };
  static const double kBucketLimit[kNumBuckets];
  double buckets_[kNumBuckets];
};

}  // namespace pdlfs
