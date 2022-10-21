/*
 * Copyright (c) 2022 Carnegie Mellon University,
 * Copyright (c) 2022 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <algorithm>
#include <inttypes.h>

#include "range.h"
#include "pdlfs-common/testharness.h"

namespace pdlfs {
namespace plfsio {

class RangeTest {};

TEST(RangeTest, Range) {
  Range r;
  ASSERT_FALSE(r.IsSet());
  ASSERT_TRUE(r.IsValid());

  r.Set(10.0, 20.0);
  ASSERT_TRUE(r.IsSet());
  ASSERT_TRUE(r.IsValid());

  ASSERT_EQ(r.rmin(), 10.0);
  ASSERT_EQ(r.rmax(), 20.0);
  ASSERT_FALSE(r.Inside(-10.0));
  ASSERT_FALSE(r.Inside(0.0));
  ASSERT_TRUE(r.Inside(10.0));
  ASSERT_TRUE(r.Inside(15.0));
  ASSERT_FALSE(r.Inside(20.0));
  ASSERT_FALSE(r.Inside(25.0));

  r.Extend(10.0, 30.0);
  ASSERT_EQ(r.rmin(), 10.0);
  ASSERT_EQ(r.rmax(), 30.0);
  ASSERT_FALSE(r.Inside(-10.0));
  ASSERT_FALSE(r.Inside(0.0));
  ASSERT_TRUE(r.Inside(10.0));
  ASSERT_TRUE(r.Inside(15.0));
  ASSERT_TRUE(r.Inside(20.0));
  ASSERT_TRUE(r.Inside(25.0));
  ASSERT_FALSE(r.Inside(30.0));

  ASSERT_TRUE(r.IsSet());
  ASSERT_TRUE(r.IsValid());
  r.Reset();
  ASSERT_FALSE(r.IsSet());
  ASSERT_TRUE(r.IsValid());
}

TEST(RangeTest, ObservedRange) {
  ObservedRange o, o2;

  ASSERT_FALSE(o.IsSet());
  ASSERT_TRUE(o.IsValid());
  o.Set(10.0, 20.0);
  ASSERT_TRUE(o.IsSet());
  ASSERT_TRUE(o.IsValid());

  o.Observe(20.0);
  o.Observe(14.0);
  o.Observe(50.0);
  ASSERT_EQ(o.rmin(), 10.0);
  ASSERT_EQ(o.rmax(), 20.0);
  ASSERT_EQ(o.smallest(), 14.0);
  ASSERT_EQ(o.largest(), 50.0);
  ASSERT_EQ(o.num_items(), 3);
  ASSERT_EQ(o.num_items_oob(), 2);

  o.ClearObservations();
  o.Observe(19.0);
  o.Observe(16.0);
  o.Observe(48.0);
  ASSERT_EQ(o.rmin(), 10.0);
  ASSERT_EQ(o.rmax(), 20.0);
  ASSERT_EQ(o.smallest(), 16.0);
  ASSERT_EQ(o.largest(), 48.0);
  ASSERT_EQ(o.num_items(), 3);
  ASSERT_EQ(o.num_items_oob(), 1);

  o2.Set(5.0, 15.0);
  o2.Observe(6.0);
  o2.Observe(16.0);
  o.MergeRangeObservations(&o2);
  ASSERT_EQ(o.rmin(), 5.0);
  ASSERT_EQ(o.rmax(), 20.0);
  ASSERT_EQ(o.smallest(), 6.0);
  ASSERT_EQ(o.largest(), 48.0);
  ASSERT_EQ(o.num_items(), 5);
  ASSERT_EQ(o.num_items_oob(), 2);
}

}  // namespace plfsio
}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
