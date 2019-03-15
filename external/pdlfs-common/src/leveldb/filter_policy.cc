/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/leveldb/filter_policy.h"

namespace pdlfs {

FilterPolicy::~FilterPolicy() {
  // Empty
}

}  // namespace pdlfs
