#pragma once

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <string>

#include "pdlfs-common/leveldb/db/dbformat.h"
#include "pdlfs-common/leveldb/db/options.h"

namespace pdlfs {

// Grouping of constants.  We may want to make some of these
// parameters set via options.
namespace config {
static const int kNumLevels = 7;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
static const int kMaxMemCompactLevel = 2;

// Approximate gap in bytes between samples of data read during iteration.
static const int kReadBytesPeriod = 1048576;

// Number of files reserved from the limit of max open files.
static const int kNumNonTableCacheFiles = 10;
}  // namespace config

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern DBOptions SanitizeOptions(const std::string& db,
                                 const InternalKeyComparator* icmp,
                                 const InternalFilterPolicy* ipolicy,
                                 const DBOptions& src);

}  // namespace pdlfs
