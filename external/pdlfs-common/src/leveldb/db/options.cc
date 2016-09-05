/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/cache.h"
#include "pdlfs-common/dbfiles.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/leveldb/comparator.h"
#include "pdlfs-common/leveldb/filter_policy.h"

#include "options_internal.h"

namespace pdlfs {

DBOptions::DBOptions()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(NULL),
      write_buffer_size(4 << 20),
      table_cache(NULL),
      block_cache(NULL),
      block_size(4096),
      block_restart_interval(16),
      compression(kSnappyCompression),
      filter_policy(NULL),
      gc_skip_deletion(false),
      skip_lock_file(false),
      rotating_manifest(false),
      disable_compaction(false),
      disable_seek_compaction(false),
      table_file_size(2 * 1048576),
      level_factor(10),
      l1_compaction_trigger(5),
      l0_compaction_trigger(4),
      l0_soft_limit(8),
      l0_hard_limit(12) {}

ReadOptions::ReadOptions()
    : verify_checksums(false),
      fill_cache(true),
      limit(1 << 31),
      snapshot(NULL) {}

WriteOptions::WriteOptions() : sync(false) {}

FlushOptions::FlushOptions() : wait(true) {}

InsertOptions::InsertOptions()
    : no_seq_adjustment(false),
      suggested_max_seq(0),
      verify_checksums(false),
      method(kRename) {}

DumpOptions::DumpOptions() : verify_checksums(false), snapshot(NULL) {}

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

DBOptions SanitizeOptions(const std::string& dbname,
                          const InternalKeyComparator* icmp,
                          const InternalFilterPolicy* ipolicy,
                          const DBOptions& src, bool create_infolog) {
  DBOptions result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (create_infolog && result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  if (result.table_cache == NULL) {
    result.table_cache = NewLRUCache(1000);
  }
  return result;
}

}  // namespace pdlfs
