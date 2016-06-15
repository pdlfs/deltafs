/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "builder.h"
#include "../table_stats.h"
#include "table_cache.h"
#include "version_edit.h"

#include "pdlfs-common/dbfiles.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/leveldb/db/db.h"
#include "pdlfs-common/leveldb/db/dbformat.h"
#include "pdlfs-common/leveldb/db/options.h"
#include "pdlfs-common/leveldb/iterator.h"
#include "pdlfs-common/leveldb/table_builder.h"

namespace pdlfs {

static Status CheckFirstKey(Table* table, const Options& options,
                            const InternalKey& key) {
  Slice reported = TableStats::FirstKey(table);
  const InternalKeyComparator* icmp =
      reinterpret_cast<const InternalKeyComparator*>(options.comparator);
  if (icmp->Compare(reported, key.Encode()) != 0) {
    return Status::Corruption("First key is reported wrong");
  } else {
    return Status::OK();
  }
}

static Status CheckLastKey(Table* table, const Options& options,
                           const InternalKey& key) {
  Slice reported = TableStats::LastKey(table);
  const InternalKeyComparator* icmp =
      reinterpret_cast<const InternalKeyComparator*>(options.comparator);
  if (icmp->Compare(reported, key.Encode()) != 0) {
    return Status::Corruption("Last key is reported wrong");
  } else {
    return Status::OK();
  }
}

static Status LoadAndCheckTable(const Options& options, TableCache* table_cache,
                                const FileMetaData* meta) {
  Status s;
  Table* table;
  Iterator* it = table_cache->NewIterator(
      ReadOptions(), meta->number, meta->file_size, meta->seq_off, &table);

  s = it->status();
  const bool paranoid_checks = options.paranoid_checks;
  if (s.ok() && paranoid_checks) {
    if (TableStats::HasStats(table)) {
      if (s.ok()) {
        s = CheckFirstKey(table, options, meta->smallest);
      }
      if (s.ok()) {
        s = CheckLastKey(table, options, meta->largest);
      }
    }
  }

  delete it;
  return s;
}

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  assert(meta->number != 0);
  meta->file_size = 0;
  meta->seq_off = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      builder->Add(key, iter->value());
    }

    // Finish and check for builder errors
    if (s.ok()) {
      s = builder->Finish();
      if (s.ok()) {
        meta->file_size = builder->FileSize();
        assert(meta->file_size > 0);
      }
    } else {
      builder->Abandon();
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;

    if (s.ok()) {
      s = LoadAndCheckTable(options, table_cache, meta);
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace pdlfs
