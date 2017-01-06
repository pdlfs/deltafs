/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "builder.h"
#include "table_cache.h"
#include "version_edit.h"

#include "pdlfs-common/dbfiles.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/leveldb/iterator.h"
#include "pdlfs-common/leveldb/table_builder.h"
#include "pdlfs-common/leveldb/table_properties.h"

namespace pdlfs {

Status BuildTable(const std::string& dbname, Env* env, const DBOptions& options,
                  TableCache* table_cache, Iterator* iter,
                  SequenceNumber* min_seq, SequenceNumber* max_seq,
                  FileMetaData* meta) {
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
    for (; iter->Valid(); iter->Next()) {
      builder->Add(iter->key(), iter->value());
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

    // Obtain table properties
    if (s.ok()) {
      const TableProperties* props = builder->properties();
      meta->smallest.DecodeFrom(props->first_key());
      meta->largest.DecodeFrom(props->last_key());
      *min_seq = props->min_seq();
      *max_seq = props->max_seq();
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
      Table* table;
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(
          ReadOptions(), meta->number, meta->file_size, meta->seq_off, &table);
      s = it->status();

#if 0
      if (s.ok()) {
        const TableProperties* props = table->GetProperties();
        if (props != NULL) {
          if (props->first_key() != meta->smallest.Encode() ||
              props->last_key() != meta->largest.Encode()) {
            s = Status::Corruption("Table properties fail to match MANIFEST");
            table_cache->Evict(meta->number);
          }
        }
      }
#endif

      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    if (s.ok()) {
      s = iter->status();
    }
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace pdlfs
