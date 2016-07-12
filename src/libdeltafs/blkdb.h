#pragma once

/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>

#include "pdlfs-common/dcntl.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/fstypes.h"
#include "pdlfs-common/leveldb/db/db.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

struct Stream {
  DirId pid;
  char nhash[8];
  Stream* prev;
  Stream* next;
  Iterator* iter;
  uint64_t mtime;
  uint64_t size;       // Total size of stream
  char prefix_rep[1];  // Beginning of prefix encoding

  Slice prefix() const {
    return Slice(prefix_rep + 1, prefix_rep[0]);  // Stream identifier
  }
};

struct StreamInfo {
  StreamInfo() {}
  bool DecodeFrom(Slice* input);
  Slice EncodeTo(char* scratch) const;
  uint64_t mtime;
  uint64_t size;
};

struct BlkDBOptions {
  BlkDBOptions();
  int cli_id;
  bool verify_checksum;
  bool sync;
  DB* db;
};

class BlkDB {
 public:
  BlkDB(const BlkDBOptions& options)
      : verify_checksum_(options.verify_checksum),
        sync_(options.sync),
        db_(options.db) {
    assert(db_ != NULL);
    head_.next = &head_;
    head_.prev = &head_;
  }
  ~BlkDB();

  Status Open(const DirId& pid, const Slice& nhash, const Stat& stat,
              bool create_if_missing, bool error_if_exists, Stream* s);
  Status Pwrite(Stream* s, const Slice& data, uint64_t off);
  Status Pread(Stream* s, Slice* result, uint64_t off, uint64_t size,
               char* scratch);
  Status Sync(Stream* s);
  Status Close(Stream* s);

 private:
  void Append(Stream* s);
  void Remove(Stream* s);
  port::Mutex mutex_;
  Stream head_;

  int cli_id_;
  bool verify_checksum_;
  bool sync_;
  DB* db_;

  // No copying allowed
  void operator=(const BlkDB&);
  BlkDB(const BlkDB&);
};

}  // namespace pdlfs
