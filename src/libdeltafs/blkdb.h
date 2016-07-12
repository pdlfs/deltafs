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

typedef int sid_t;  // Opaque stream descriptor

struct Stream {
  DirId pid;
  char nhash[8];
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
  size_t max_open_streams;
  bool verify_checksum;
  bool sync;
  DB* db;
};

class BlkDB {
 public:
  BlkDB(const BlkDBOptions&);
  ~BlkDB();

  Status Open(const DirId& pid, const Slice& nhash, const Stat& stat,
              bool create_if_missing, bool error_if_exists, sid_t* result);
  Status Pwrite(sid_t sid, const Slice& data, uint64_t off);
  Status Pread(sid_t sid, Slice* result, uint64_t off, uint64_t size,
               char* scratch);
  Status Sync(sid_t sid);
  Status Close(sid_t sid);

  Stream* GetStream(sid_t sid);

 private:
  // Constant after construction
  int cli_id_;
  size_t max_open_streams_;
  bool verify_checksum_;
  bool sync_;
  DB* db_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  size_t Append(Stream* s);
  void Remove(size_t idx);
  size_t next_stream_;
  size_t num_open_streams_;
  Stream** streams_;

  // No copying allowed
  void operator=(const BlkDB&);
  BlkDB(const BlkDB&);
};

}  // namespace pdlfs
