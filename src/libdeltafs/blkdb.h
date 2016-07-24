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

#include "mds_cli.h"
#include "pdlfs-common/dcntl.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/fstypes.h"
#include "pdlfs-common/leveldb/db/db.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

// Opaque stream descriptor
typedef int sid_t;

struct Stream {
  Iterator* iter;
  uint64_t mtime;
  uint64_t size;     // Total size of the stream
  uint32_t nwrites;  // Number of blocks written
  uint32_t nflus;    // Number of blocks flushed to the DB

  // Formatted as:
  //   encoding_length  unsigned char
  //   encoding         char[encoding_length]
  char encoding_data[1];  // Beginning of encoded data

  Slice encoding() const {
    return Slice(encoding_data + 1, encoding_data[0]);  // Fentry encoding
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
  int session_id;
  size_t max_open_streams;
  bool verify_checksum;
  bool sync;
  DB* db;
};

class BlkDB {
 public:
  BlkDB(const BlkDBOptions&);
  ~BlkDB();

  Status Creat(const Fentry&, sid_t*);
  Status Open(const Fentry&, bool create_if_missing, bool truncate_if_exists,
              uint64_t* mtime, uint64_t* size, sid_t*);
  Status Pwrite(sid_t sid, const Slice& data, uint64_t off);
  Status Pread(sid_t sid, Slice* result, uint64_t off, uint64_t size,
               char* scratch);
  Status Flush(sid_t sid, bool force_sync = false);
  Status Close(sid_t sid);

  Status GetInfo(sid_t sid, Fentry* entry, bool* dirty, uint64_t* mtime,
                 uint64_t* size);

 private:
  // Constant after construction
  int session_id_;
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
