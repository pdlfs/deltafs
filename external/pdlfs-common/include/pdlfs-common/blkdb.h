#pragma once

/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/env.h"
#include "pdlfs-common/fio.h"
#include "pdlfs-common/leveldb/db/db.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

// Each file is regarded as a stream of variable-size data blocks with
// each block having an offset, a size, and an extent of file data.
struct Stream : public Fio::Handle {
  Stream() {}
  uint64_t mtime;
  uint64_t size;
  uint64_t off;  // Current read/write position

  Iterator* iter;   // Cursor to the current data block
  int32_t nwrites;  // Number of block writes
  int32_t nflus;    // Number of writes committed to the header
};

struct StreamHeader {
  StreamHeader() {}
  bool DecodeFrom(Slice* input);
  Slice EncodeTo(char* scratch) const;
  uint64_t mtime;
  uint64_t size;
};

struct BlkDBOptions {
  BlkDBOptions();
  int uniquefier;
  bool sync;
  bool verify_checksum;
  bool owns_db;
  DB* db;
};

class BlkDB : public Fio {
  Status WriteTo(Stream*, const Fentry& fentry, const Slice& data,
                 uint64_t off);

  Status ReadFrom(Stream*, const Fentry& fentry, Slice* result, uint64_t off,
                  uint64_t size, char* scratch);

  static const KeyType kHeaderType = kDataDesType;

 public:
  BlkDB(const BlkDBOptions&);
  virtual ~BlkDB();

  virtual Status Creat(const Fentry&, bool o_append, Handle**);
  virtual Status Open(const Fentry&, bool o_creat, bool o_trunc, bool o_append,
                      uint64_t* mtime, uint64_t* size, Handle**);
  virtual Status Fstat(const Fentry&, Handle*, uint64_t* mtime, uint64_t* size,
                       bool skip_cache = false);

  virtual Status Write(const Fentry&, Handle*, const Slice& data);
  virtual Status Pwrite(const Fentry&, Handle*, const Slice& data,
                        uint64_t off);
  virtual Status Read(const Fentry&, Handle*, Slice* result, uint64_t size,
                      char* scratch);
  virtual Status Pread(const Fentry&, Handle*, Slice* result, uint64_t off,
                       uint64_t size, char* scratch);
  virtual Status Ftrunc(const Fentry&, Handle*, uint64_t size);
  virtual Status Flush(const Fentry&, Handle*, bool force_sync = false);
  virtual Status Close(const Fentry&, Handle*);

  virtual Status Trunc(const Fentry&, uint64_t size);
  virtual Status Stat(const Fentry&, uint64_t* mtime, uint64_t* size);
  virtual Status Drop(const Fentry&);

 private:
  // No copying allowed
  void operator=(const BlkDB&);
  BlkDB(const BlkDB&);
  port::Mutex mutex_;

  // Constant after construction
  int uniquefier_;
  bool sync_;
  bool verify_checksum_;
  bool owns_db_;
  DB* db_;
};

}  // namespace pdlfs
