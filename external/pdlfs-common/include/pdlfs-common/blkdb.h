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
struct Stream {
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
  Status WriteTo(Stream*, const Slice& fentry, const Slice& data, uint64_t off);

  Status ReadFrom(Stream*, const Slice& fentry, Slice* result, uint64_t off,
                  uint64_t size, char* scratch);

  static const KeyType kHeaderType = kDataDesType;

 public:
  BlkDB(const BlkDBOptions&);
  virtual ~BlkDB();

  virtual Status Creat(const Slice& fentry, Handle** fh);
  virtual Status Open(const Slice& fentry, bool create_if_missing,
                      bool truncate_if_exists, uint64_t* mtime, uint64_t* size,
                      Handle** fh);
  virtual Status Fstat(const Slice& fentry, Handle* fh, uint64_t* mtime,
                       uint64_t* size, bool skip_cache = false);
  virtual Status Write(const Slice& fentry, Handle* fh, const Slice& data);
  virtual Status Pwrite(const Slice& fentry, Handle* fh, const Slice& data,
                        uint64_t off);
  virtual Status Read(const Slice& fentry, Handle* fh, Slice* result,
                      uint64_t size, char* scratch);
  virtual Status Pread(const Slice& fentry, Handle* fh, Slice* result,
                       uint64_t off, uint64_t size, char* scratch);
  virtual Status Ftruncate(const Slice& fentry, Handle* fh, uint64_t size);
  virtual Status Flush(const Slice& fentry, Handle* fh,
                       bool force_sync = false);
  virtual Status Close(const Slice& fentry, Handle* fh);

  virtual Status Truncate(const Slice& fentry, uint64_t size);
  virtual Status Stat(const Slice& fentry, uint64_t* mtime, uint64_t* size);
  virtual Status Drop(const Slice& fentry);

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
