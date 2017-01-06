/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/leveldb/db/db.h"
#include "pdlfs-common/leveldb/db/options.h"
#include "pdlfs-common/leveldb/db/snapshot.h"
#include "pdlfs-common/leveldb/db/write_batch.h"

#include "pdlfs-common/dbfiles.h"
#include "pdlfs-common/env.h"

namespace pdlfs {

DB::~DB() {}

Snapshot::~Snapshot() {}

SnapshotImpl::~SnapshotImpl() {}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

Status DestroyDB(const std::string& dbname, const DBOptions& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }

    // Ignore error since state is already gone
    env->UnlockFile(lock);
    env->DeleteFile(lockname);

    // Ignore error in case dir contains other files
    env->DeleteDir(dbname);
  }
  return result;
}

}  // namespace pdlfs
