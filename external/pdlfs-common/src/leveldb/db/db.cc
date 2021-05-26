/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */
#include "pdlfs-common/leveldb/db.h"

#include "pdlfs-common/leveldb/filenames.h"
#include "pdlfs-common/leveldb/options.h"
#include "pdlfs-common/leveldb/snapshot.h"
#include "pdlfs-common/leveldb/write_batch.h"

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
  if (!env) env = Env::Default();
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname.c_str(), &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  std::string lockname = LockFileName(dbname);
  FileLock* lock = NULL;
  Status result;
  if (!options.skip_lock_file) {
    result = env->LockFile(lockname.c_str(), &lock);
  }
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        const std::string fname = dbname + "/" + filenames[i];
        Status del = env->DeleteFile(fname.c_str());
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }

    // Ignore error since state is already gone
    if (lock) {
      env->UnlockFile(lock);
    }
    env->DeleteFile(lockname.c_str());
    // Ignore error in case dir contains other files
    env->DeleteDir(dbname.c_str());
  }

  return result;
}

}  // namespace pdlfs
