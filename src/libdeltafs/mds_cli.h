#pragma once

/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "mds_api.h"
#include "pdlfs-common/fio.h"
#include "pdlfs-common/guard.h"
#include "pdlfs-common/index_cache.h"
#include "pdlfs-common/lookup_cache.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

class MDSFactory {
 public:
  virtual MDS* Get(size_t srv_id) = 0;

 protected:
  virtual ~MDSFactory();
};

struct MDSCliOptions {
  MDSCliOptions();
  Env* env;
  MDSFactory* factory;
  size_t index_cache_size;
  size_t lookup_cache_size;
  bool paranoid_checks;
  bool atomic_path_resolution;
  int max_redirects_allowed;
  int num_virtual_servers;
  int num_servers;
  int session_id;
  int cli_id;
  int uid;
  int gid;
};

class MDS::CLI {
 public:
  static CLI* Open(const MDSCliOptions&);
  ~CLI();

  Status Fstat(const Slice& path, Fentry*);
  Status Fcreat(const Slice& path, bool error_if_exists, int mode, Fentry*);
  Status Ftruncate(const Fentry&, uint64_t mtime, uint64_t size);
  Status Mkdir(const Slice& path, int mode, Fentry*);
  Status Chmod(const Slice& path, int mode, Fentry*);
  Status Unlink(const Slice& path, bool error_not_found, Fentry*);
  Status Listdir(const Slice& path, std::vector<std::string>* names);
  Status Accessdir(const Slice& path, int mode);

  int uid() const { return uid_; }
  int gid() const { return gid_; }

 private:
  CLI(const MDSCliOptions&);

// REQUIRES: mutex_ has been locked.
#define HELPER(OP) \
  Status _##OP(const DirIndex*, const OP##Options& opts, OP##Ret* ret)

  HELPER(Lookup);
  HELPER(Fstat);
  HELPER(Fcreat);
  HELPER(Mkdir);
  HELPER(Chmod);
  HELPER(Unlink);

#undef HELPER

  struct PathInfo {
    DirId pid;
    Slice name;
    uint64_t lease_due;
    int zserver;
    int depth;
    mode_t mode;
    uid_t uid;
    gid_t gid;
  };
  Status ResolvePath(const Slice& path, PathInfo*);

  // Convenient methods for permission checking
  bool IsReadDirOk(const PathInfo*);
  bool IsWriteDirOk(const PathInfo*);
  bool IsLookupOk(const PathInfo*);

  typedef LookupCache::Handle LookupHandle;
  Status Lookup(const DirId&, const Slice& name, int zserver, uint64_t op_due,
                LookupHandle**);
  typedef IndexCache::Handle IndexHandle;
  Status FetchIndex(const DirId&, int zserver, IndexHandle**);
  typedef RefGuard<IndexCache, IndexHandle> IndexGuard;

  // Constant after construction
  Env* env_;
  MDSFactory* factory_;
  typedef DirIndexOptions GIGA;
  GIGA giga_;
  bool paranoid_checks_;
  bool atomic_path_resolution_;
  int max_redirects_allowed_;
  int session_id_;
  int cli_id_;
  int uid_;
  int gid_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  LookupCache* lookup_cache_;
  IndexCache* index_cache_;

  friend class MDS;
  // No copying allowed
  void operator=(const CLI&);
  CLI(const CLI&);
};

}  // namespace pdlfs
