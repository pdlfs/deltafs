#pragma once

/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory
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

  Status Fstat(const Slice& path, Fentry* result = NULL,
               const Fentry* at = NULL);
  Status Fcreat(const Slice& path, mode_t mode, Fentry* result = NULL,
                bool error_if_exists = true, bool* created = NULL,
                const Fentry* at = NULL);
  Status Ftruncate(const Fentry&, uint64_t mtime, uint64_t size);
  Status Mkdir(const Slice& path, mode_t mode, Fentry* result = NULL,
               bool create_if_missing = false, bool error_if_exists = true);
  Status Chmod(const Slice& path, mode_t mode, Fentry* result = NULL);
  Status Chown(const Slice& path, uid_t usr, gid_t grp, Fentry* result = NULL);
  Status Unlink(const Slice& path, Fentry* result = NULL,
                bool error_if_absent = true, const Fentry* at = NULL);
  Status Listdir(const Slice& path, std::vector<std::string>* names);
  Status Accessdir(const Slice& path, int mode);
  Status Access(const Slice& path, int mode);

  uid_t uid() const { return uid_; }
  gid_t gid() const { return gid_; }

  // Convenient methods for general access control
  bool HasAccess(int mode, const Stat* stat);

  bool IsReadOk(const Stat* st);
  bool IsWriteOk(const Stat* st);
  bool IsExecOk(const Stat* st);

 private:
  CLI(const MDSCliOptions&);

#define HELPER(OP) \
  Status _##OP(const DirIndex*, const OP##Options& opts, OP##Ret* ret)

  // REQUIRES: mutex_ has been locked.
  HELPER(Lookup);
  HELPER(Fstat);
  HELPER(Fcreat);
  HELPER(Mkdir);
  HELPER(Chmod);
  HELPER(Chown);
  HELPER(Unlink);

#undef HELPER

  // Result of a successful path resolution
  struct PathInfo {
    DirId pid;
    char tmp[DELTAFS_NAME_HASH_BUFSIZE];
    Slice nhash;  // Empty iff path is root or pseudo root
    Slice name;
    uint64_t lease_due;
    int zserver;
    int depth;
    mode_t mode;
    uid_t uid;
    gid_t gid;
  };
  // If at is not NULL, path resolution will start from that instead of root.
  // If path resolution fails due to a missing parent, the path of that
  // parent will be stored in *missing_parent.
  // REQUIRES: path is not empty and must be absolute
  // REQUIRES: *at must be a directory
  Status ResolvePath(const Slice& path, PathInfo* info, const Fentry* at = NULL,
                     std::string* missing_parent = NULL);
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

  friend class MDS;
  // State below is protected by mutex_
  port::Mutex mutex_;
  LookupCache* lookup_cache_;
  IndexCache* index_cache_;
  // No copying allowed
  void operator=(const CLI&);
  CLI(const CLI&);
};

inline Status FileNameExceeedsLimit() {
  return Status::InvalidArgument("file name too long");
}

}  // namespace pdlfs
