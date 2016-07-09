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
#include "pdlfs-common/index_cache.h"
#include "pdlfs-common/lookup_cache.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

class MDSFactory {
 public:
  virtual MDS* Get(int srv_id) = 0;

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
  int cli_id;
  int uid;
  int gid;
};

class MDS::CLI {
 public:
  static CLI* Open(const MDSCliOptions&);
  ~CLI();

  Status Fstat(const Slice& path, Stat* stat);
  Status Fcreat(const Slice& path, int mode, Stat* stat);
  Status Mkdir(const Slice& path, int mode, Stat* stat);
  Status Listdir(const Slice& path, std::vector<std::string>* names);

 private:
  CLI(const MDSCliOptions&);

  struct PathInfo {
    uint64_t lease_due;
    DirId pid;
    Slice name;
    int zserver;
    int depth;
  };
  Status ResolvePath(const Slice& path, PathInfo*);

  typedef LookupCache::Handle LookupHandle;
  Status Lookup(const DirId&, const Slice& name, int zserver, uint64_t op_due,
                LookupHandle**);
  typedef IndexCache::Handle IndexHandle;
  Status FetchIndex(const DirId&, int zserver, IndexHandle**);

  Env* env_;
  MDSFactory* factory_;
  typedef DirIndexOptions GIGA;
  GIGA giga_;
  bool paranoid_checks_;
  bool atomic_path_resolution_;
  int max_redirects_allowed_;
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
