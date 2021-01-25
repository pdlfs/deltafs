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
#pragma once

#include "mds_api.h"
#include "util/dcntl.h"
#include "util/lease.h"

#include "pdlfs-common/hashmap.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

class MDS::SRV : public MDS {
 public:
  SRV(const MDSOptions&);
  virtual ~SRV();

#define DEC_OP(OP) virtual Status OP(const OP##Options&, OP##Ret*);

  DEC_OP(Fstat)
  DEC_OP(Fcreat)
  DEC_OP(Mkdir)
  DEC_OP(Chmod)
  DEC_OP(Chown)
  DEC_OP(Uperm)
  DEC_OP(Utime)
  DEC_OP(Trunc)
  DEC_OP(Unlink)
  DEC_OP(Lookup)
  DEC_OP(Listdir)
  DEC_OP(Readidx)
  DEC_OP(Opensession)
  DEC_OP(Getinput)
  DEC_OP(Getoutput)

#undef DEC_OP

 private:
  Status LoadDir(const DirId& id, DirInfo* info, DirIndex* index);
  Status FetchDir(const DirId& id, Dir::Ref** ref);
  Status ProbeDir(const Dir* dir);

  // Constant after construction
  MDSEnv* mds_env_;
  MDB* mdb_;
  typedef DirIndexOptions GIGA;
  GIGA giga_;
  bool paranoid_checks_;
  uint64_t lease_duration_;
  uint64_t snap_id_;
  uint64_t reg_id_;
  int srv_id_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  LeaseTable* leases_;
  HashSet loading_dirs_;  // A set of dirs being loaded into a memory cache
  port::CondVar loading_cv_;
  DirTable* dirs_;
  uint32_t NextSession();
  uint32_t session_;  // The last session id we allocated
  void TryReuseIno(uint64_t ino);
  uint64_t NextIno();
  uint64_t ino_;  // The last ino num we allocated
  Status status_;

  friend class MDS;
  // No copying allowed
  void operator=(const SRV&);
  SRV(const SRV&);
};

inline Status MDS::SRV::Chmod(const ChmodOptions& options, ChmodRet* ret) {
  UpermRet uperm_ret;
  UpermOptions uperm_options;
  *static_cast<BaseOptions*>(&uperm_options) =
      static_cast<const BaseOptions&>(options);
  uperm_options.mode = options.mode;
  uperm_options.uid = DELTAFS_NON_UID;
  uperm_options.gid = DELTAFS_NON_GID;
  Status s = Uperm(uperm_options, &uperm_ret);
  if (s.ok()) {
    ret->stat = uperm_ret.stat;
  }
  return s;
}

inline Status MDS::SRV::Chown(const ChownOptions& options, ChownRet* ret) {
  UpermRet uperm_ret;
  UpermOptions uperm_options;
  *static_cast<BaseOptions*>(&uperm_options) =
      static_cast<const BaseOptions&>(options);
  uperm_options.uid = options.uid;
  uperm_options.gid = options.gid;
  uperm_options.mode = DELTAFS_NON_MOD;
  Status s = Uperm(uperm_options, &uperm_ret);
  if (s.ok()) {
    ret->stat = uperm_ret.stat;
  }
  return s;
}

}  // namespace pdlfs
