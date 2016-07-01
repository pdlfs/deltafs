#pragma once

/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <set>

#include "mds_api.h"
#include "pdlfs-common/dcntl.h"
#include "pdlfs-common/lease.h"
#include "pdlfs-common/mdb.h"
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
  DEC_OP(Lookup)
  DEC_OP(Listdir)

#undef DEC_OP
  static int PickupServer(uint64_t dir_ino);

 private:
  Status LoadDir(uint64_t ino, DirInfo* info, DirIndex* index);
  Status FetchDir(uint64_t ino, Dir::Ref** ref);
  Status ProbeDir(const Dir* dir);

  // Constant after construction
  Env* env_;
  MDB* mdb_;
  DirIndexOptions idx_opts_;
  bool paranoid_checks_;
  uint64_t lease_duration_;
  uint64_t snap_id_;
  uint64_t reg_id_;
  int srv_id_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  LeaseTable* leases_;
  std::set<uint64_t> loading_dirs_;  // Directories being loaded into cache
  port::CondVar loading_cv_;
  DirTable* dirs_;
  void TryReuseIno(uint64_t ino);
  uint64_t NextIno();
  uint64_t ino_;  // The largest we have allocated

  friend class MDS;
  // No copying allowed
  void operator=(const SRV&);
  SRV(const SRV&);
};

}  // namespace pdlfs
