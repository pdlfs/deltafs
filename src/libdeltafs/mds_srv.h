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
#define DEC_OP(OP) virtual Status OP(const OP##Options&, OP##Ret*);

  DEC_OP(Fstat)
  DEC_OP(Fcreat)
  DEC_OP(Mkdir)
  DEC_OP(Chmod)
  DEC_OP(Lookup)
  DEC_OP(Listdir)

#undef DEC_OP

 private:
  Status FetchDir(uint64_t ino, Dir::Ref** ref);
  Status ProbeDir(const Dir* dir);

  Env* env_;
  MDB* mdb_;
  DirIndexOptions idx_opts_;
  uint64_t lease_duration_;
  int srv_id_;

  port::Mutex mutex_;
  uint64_t ino_;  // The largest we have allocated
  uint64_t NextIno();
  void TryReuseIno(uint64_t ino);
  LeaseTable* leases_;
  DirTable* dirs_;
  std::set<uint64_t> loading_dirs_;  // Directories being loaded into cache
  port::CondVar loading_cv_;

  // No copying allowed
  void operator=(const SRV&);
  SRV(const SRV&);
};

}  // namespace pdlfs
