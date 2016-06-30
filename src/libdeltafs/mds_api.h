#pragma once

/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <string>
#include <vector>

#include "pdlfs-common/fstypes.h"
#include "rpc.h"

namespace pdlfs {

class MDS {
 public:
  MDS() {}
  virtual ~MDS();
  struct RPC;

  typedef std::string Redirect;
#define MDS_OP(OP) virtual Status OP(const OP##Options&, OP##Ret*) = 0;

  struct FstatOptions {
    uint64_t dir_ino;  // Parent directory id
    Slice name_hash;   // Name hash
    Slice name;
  };
  struct FstatRet {
    Stat stat;
  };
  MDS_OP(Fstat)

  struct FcreatOptions {
    uint64_t dir_ino;  // Parent directory id
    Slice name_hash;   // Name hash
    Slice name;
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
  };
  struct FcreatRet {
    Stat stat;
  };
  MDS_OP(Fcreat)

  struct MkdirOptions {
    uint64_t dir_ino;  // Parent directory id
    Slice name_hash;   // Name hash
    Slice name;
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
    uint32_t zserver;
  };
  struct MkdirRet {
    Stat stat;
  };
  MDS_OP(Mkdir)

  struct ChmodOptions {
    uint64_t dir_ino;  // Parent directory id
    Slice name_hash;   // Name hash
    Slice name;
    uint32_t mode;
  };
  struct ChmodRet {
    Stat stat;
  };
  MDS_OP(Chmod)

  struct LookupOptions {
    uint64_t dir_ino;  // Parent directory id
    Slice name_hash;   // Name hash
    Slice name;
  };
  struct LookupRet {
    LookupEntry entry;
  };
  MDS_OP(Lookup)

  struct ListdirOptions {
    uint64_t dir_ino;  // Parent directory id
  };
  struct ListdirRet {
    std::vector<std::string> names;
  };
  MDS_OP(Listdir)

#undef MDS_OP
  class SRV;
  class CLI;

 private:
  // No copying allowed
  void operator=(const MDS&);
  MDS(const MDS&);
};

// RPC adaptors
struct MDS::RPC {
  class CLI;  // MDS on top of RPC
  class SRV;  // RPC on top of MDS
};

class MDS::RPC::CLI : public MDS {
  typedef rpc::If::Message Msg;

 public:
  CLI(rpc::If* stub) : stub_(stub) {}
  virtual ~CLI();

#define DEC_OP(OP) virtual Status OP(const OP##Options&, OP##Ret*);

  DEC_OP(Fstat)
  DEC_OP(Fcreat)
  DEC_OP(Mkdir)
  DEC_OP(Chmod)
  DEC_OP(Lookup)
  DEC_OP(Listdir)

#undef DEC_OP

 private:
  rpc::If* stub_;
};

class MDS::RPC::SRV : public rpc::If {
  typedef rpc::If::Message Msg;

 public:
  SRV(MDS* mds) : mds_(mds) {}
  virtual ~SRV();

#define DEC_RPC(OP) virtual void OP(Msg& in, Msg& out);

  DEC_RPC(NONOP)
  DEC_RPC(FSTAT)
  DEC_RPC(MKDIR)
  DEC_RPC(FCRET)
  DEC_RPC(CHMOD)
  DEC_RPC(CHOWN)
  DEC_RPC(UNLNK)
  DEC_RPC(RMDIR)
  DEC_RPC(RENME)
  DEC_RPC(LOKUP)
  DEC_RPC(LSDIR)

#undef DEC_RPC

 private:
  MDS* mds_;
};

}  // namespace pdlfs
