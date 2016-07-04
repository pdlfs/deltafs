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

class Env;
class MDB;

struct MDSOptions {
  MDSOptions();
  Env* env;
  MDB* mdb;
  size_t dir_table_size;
  size_t lease_table_size;
  uint64_t lease_duration;
  uint64_t snap_id;
  uint64_t reg_id;
  bool paranoid_checks;
  int num_virtual_servers;
  int num_servers;
  int srv_id;
};

class MDS {
 public:
  MDS() {}
  virtual ~MDS();
  static MDS* Open(const MDSOptions&);
  struct RPC;

  typedef std::string Redirect;
#define MDS_OP(OP) virtual Status OP(const OP##Options&, OP##Ret*) = 0;

  struct FstatOptions {
    uint64_t reg_id;
    uint64_t snap_id;
    uint64_t dir_ino;  // Parent directory id
    uint32_t token;    // Transient client session id
    Slice name_hash;
    Slice name;
  };
  struct FstatRet {
    Stat stat;
  };
  MDS_OP(Fstat)

  struct FcreatOptions {
    uint64_t reg_id;
    uint64_t snap_id;
    uint64_t dir_ino;  // Parent directory id
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
    uint32_t token;  // Transient client session id
    Slice name_hash;
    Slice name;
  };
  struct FcreatRet {
    Stat stat;
  };
  MDS_OP(Fcreat)

  struct MkdirOptions {
    uint64_t reg_id;
    uint64_t snap_id;
    uint64_t dir_ino;  // Parent directory id
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
    uint32_t zserver;
    uint32_t token;  // Transient client session id
    Slice name_hash;
    Slice name;
  };
  struct MkdirRet {
    Stat stat;
  };
  MDS_OP(Mkdir)

  struct ChmodOptions {
    uint64_t reg_id;
    uint64_t snap_id;
    uint64_t dir_ino;  // Parent directory id
    uint32_t mode;
    uint32_t token;  // Transient client session id
    Slice name_hash;
    Slice name;
  };
  struct ChmodRet {
    Stat stat;
  };
  MDS_OP(Chmod)

  struct LookupOptions {
    uint64_t reg_id;
    uint64_t snap_id;
    uint64_t dir_ino;  // Parent directory id
    uint32_t token;    // Transient client session id
    Slice name_hash;
    Slice name;
  };
  struct LookupRet {
    LookupEntry entry;
  };
  MDS_OP(Lookup)

  struct ListdirOptions {
    uint64_t reg_id;
    uint64_t snap_id;
    uint64_t dir_ino;  // Parent directory id
    uint32_t token;    // Transient client session id
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

// Helper class that forwards all calls to another MDS if one exists.
// May be useful to clients who wish to implement just part of the
// functionality of MDS.
class MDSWrapper : public MDS {
 public:
  explicit MDSWrapper(MDS* base = NULL) : base_(base) {}
  ~MDSWrapper();

#define DEF_OP(OP)                                              \
  virtual Status OP(const OP##Options& options, OP##Ret* ret) { \
    if (base_ != NULL) {                                        \
      return base_->OP(options, ret);                           \
    } else {                                                    \
      return Status::NotSupported(Slice());                     \
    }                                                           \
  }

  DEF_OP(Fstat)
  DEF_OP(Fcreat)
  DEF_OP(Mkdir)
  DEF_OP(Chmod)
  DEF_OP(Lookup)
  DEF_OP(Listdir)

#undef DEF_OP

 private:
  MDS* base_;
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
