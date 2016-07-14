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
#include "pdlfs-common/logging.h"
#include "pdlfs-common/mdb.h"
#include "pdlfs-common/rpc.h"
#include "pdlfs-common/strutil.h"

namespace pdlfs {

// The farthest time in future.
static const uint64_t kMaxMicros = ((0x1ull << 63) - 1);

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
  // Common input/output parameters shared by all RPC calls.
  struct BaseOptions {
    DirId dir_id;  // Parent directory id
    uint32_t session_id;
    uint64_t op_due;
    Slice name_hash;
    Slice name;
  };
  struct BaseRet {};

#define MDS_OP(OP) virtual Status OP(const OP##Options&, OP##Ret*) = 0;
#define MDS_OP_OPTIONS(OP) struct OP##Options : public BaseOptions
#define MDS_OP_RET(OP) struct OP##Ret : public BaseRet

  // -------------
  // MDS interface
  // -------------

  MDS_OP_OPTIONS(Fstat){};
  MDS_OP_RET(Fstat) { Stat stat; };
  MDS_OP(Fstat)

  MDS_OP_OPTIONS(Fcreat) {
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
  };
  MDS_OP_RET(Fcreat) { Stat stat; };
  MDS_OP(Fcreat)

  MDS_OP_OPTIONS(Mkdir) {
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
  };
  MDS_OP_RET(Mkdir) { Stat stat; };
  MDS_OP(Mkdir)

  MDS_OP_OPTIONS(Chmod) { uint32_t mode; };
  MDS_OP_RET(Chmod) { Stat stat; };
  MDS_OP(Chmod)

  MDS_OP_OPTIONS(Lookup){};
  MDS_OP_RET(Lookup) { LookupStat stat; };
  MDS_OP(Lookup)

  MDS_OP_OPTIONS(Listdir){};
  MDS_OP_RET(Listdir) { std::vector<std::string>* names; };
  MDS_OP(Listdir)

  MDS_OP_OPTIONS(Readidx){};
  MDS_OP_RET(Readidx) { std::string idx; };
  MDS_OP(Readidx)

#undef MDS_OP_RET
#undef MDS_OP_OPTIONS
#undef MDS_OP

  static Slice EncodeId(const DirId& id, char* scratch);
  static int PickupServer(const DirId& id);
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
  virtual ~MDSWrapper();

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
  DEF_OP(Readidx)

#undef DEF_OP

 protected:
  MDS* base_;
};

// Log every RPC message to assist debugging.
class MDSTracer : public MDSWrapper {
 public:
  explicit MDSTracer(MDS* base) : MDSWrapper(base) {}
  virtual ~MDSTracer();

#ifndef NTRACE
#define DEF_OP(OP)                                                           \
  virtual Status OP(const OP##Options& options, OP##Ret* ret) {              \
    std::string pid = options.dir_id.DebugString();                          \
    std::string h = EscapeString(options.name_hash);                         \
    std::string n = options.name.ToString();                                 \
    Verbose(__LOG_ARGS__, 5, ">> %s %s/N[%s]H[%s]", #OP, pid.c_str(),        \
            n.c_str(), h.c_str());                                           \
    Status s;                                                                \
    try {                                                                    \
      s = base_->OP(options, ret);                                           \
    } catch (Redirect & re) {                                                \
      Verbose(__LOG_ARGS__, 3, "<< %s %s/N[%s]H[%s]: %s", #OP, pid.c_str(),  \
              n.c_str(), h.c_str(), "Redirected");                           \
      throw re;                                                              \
    }                                                                        \
    Verbose(__LOG_ARGS__, (!s.ok()) ? 1 : 3, "<< %s %s/N[%s]H[%s]: %s", #OP, \
            pid.c_str(), n.c_str(), h.c_str(), s.ToString().c_str());        \
    return s;                                                                \
  }

  DEF_OP(Fstat)
  DEF_OP(Fcreat)
  DEF_OP(Mkdir)
  DEF_OP(Chmod)
  DEF_OP(Lookup)
  DEF_OP(Listdir)
  DEF_OP(Readidx)

#undef DEF_OP
#endif
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
  DEC_OP(Readidx)

#undef DEC_OP

 private:
  rpc::If* stub_;
};

class MDS::RPC::SRV : public rpc::IfWrapper {
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
  DEC_RPC(LOKUP)
  DEC_RPC(LSDIR)
  DEC_RPC(RDIDX)

#undef DEC_RPC

 private:
  MDS* mds_;
};

}  // namespace pdlfs
