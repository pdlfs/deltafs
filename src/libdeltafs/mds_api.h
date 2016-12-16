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

#include "deltafs/deltafs_api.h"
#include "pdlfs-common/fstypes.h"
#include "pdlfs-common/logging.h"
#include "pdlfs-common/mdb.h"
#include "pdlfs-common/rpc.h"
#include "pdlfs-common/strutil.h"

namespace pdlfs {

class Env;
class Fio;
class MDB;

#define DELTAFS_MAX_MICROS ((uint64_t(1) << 63) - 1) /* Max future */

#define DELTAFS_NON_MOD (~uint32_t(0)) /* Invalid file mode */

#define DELTAFS_NON_UID (~uint32_t(0)) /* Invalid uid */
#define DELTAFS_NON_GID (~uint32_t(0)) /* Invalid gid */

#define DELTAFS_NAME_HASH_BUFSIZE 24

#define DELTAFS_NAME_MAX 255 /* Max number of chars in a file name */

struct MDSEnv {
  std::string output_conf;
  std::string input_conf;
  std::string env_name;
  std::string env_conf;
  std::string fio_name;
  std::string fio_conf;
  Env* env;
  Fio* fio;
};

struct MDSOptions {
  MDSOptions();
  MDSEnv* mds_env;
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
  struct BaseRet {};
  struct BaseOptions {
    DirId dir_id;  // Parent directory id
    uint32_t session_id;
    uint64_t op_due;
    Slice name_hash;
    Slice name;
  };

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
    uint32_t flags;
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
  };
  MDS_OP_RET(Fcreat) {
    unsigned char created;
    Stat stat;
  };
  MDS_OP(Fcreat)

  MDS_OP_OPTIONS(Mkdir) {
    uint32_t flags;
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
  };
  MDS_OP_RET(Mkdir) { Stat stat; };
  MDS_OP(Mkdir)

  MDS_OP_OPTIONS(Chmod) { uint32_t mode; };
  MDS_OP_RET(Chmod) { Stat stat; };
  MDS_OP(Chmod)

  MDS_OP_OPTIONS(Chown) {
    uint32_t uid;
    uint32_t gid;
  };
  MDS_OP_RET(Chown) { Stat stat; };
  MDS_OP(Chown)

  MDS_OP_OPTIONS(Uperm) {
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
  };
  MDS_OP_RET(Uperm) { Stat stat; };
  MDS_OP(Uperm)

  MDS_OP_OPTIONS(Utime) {
    uint64_t atime;
    uint64_t mtime;
  };
  MDS_OP_RET(Utime) { Stat stat; };
  MDS_OP(Utime)

  MDS_OP_OPTIONS(Trunc) {
    uint64_t mtime;
    uint64_t size;
  };
  MDS_OP_RET(Trunc) { Stat stat; };
  MDS_OP(Trunc)

  MDS_OP_OPTIONS(Unlink) { uint32_t flags; };
  MDS_OP_RET(Unlink) { Stat stat; };
  MDS_OP(Unlink)

  MDS_OP_OPTIONS(Lookup){};
  MDS_OP_RET(Lookup) { LookupStat stat; };
  MDS_OP(Lookup)

  MDS_OP_OPTIONS(Listdir){};
  MDS_OP_RET(Listdir) { std::vector<std::string>* names; };
  MDS_OP(Listdir)

  MDS_OP_OPTIONS(Readidx){};
  MDS_OP_RET(Readidx) { std::string idx; };
  MDS_OP(Readidx)

  // -------------
  // MDS admin interface
  // -------------

  MDS_OP_OPTIONS(Opensession){};
  MDS_OP_RET(Opensession) {
    std::string env_name;
    std::string env_conf;
    std::string fio_name;
    std::string fio_conf;
    uint32_t session_id;
  };
  MDS_OP(Opensession)

  MDS_OP_OPTIONS(Getinput){};
  MDS_OP_RET(Getinput) { std::string info; };
  MDS_OP(Getinput)

  MDS_OP_OPTIONS(Getoutput){};
  MDS_OP_RET(Getoutput) { std::string info; };
  MDS_OP(Getoutput)

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
  DEF_OP(Chown)
  DEF_OP(Uperm)
  DEF_OP(Utime)
  DEF_OP(Trunc)
  DEF_OP(Unlink)
  DEF_OP(Lookup)
  DEF_OP(Listdir)
  DEF_OP(Readidx)
  DEF_OP(Opensession)
  DEF_OP(Getinput)
  DEF_OP(Getoutput)

#undef DEF_OP

 protected:
  MDS* base_;
};

// Log every RPC message to assist debugging.
class MDSTracer : public MDSWrapper {
  void Trace(const char* type, const char* op, const std::string& pid,
             const std::string& hash, const std::string& name,
             const Status& status) {
#if VERBOSE >= 1
    Verbose(__LOG_ARGS__, 1, "%s %s) %s) %sN[%s]H[%s] - %s", type, uri_.c_str(),
            op, pid.c_str(), name.c_str(), hash.c_str(),
            status.ToString().c_str());
#endif
  }

 public:
  explicit MDSTracer(const std::string& uri, MDS* base)
      : MDSWrapper(base), uri_(uri) {}
  virtual ~MDSTracer();

#define DEF_OP(OP)                                              \
  virtual Status OP(const OP##Options& options, OP##Ret* ret) { \
    Status s;                                                   \
    std::string pid = options.dir_id.DebugString();             \
    std::string h = EscapeString(options.name_hash);            \
    std::string n = options.name.ToString();                    \
    Trace(">>", #OP, pid, h, n, s);                             \
    try {                                                       \
      s = base_->OP(options, ret);                              \
    } catch (Redirect & re) {                                   \
      s = Status::TryAgain("redirected");                       \
      Trace("<<", #OP, pid, h, n, s);                           \
      throw re;                                                 \
    }                                                           \
    Trace("<<", #OP, pid, h, n, s);                             \
    return s;                                                   \
  }

  DEF_OP(Opensession)
  DEF_OP(Getinput)
  DEF_OP(Getoutput)
  DEF_OP(Fstat)
  DEF_OP(Fcreat)
  DEF_OP(Mkdir)
  DEF_OP(Chmod)
  DEF_OP(Chown)
  DEF_OP(Uperm)
  DEF_OP(Utime)
  DEF_OP(Trunc)
  DEF_OP(Unlink)
  DEF_OP(Lookup)
  DEF_OP(Listdir)
  DEF_OP(Readidx)

#undef DEF_OP

 private:
  std::string uri_;
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
  rpc::If* stub_;
};

class MDS::RPC::SRV : public rpc::If {
  typedef rpc::If::Message Msg;

 public:
  // Always return OK.
  virtual Status Call(Msg& in, Msg& out) RPCNOEXCEPT;
  SRV(MDS* mds) : mds_(mds) {}
  virtual ~SRV();

#define DEC_RPC(OP) void OP(Msg& in, Msg& out);

  DEC_RPC(FSTAT)
  DEC_RPC(MKDIR)
  DEC_RPC(FCRET)
  DEC_RPC(CHMOD)
  DEC_RPC(CHOWN)
  DEC_RPC(UPERM)
  DEC_RPC(UTIME)
  DEC_RPC(TRUNC)
  DEC_RPC(UNLNK)
  DEC_RPC(LOKUP)
  DEC_RPC(LSDIR)
  DEC_RPC(RDIDX)
  DEC_RPC(OPSES)
  DEC_RPC(GINPT)
  DEC_RPC(GOUPT)

#undef DEC_RPC

 private:
  MDS* mds_;
};

}  // namespace pdlfs
