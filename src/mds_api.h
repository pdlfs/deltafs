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

#include "mds_types.h"
#include "rpc.h"

namespace pdlfs {

class MDS {
 public:
  struct RPC;
  struct Redirect {};

  MDS() {}
  virtual ~MDS();

  struct FstatOptions {
    uint64_t dir_ino;   // Parent directory id
    char name_hash[8];  // Name hash
    Slice name;
  };
  struct FstatRet {
    Stat stat;
  };
  virtual Status Fstat(const FstatOptions& options, FstatRet* ret) = 0;

  struct FcreatOptions {
    uint64_t dir_ino;   // Parent directory id
    char name_hash[8];  // Name hash
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
    Slice name;  // File name
  };
  struct FcreatRet {
    Stat stat;
  };
  virtual Status Fcreat(const FcreatOptions& options, FcreatRet* ret) = 0;

  struct LookupOptions {
    uint64_t dir_ino;   // Parent directory id
    char name_hash[8];  // Name hash
    Slice name;
  };
  struct LookupRet {
    DirEntry entry;
  };
  virtual Status Lookup(const LookupOptions& options, LookupRet* ret) = 0;

  struct ListdirOptions {
    uint64_t dir_ino;  // Parent directory id
  };
  struct ListdirRet {
    std::vector<std::string> names;
  };
  virtual Status Listdir(const ListdirOptions& options, ListdirRet* ret) = 0;

 private:
  // No copying allowed
  void operator=(const MDS&);
  MDS(const MDS&);
};

// RPC adaptors
struct MDS::RPC {
  class CLI;  // MDS on top of RPC
  class SRV;  // RPC on top of MDS

  static char* EncodeHash(char* dst, const char (&hash)[8]) {
    memcpy(dst, hash, sizeof(hash));
    dst += sizeof(hash);
    return dst;
  }

  static bool GetHash(Slice* input, char (&hash)[8]) {
    if (input->size() < sizeof(hash)) {
      return false;
    } else {
      memcpy(hash, input->data(), sizeof(hash));
      input->remove_prefix(sizeof(hash));
      return true;
    }
  }
};

class MDS::RPC::CLI : public MDS {
  typedef rpc::If::Message Msg;

 public:
  CLI(rpc::If* stub);
  virtual ~CLI();

#define DEC_OP(OP) virtual Status OP(const OP##Options&, OP##Ret*);

  DEC_OP(Fstat)
  DEC_OP(Fcreat)
  DEC_OP(Lookup)
  DEC_OP(Listdir)

#undef DEC_OP

 private:
  rpc::If* stub_;
};

class MDS::RPC::SRV : public rpc::If {
  typedef rpc::If::Message Msg;

 public:
  SRV(MDS* mds);
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
