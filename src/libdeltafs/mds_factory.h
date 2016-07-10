#pragma once

/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "mds_cli.h"

namespace pdlfs {

class RPCMDSFactory : public MDSFactory {
  typedef MDS::RPC::CLI MDSRPCWrapper;
  struct StubInfo {
    rpc::If* stub;
    MDS* wrapper;
    MDS* tracer;
  };

 public:
  virtual MDS* Get(size_t srv_id);
  Status Init(const std::string& base_uri);
  Status Start();
  Status Stop();

  void AddRPCStub(const std::string& srv_uri);
  explicit RPCMDSFactory(Env* env = NULL) : env_(env) {}
  virtual ~RPCMDSFactory();

 private:
  // No copying allowed
  void operator=(const RPCMDSFactory&);
  RPCMDSFactory(const RPCMDSFactory&);

  std::vector<StubInfo> stubs_;
  RPC* rpc_;
  Env* env_;
};

}  // namespace pdlfs
