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

struct MDSTopology {
  bool rpc_tracing;
  std::string rpc_proto;
  std::vector<std::string> srv_addrs;
  int num_vir_srvs;
  int num_srvs;
};

class MDSFactoryImpl : public MDSFactory {
  typedef MDS::RPC::CLI MDSRPCWrapper;
  struct StubInfo {
    MDS* mds;
    MDSRPCWrapper* wrapper;
    rpc::If* stub;
  };

 public:
  Status Init(const MDSTopology&);
  Status Start();
  Status Stop();

  virtual MDS* Get(size_t srv_id);
  explicit MDSFactoryImpl(Env* env = NULL) : rpc_(NULL), env_(env) {}
  virtual ~MDSFactoryImpl();

 private:
  // No copying allowed
  void operator=(const MDSFactoryImpl&);
  MDSFactoryImpl(const MDSFactoryImpl&);

  void AddTarget(const std::string& uri, bool trace);
  std::vector<StubInfo> stubs_;
  RPC* rpc_;
  Env* env_;
};

}  // namespace pdlfs
