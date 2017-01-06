#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "mds_cli.h"

namespace pdlfs {

struct MDSTopology {
  bool mds_tracing;
  std::string rpc_proto;
  std::vector<std::string> srv_addrs;
  int num_vir_srvs;
  int num_srvs;
};

class MDSFactoryImpl : public MDSFactory {
  typedef MDS::RPC::CLI MDSWrapper;  // convert RPC to MDS...
  struct StubInfo {
    MDS* mds;
    MDSWrapper* wrapper;
    rpc::If* stub;
  };

 public:
  virtual MDS* Get(size_t srv_id);
  explicit MDSFactoryImpl(Env* env = NULL) : env_(env), rpc_(NULL) {}
  virtual ~MDSFactoryImpl();
  Status Init(const MDSTopology&);
  Status Start();
  Status Stop();

 private:
  // No copying allowed
  void operator=(const MDSFactoryImpl&);
  MDSFactoryImpl(const MDSFactoryImpl&);

  Env* env_;  // okay to be NULL
  void AddTarget(const std::string& uri, bool trace);
  std::vector<StubInfo> stubs_;
  RPC* rpc_;
};

}  // namespace pdlfs
