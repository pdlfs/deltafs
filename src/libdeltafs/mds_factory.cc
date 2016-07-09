/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "mds_factory.h"

namespace pdlfs {

Status RPCMDSFactory::Init(const std::string& base_uri) {
  Status s;
  RPCOptions options;
  options.env = env_;  // okay to be NULL
  options.mode = kClientOnly;
  options.uri = base_uri;
  rpc_ = RPC::Open(options);
  return s;
}

// REQUIRES: Init() has been called before.
Status RPCMDSFactory::Start() {
  assert(rpc_ != NULL);
  return rpc_->Start();
}

// REQUIRES: Init() has been called before.
Status RPCMDSFactory::Stop() {
  assert(rpc_ != NULL);
  return rpc_->Stop();
}

void RPCMDSFactory::AddRPCStub(const std::string& srv_uri) {
  StubInfo info;
  info.stub = rpc_->NewClient(srv_uri);
  info.wrapper = new MDS::RPC::CLI(info.stub);
  stubs_.push_back(info);
}

MDS* RPCMDSFactory::Get(size_t srv_id) {
  assert(srv_id < stubs_.size());
  return stubs_[srv_id].wrapper;
}

RPCMDSFactory::~RPCMDSFactory() {
  std::vector<StubInfo>::iterator it;
  for (it = stubs_.begin(); it != stubs_.end(); ++it) {
    delete it->wrapper;
    delete it->stub;
  }
  delete rpc_;
}

}  // namespace pdlfs
