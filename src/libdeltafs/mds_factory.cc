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

Status MDSFactoryImpl::Init(const MDSTopology& topo) {
  Status s;
  RPCOptions options;
  options.env = env_;  // okay to be NULL
  options.mode = kClientOnly;
  options.uri = topo.rpc_proto;
  rpc_ = RPC::Open(options);
  const std::vector<std::string>* addrs = &topo.srv_addrs;
  std::vector<std::string>::const_iterator it;
  for (it = addrs->begin(); it != addrs->end(); ++it) {
    AddTarget(*it, topo.rpc_tracing);
  }
  return s;
}

// REQUIRES: Init() has been called before.
Status MDSFactoryImpl::Start() {
  assert(rpc_ != NULL);
  return rpc_->Start();
}

// REQUIRES: Init() has been called before.
Status MDSFactoryImpl::Stop() {
  assert(rpc_ != NULL);
  return rpc_->Stop();
}

void MDSFactoryImpl::AddTarget(const std::string& uri, bool trace) {
  StubInfo info;
  assert(rpc_ != NULL);
  info.stub = rpc_->NewClient(uri);
  info.wrapper = new MDSRPCWrapper(info.stub);
  if (trace) {
    info.mds = new MDSTracer(info.wrapper);
  } else {
    info.mds = info.wrapper;
  }
  stubs_.push_back(info);
}

MDS* MDSFactoryImpl::Get(size_t srv_id) {
  assert(srv_id < stubs_.size());
  return stubs_[srv_id].mds;
}

MDSFactoryImpl::~MDSFactoryImpl() {
  std::vector<StubInfo>::iterator it;
  for (it = stubs_.begin(); it != stubs_.end(); ++it) {
    if (it->mds != it->wrapper) {
      delete it->mds;
    }
    delete it->wrapper;
    delete it->stub;
  }
  delete rpc_;
}

}  // namespace pdlfs
