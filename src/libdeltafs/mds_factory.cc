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
  options.uri = topo.rpc_proto;  // such as bmi+tcp, mpi
  rpc_ = RPC::Open(options);
  std::string full_uri = options.uri;
  if (!options.uri.empty()) {
    full_uri.append("://");
  }
  size_t prefix = full_uri.size();
  const std::vector<std::string>* addrs = &topo.srv_addrs;
  std::vector<std::string>::const_iterator it;
  for (it = addrs->begin(); it != addrs->end(); ++it) {
    const std::string* uri = &(*it);
    // Add RPC proto prefix if necessary
    if (!options.uri.empty() && !Slice(*it).starts_with(options.uri)) {
      full_uri.resize(prefix);
      full_uri.append(*it);
      uri = &full_uri;
    }
    AddTarget(*uri, topo.rpc_tracing);
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

void MDSFactoryImpl::AddTarget(const std::string& target_uri, bool trace) {
  StubInfo info;
  assert(rpc_ != NULL);
  info.stub = rpc_->OpenClientFor(target_uri);
  info.wrapper = new MDSWrapper(info.stub);
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
