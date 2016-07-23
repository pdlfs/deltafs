/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdio.h>
#include <stdlib.h>
#include <vector>

#include "pdlfs-common/logging.h"
#include "pdlfs-common/rpc.h"
#if defined(MERCURY)
#include "mercury_rpc.h"
#endif

namespace pdlfs {

RPCOptions::RPCOptions()
    : mode(kServerClient),
      num_io_threads(1),  // TODO: can we really use multiple I/O threads?
      extra_workers(NULL),
      fs(NULL),
      env(NULL) {}

RPC::~RPC() {}

RPCServer::~RPCServer() {
  std::vector<RPCInfo>::iterator it;
  for (it = rpcs_.begin(); it != rpcs_.end(); ++it) {
    delete it->rpc;
    delete it->pool;
  }
}

void RPCServer::AddChannel(const std::string& listening_uri, int workers) {
  RPCInfo info;
  RPCOptions options;
  options.env = env_;
  info.pool = ThreadPool::NewFixed(workers);
  options.extra_workers = info.pool;
  options.fs = fs_;
  options.uri = listening_uri;
  info.rpc = RPC::Open(options);
  rpcs_.push_back(info);
}

Status RPCServer::Start() {
  Status s;
  std::vector<RPCInfo>::iterator it;
  for (it = rpcs_.begin(); it != rpcs_.end(); ++it) {
    s = it->rpc->Start();
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

Status RPCServer::Stop() {
  Status s;
  std::vector<RPCInfo>::iterator it;
  for (it = rpcs_.begin(); it != rpcs_.end(); ++it) {
    s = it->rpc->Stop();
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

namespace rpc {

If::~If() {}

#if defined(MERCURY)
class RPCImpl : public RPC {
  MercuryRPC::LocalLooper* looper_;
  MercuryRPC* rpc_;

 public:
  virtual Status Start() { return looper_->Start(); }
  virtual Status Stop() { return looper_->Stop(); }

  virtual If* OpenClientStub(const std::string& addr) {
    return new MercuryRPC::Client(rpc_, addr);
  }

  RPCImpl(const RPCOptions& options) {
    rpc_ = new MercuryRPC(options.mode == kServerClient, options);
    looper_ = new MercuryRPC::LocalLooper(rpc_, options);
    rpc_->Ref();
  }

  virtual ~RPCImpl() {
    delete looper_;
    rpc_->Unref();
  }
};
#endif

}  // namespace rpc

RPC* RPC::Open(const RPCOptions& raw_options) {
  assert(raw_options.mode != kServerClient || raw_options.fs != NULL);
  assert(!raw_options.uri.empty());
  RPCOptions options(raw_options);
  if (options.env == NULL) {
    options.env = Env::Default();
  }
#if VERBOSE >= 1
  Verbose(__LOG_ARGS__, 1, "rpc.uri=%s", options.uri.c_str());
  Verbose(__LOG_ARGS__, 1, "rpc.num_io_threads=%d", options.num_io_threads);
#endif
#if defined(MERCURY)
  return new rpc::RPCImpl(options);
#else
  char msg[] = "Not possible: no rpc impl available\n";
  fwrite(msg, 1, sizeof(msg), stderr);
  abort();
#endif
}

}  // namespace pdlfs
