/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
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
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/rpc.h"

#if defined(PDLFS_MARGO_RPC)
#include "margo_rpc.h"
#endif

#if defined(PDLFS_MERCURY_RPC)
#include "mercury_rpc.h"
#endif

namespace pdlfs {

RPCOptions::RPCOptions()
    : impl(kMercuryRPC),
      mode(kServerClient),
      rpc_timeout(5000000),
      num_io_threads(1),
      extra_workers(NULL),
      addr_cache_size(128),
      env(NULL),
      fs(NULL) {}

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

Status RPCServer::status() const {
  Status s;
  std::vector<RPCInfo>::const_iterator it;
  for (it = rpcs_.begin(); it != rpcs_.end(); ++it) {
    assert(it->rpc != NULL);
    s = it->rpc->status();
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

Status RPCServer::Start() {
  Status s;
  std::vector<RPCInfo>::iterator it;
  for (it = rpcs_.begin(); it != rpcs_.end(); ++it) {
    assert(it->rpc != NULL);
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
    assert(it->rpc != NULL);
    s = it->rpc->Stop();
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

namespace rpc {

If::~If() {}

namespace {
#if defined(PDLFS_MARGO_RPC)
class MargoRPCImpl : public RPC {
  MargoRPC* rpc_;

 public:
  virtual Status Start() { return rpc_->Start(); }
  virtual Status Stop() { return rpc_->Stop(); }

  virtual If* OpenClientFor(const std::string& addr) {
    return new MargoRPC::Client(rpc_, addr);
  }

  MargoRPCImpl(const RPCOptions& options) {
    rpc_ = new MargoRPC(options.mode == kServerClient, options);
    rpc_->Ref();
  }

  virtual ~MargoRPCImpl() { rpc_->Unref(); }
};
#endif
}

namespace {
#if defined(PDLFS_MERCURY_RPC)
class MercuryRPCImpl : public RPC {
  MercuryRPC::LocalLooper* looper_;
  MercuryRPC* rpc_;

 public:
  virtual Status status() const { return rpc_->status(); }
  virtual Status Start() { return looper_->Start(); }
  virtual Status Stop() { return looper_->Stop(); }

  virtual If* OpenClientFor(const std::string& addr) {
    return new MercuryRPC::Client(rpc_, addr);
  }

  MercuryRPCImpl(const RPCOptions& options) {
    rpc_ = new MercuryRPC(options.mode == kServerClient, options);
    looper_ = new MercuryRPC::LocalLooper(rpc_, options);
    rpc_->Ref();
  }

  virtual ~MercuryRPCImpl() {
    rpc_->Unref();
    delete looper_;
  }
};
#endif
}

}  // namespace rpc

RPC* RPC::Open(const RPCOptions& raw_options) {
  assert(raw_options.uri.size() != 0);
  assert(raw_options.mode != kServerClient || raw_options.fs != NULL);
  RPCOptions options(raw_options);
  if (options.env == NULL) {
    options.env = Env::Default();
  }
#if VERBOSE >= 1
  Verbose(__LOG_ARGS__, 1, "rpc.uri -> %s", options.uri.c_str());
  Verbose(__LOG_ARGS__, 1, "rpc.timeout -> %llu (microseconds)",
          (unsigned long long)options.rpc_timeout);
  Verbose(__LOG_ARGS__, 1, "rpc.num_io_threads -> %d", options.num_io_threads);
  Verbose(__LOG_ARGS__, 1, "rpc.extra_workers -> [%s]",
          options.extra_workers != NULL
              ? options.extra_workers->ToDebugString().c_str()
              : "NULL");
#endif
  RPC* rpc = NULL;
#if defined(PDLFS_MARGO_RPC)
  if (options.impl == kMargoRPC) {
    rpc = new rpc::MargoRPCImpl(options);
  }
#endif
#if defined(PDLFS_MERCURY_RPC)
  if (options.impl == kMercuryRPC) {
    rpc = new rpc::MercuryRPCImpl(options);
  }
#endif
  if (rpc == NULL) {
#ifndef NDEBUG
    char msg[] = "No rpc implementation is available\n";
    fwrite(msg, 1, sizeof(msg), stderr);
    abort();
#else
    Error(__LOG_ARGS__, "No rpc implementation is available");
    exit(EXIT_FAILURE);
#endif
  } else {
    return rpc;
  }
}

}  // namespace pdlfs
