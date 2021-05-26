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
#include "pdlfs-common/rpc.h"

#include "posix/posix_rpc.h"

#include "pdlfs-common/env.h"
#include "pdlfs-common/pdlfs_config.h"

#include <assert.h>
#include <stdlib.h>
#include <vector>

#if defined(PDLFS_MARGO_RPC)
#include "margo/margo_rpc.h"
#endif

#if defined(PDLFS_MERCURY_RPC)
#include "mercury/mercury_rpc.h"
#endif

namespace pdlfs {

RPCOptions::RPCOptions()
    : impl(rpc::kSocketRPC),
      mode(rpc::kServerClient),
      rpc_timeout(5000000),
      num_rpc_threads(1),
      extra_workers(NULL),
      env(NULL),
      info_log(NULL),
      fs(NULL),
      addr_cache_size(128),
      udp_max_unexpected_msgsz(1432),
      udp_max_expected_msgsz(1432),
      udp_srv_rcvbuf(-1),
      udp_srv_sndbuf(-1) {}

int RPC::GetPort() { return -1; }

std::string RPC::GetUsageInfo() { return ""; }

std::string RPC::GetUri() { return std::string("-1:-1"); }

Status RPC::status() { return Status::OK(); }

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
}  // namespace

namespace {
#if defined(PDLFS_MERCURY_RPC)
class MercuryRPCImpl : public RPC {
  MercuryRPC::LocalLooper* looper_;
  MercuryRPC* rpc_;

 public:
  virtual int GetPort() { return RPC::GetPort(); }
  virtual std::string GetUsageInfo() { return RPC::GetUsageInfo(); }
  virtual std::string GetUri() { return RPC::GetUri(); }
  virtual Status status() { return rpc_->status(); }
  virtual Status Start() { return looper_->Start(); }
  virtual Status Stop() { return looper_->Stop(); }

  virtual If* OpenStubFor(const std::string& addr) {
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
}  // namespace

}  // namespace rpc

RPC* RPC::Open(const RPCOptions& raw_options) {
  assert(raw_options.uri.size() != 0);
  assert(raw_options.mode != rpc::kServerClient || raw_options.fs != NULL);
  RPCOptions options(raw_options);
  if (!options.info_log) {
    options.info_log = Logger::Default();
  }
  if (!options.env) {
    options.env = Env::Default();
  }
#if VERBOSE >= 3
  Log(options.info_log, 3, "rpc.uri -> %s", options.uri.c_str());
  Log(options.info_log, 3, "rpc.timeout -> %llu (microseconds)",
      static_cast<unsigned long long>(options.rpc_timeout));
  Log(options.info_log, 3, "rpc.num_io_threads -> %d", options.num_rpc_threads);
  Log(options.info_log, 3, "rpc.extra_workers -> [%s]",
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
  if (options.impl == rpc::kMercuryRPC) {
    rpc = new rpc::MercuryRPCImpl(options);
  }
#endif
  if (options.impl == rpc::kSocketRPC) {
    rpc = new PosixRPC(options);
  }
  if (rpc == NULL) {
    char msg[] = "The requested rpc impl is not available\n";
    fwrite(msg, 1, sizeof(msg), stderr);
    fflush(stderr);
    abort();
  } else {
    return rpc;
  }
}

}  // namespace pdlfs
