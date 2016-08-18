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
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/rpc.h"
#if defined(MARGO) && defined(MERCURY)
#include "margo_rpc.h"
#endif
#if defined(MERCURY)
#include "mercury_rpc.h"
#endif

namespace pdlfs {

RPCOptions::RPCOptions()
    : impl(kMercuryRPC),
      mode(kServerClient),
      rpc_timeout(5000000LLU),
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

namespace {
#if defined(MARGO) && defined(MERCURY)
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
#if defined(MERCURY)
class MercuryRPCImpl : public RPC {
  MercuryRPC::LocalLooper* looper_;
  MercuryRPC* rpc_;

 public:
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

#if defined(MERCURY)
/* clang-format off */

// plugin | protocol          | initialization format       | lookup format
// ------ | --------          | ---------------------       | -------------
// bmi    | tcp               | `bmi+tcp://<port>`          | `[bmi+]tcp://<hostname>:<port>`
// cci    | tcp, verbs, gni   | `cci+<protocol>[://<port>]` | `[cci+]<protocol>://<hostname>:<port>`
// cci    | sm                | `cci+sm[://<id>/<id>]`      | `[cci+]sm://<cci shmem path>/<id>/<id>`
// mpi    | dynamic, static   | `mpi+<protocol>`            | `[mpi+]<protocol>://<port>`

/* clang-format on */
// Convert from a lookup format to its initialization format.
static void ConvertUri(RPCOptions* options) {
  // XXX: current implementation only works for bmi-based uri strings
  std::string new_url;
  size_t pos1 = options->uri.find("://");
  new_url += options->uri.substr(0, pos1 + 3);
  size_t pos2 = options->uri.rfind(":");
  new_url += options->uri.substr(pos2 + 1);
  options->uri = new_url;
}
#endif

RPC* RPC::Open(const RPCOptions& raw_options) {
  assert(raw_options.uri.size() != 0);
  assert(raw_options.mode != kServerClient || raw_options.fs != NULL);
  RPCOptions options(raw_options);
  if (options.env == NULL) {
    options.env = Env::Default();
  }
#if defined(MERCURY)
  if (options.impl == kMercuryRPC || options.impl == kMargoRPC) {
    ConvertUri(&options);
  }
#endif
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
#if defined(MARGO) && defined(MERCURY)
  if (options.impl == kMargoRPC) {
    rpc = new rpc::MargoRPCImpl(options);
  }
#elif defined(MERCURY)
  if (options.impl == kMercuryRPC) {
    rpc = new rpc::MercuryRPCImpl(options);
  }
#endif
  if (rpc == NULL) {
    char msg[] = "No rpc implementation is available\n";
    fwrite(msg, 1, sizeof(msg), stderr);
    abort();
  } else {
    return rpc;
  }
}

}  // namespace pdlfs
