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

#include "pdlfs-common/rpc.h"
#include "mercury_rpc.h"

namespace pdlfs {

RPCOptions::RPCOptions()
    : mode(kServerClient),
      num_io_threads_(1),  // TODO: can we really use multiple I/O threads?
      extra_workers(NULL),
      fs(NULL),
      env(NULL) {}

RPC::~RPC() {}

namespace rpc {

If::~If() {}

IfWrapper::~IfWrapper() {}

#if defined(MERCURY)
class RPCImpl : public RPC {
  MercuryRPC::LocalLooper* looper_;
  MercuryRPC* rpc_;

 public:
  virtual Status Start() { return looper_->Start(); }
  virtual Status Stop() { return looper_->Stop(); }

  virtual If* NewClient(const std::string& addr) {
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
  assert(raw_options.fs != NULL && !raw_options.uri.empty());
  RPCOptions options(raw_options);
  if (options.env == NULL) {
    options.env = Env::Default();
  }
#if defined(MERCURY)
  return new rpc::RPCImpl(options);
#else
  char msg[] = "Not possible: no rpc impl available\n";
  fwrite(msg, 1, sizeof(msg), stderr);
  abort();
#endif
}

}  // namespace pdlfs
