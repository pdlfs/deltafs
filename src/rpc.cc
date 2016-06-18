/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "rpc.h"
#include "mercury_rpc.h"

namespace pdlfs {

RPC::~RPC() {}

namespace rpc {

If::~If() {}

IfWrapper::~IfWrapper() {}

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
}  // namespace rpc

RPC* RPC::Open(const RPCOptions& options) {
  RPC* r = new rpc::RPCImpl(options);
  return r;
}

}  // namespace pdlfs
