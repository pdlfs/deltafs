#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <abt.h>
#include <margo.h>
#include "mercury_rpc.h"

namespace pdlfs {
namespace rpc {

class MargoRPC {
 public:
  typedef MercuryRPC::AddrEntry AddrEntry;
  typedef MercuryRPC::Addr Addr;
  void Release(AddrEntry* entry) { hg_->Release(entry); }
  hg_return_t Lookup(const std::string& addr, AddrEntry** result);
  MargoRPC(bool listen, const RPCOptions& options);
  void Unref();
  void Ref();

  MercuryRPC* hg_;
  margo_instance_id margo_id_;
  hg_id_t hg_rpc_id_;
  void RegisterRPC();
  Status Start();
  Status Stop();

  class Client;

 private:
  ~MargoRPC();
  // No copying allowed
  void operator=(const MargoRPC&);
  MargoRPC(const MargoRPC&);

  friend class Client;
  int num_io_threads_;
  uint64_t rpc_timeout_;
  port::Mutex mutex_;
  int refs_;
};

// ====================
// Margo client
// ====================

class MargoRPC::Client : public If {
 public:
  explicit Client(MargoRPC* rpc, const std::string& addr)
      : rpc_(rpc), addr_(addr) {
    rpc_->Ref();
  }

  // Return OK on success, a non-OK status on RPC errors.
  virtual Status Call(Message& in, Message& out) RPCNOEXCEPT;

  virtual ~Client() {
    if (rpc_ != NULL) {
      rpc_->Unref();
    }
  }

 private:
  MargoRPC* rpc_;
  std::string addr_;
  // No copying allowed
  void operator=(const Client&);
  Client(const Client&);
};

}  // namespace rpc
}  // namespace pdlfs
