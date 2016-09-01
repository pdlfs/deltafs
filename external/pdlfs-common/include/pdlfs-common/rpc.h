#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <errno.h>
#include <string>
#include <vector>

#include "pdlfs-common/env.h"
#include "pdlfs-common/status.h"

namespace pdlfs {
// Internal RPC interface
namespace rpc {
class If;
}

enum RPCImpl { kMargoRPC, kMercuryRPC, kThriftRPC };

enum RPCMode { kServerClient, kClientOnly };

struct RPCOptions {
  RPCOptions();
  RPCImpl impl;  // Default: kMercuryRPC
  RPCMode mode;  // Default: kServerClient
  std::string uri;
  uint64_t rpc_timeout;  // In microseconds, Default: 5 secs

  // Total number of threads used to drive RPC work and execute
  // RPC callback functions. RPC implementation may choose to dedicate
  // some of them to only drive RPC work and the rest to
  // execute RPC callback functions.
  int num_io_threads;  // Default: 1

  // If not NULL, RPC callback functions will be redirected to
  // the pool instead of I/O threads for execution.
  ThreadPool* extra_workers;  // Default: NULL

  // Max number of server addrs that may be cached locally
  size_t addr_cache_size;  //  Default: 128
  Env* env;  // Default: NULL, which indicates Env::Default() should be used

  // Server callback implementation.
  // Not needed for clients.
  rpc::If* fs;
};

class RPC {
 public:
  RPC() {}
  virtual ~RPC();

  // RPC implementation should ensure the results of the following calls
  // are thread-safe so that no explicit synchronization is needed
  // to make RPC calls.
  static RPC* Open(const RPCOptions&);

  // The result should be deleted when it is no longer needed.
  virtual rpc::If* OpenClientFor(const std::string& addr) = 0;
  virtual Status status() const { return Status::OK(); }

  // RPC implementation must not use the caller thread to process
  // RPC events. Instead, one or more background looping threads should
  // be created (or destroyed) as a result of the following calls.
  virtual Status Start() = 0;
  virtual Status Stop() = 0;

 private:
  // No copying allowed
  void operator=(const RPC&);
  RPC(const RPC&);
};

// Helper class that binds multiple RPC listening ports to a single
// logical server, with each listening port associated with
// dedicated pools of I/O threads and worker threads.
class RPCServer {
  struct RPCInfo {
    ThreadPool* pool;
    RPC* rpc;
  };

 public:
  Status status() const;
  Status Start();
  Status Stop();

  void AddChannel(const std::string& uri, int workers);
  RPCServer(rpc::If* fs, Env* env = NULL) : fs_(fs), env_(env) {}
  ~RPCServer();

 private:
  // No copying allowed
  void operator=(const RPCServer&);
  RPCServer(const RPCServer&);

  std::vector<RPCInfo> rpcs_;
  rpc::If* fs_;
  Env* env_;
};

namespace rpc {
class If {
 public:
  // Each RPC message contains a chunk of un-structured data.
  // This allows us to port to different RPC frameworks with different
  // type systems.
  struct Message {
    int op;          // Operation type
    int err;         // Error code
    Slice contents;  // Message body
    Message() : op(0), err(0) {}

    char buf[500];  // Avoiding allocating dynamic memory for small messages
    std::string extra_buf;
  };

  virtual void Call(Message& in, Message& out) = 0;
  virtual ~If();
  If() {}

 private:
  // No copying allowed
  void operator=(const If&);
  If(const If&);
};

}  // namespace rpc
}  // namespace pdlfs
