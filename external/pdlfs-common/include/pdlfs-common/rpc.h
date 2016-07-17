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

enum RPCMode {
  kServerClient,  // Will also need to listen client requests
  kClientOnly
};

struct RPCOptions {
  RPCOptions();
  RPCMode mode;  // Default: kServerClient
  std::string uri;
  int num_io_threads;         // Default: 1
  ThreadPool* extra_workers;  // Default: NULL
  rpc::If* fs;
  Env* env;  // Default: NULL, which indicates Env::Default() should be used
};

class RPC {
 public:
  RPC() {}
  virtual ~RPC();

  // RPC implementation should ensure the results of the following calls
  // are thread-safe so no external synchronization is needed.
  static RPC* Open(const RPCOptions&);
  virtual rpc::If* OpenClientStub(const std::string& uri) = 0;

  // The following calls shall return immediately.
  // One or more background looping threads maybe be created or destroyed
  // after these calls.
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
