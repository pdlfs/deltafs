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
#pragma once

#include "pdlfs-common/env.h"
#include "pdlfs-common/status.h"

#include <errno.h>
#include <string>
#include <vector>

/*
 * In general we don't use c++ exceptions throughout our codebase. We expect RPC
 * implementations to not use exceptions to indicate errors. Instead, they
 * should simply return a non-OK status. Some RPC framework (such as Apache
 * Thrift) does use exceptions. We expect our wrappers to try-catch these
 * exceptions so that the rest of the codebase does not need to worry about
 * them.
 */
namespace pdlfs {
#if __cplusplus >= 201103
#define RPCNOEXCEPT noexcept
#else
#define RPCNOEXCEPT \
  throw()  // XXX: noexcept not available until c++ 11 or higher
#endif
namespace rpc {
// RPC mode selector. Each RPC instance acts either as a client or as a client
// and a server simultaneously.
enum Mode { kServerClient, kClientOnly };

// RPC backend selector. We have an implementation (kSocket) that directly uses
// TCP or UDP sockets to move RPC messages. We also have a Mercury-based
// (kMercuryRPC) and a Margo-based (kMargoRPC) implementation that wraps around
// the corresponding RPC framework to implement RPC routines. Both the Mercury
// and the Margo RPC frameworks can utilize various low-level network transports
// (e.g., RDMA, GNI, libfabric) to more efficiently move data over the network.
enum Engine { kSocketRPC, kMercuryRPC, kMargoRPC };

// All RPC messages are fired through rpc::If. This is the one and only
// interface for RPC communications.
class If;
}  // namespace rpc

struct RPCOptions {
  RPCOptions();
  rpc::Engine impl;  // Default: kSocketRPC
  rpc::Mode mode;    // Default: kServerClient
  std::string uri;
  uint64_t rpc_timeout;  // In microseconds, Default: 5 secs

  // Total number of threads used to drive RPC work and execute
  // RPC callback functions. RPC implementation may choose to dedicate
  // some of them to only drive RPC work and the rest to
  // execute RPC callback functions.
  int num_rpc_threads;  // Default: 1

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

// Each RPC* is a reference to an RPC instance. This instance either acts as a
// client or as a client and a server simultaneously. In general we assume an
// asynchronous processing model where users are expected to call Start() to
// spawn threads for progressing the sending and receiving of RPC messages in
// the background at the server side. Background progressing may be unnecessary
// at the client side. In this case an implementation may implement Start() as a
// non-op. To send RPC messages to a remote destination, use "OpenStubFor" to
// connect or bind to the remote end. The returned rpc::If handle can then be
// used by the caller to perform RPC communications. Thus the simplest form of
// an RPC instance is basically a set of network descriptors potentially
// accompanied by a pool of background progressing threads.
class RPC {
 public:
  RPC() {}
  virtual ~RPC();

  // Return a new RPC instance. Each RPC instance should ensure thread-safety
  // such that multiple caller threads may perform operations concurrently
  // without requiring explicit synchronization at the caller side.
  static RPC* Open(const RPCOptions& rpcopts);

  // Connect or bind to a remote peer and return a stub for RPC communications.
  // The returned result should be deleted when it is no longer needed.
  virtual rpc::If* OpenStubFor(const std::string& addr) = 0;

  // Start a fixed amount of threads to progressing RPC in the background. If
  // background progressing is unnecessary, this may be a non-op.
  virtual Status Start() = 0;

  // Stop background progressing and release threads.
  virtual Status Stop() = 0;

  // Return errors if there is any.
  virtual Status status() const;

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

// To simplify things, we use a fixed interface to cover all RPC communication
// use cases. This fixed RPC interface has only one method named Call that
// accepts exactly one input parameter and one output parameter. Both the input
// and the output parameter are of type Message, which carries an operation
// type, an error code, and an opaque binary string as the main payload of the
// message. Thus callers handle message encoding and decoding. RPC
// implementations only deal with the transmission of data over the network.
namespace rpc {
class If {
 public:
  // Each message holds a chunk of data opaque to the RPC implementation.
  // This makes it easier for us to program against different RPC frameworks
  // with potentially different type systems.
  struct Message {
    int op;          // Operation type
    int err;         // Error code
    Slice contents;  // Message body, reference to the
    Message() : op(0), err(0) {}

    // To reduce memory copying, a caller may put data in an external
    // buffer instead of the ones defined below
    char buf[500];  // Avoiding allocating dynamic memory for small messages
    std::string extra_buf;
  };

  // Return OK on success, or a non-OK status on errors.
  // Must not throw any exceptions.
  virtual Status Call(Message& in, Message& out) RPCNOEXCEPT = 0;
  virtual ~If();
  If() {}

 private:
  // No copying allowed
  void operator=(const If&);
  If(const If&);
};

}  // namespace rpc
}  // namespace pdlfs
