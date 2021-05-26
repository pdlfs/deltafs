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

#include "posix_rpc.h"

#include <stddef.h>
#include <sys/socket.h>

namespace pdlfs {
// RPC srv impl using TCP.
class PosixTCPServer : public PosixSocketServer {
 public:
  PosixTCPServer(const RPCOptions& options, uint64_t timeout,
                 size_t buf_sz = 4000);
  virtual ~PosixTCPServer() {
    BGStop();
  }  // More resources to be released by parent

  // On OK, BGStart() from parent should then be called to commence background
  // server progressing.
  virtual Status OpenAndBind(const std::string& uri);
  virtual std::string GetUri();

 private:
  // State for each incoming procedure call.
  struct CallState {
    struct sockaddr_storage addr;  // Location of the caller
    socklen_t addrlen;
    int fd;
  };
  void HandleIncomingCall(CallState* call);
  virtual Status BGLoop(int myid);
  const uint64_t rpc_timeout_;  // In microseconds
  const size_t buf_sz_;         // Buffer size for reading peer data
};

// TCP client.
class PosixTCPCli : public rpc::If {
 public:
  explicit PosixTCPCli(uint64_t timeout, size_t buf_sz = 4000);
  virtual ~PosixTCPCli() {}

  // Each call creates a new socket, followed by a connection operation, a send,
  // and a receive.
  virtual Status Call(Message& in, Message& out) RPCNOEXCEPT;

  // If we fail to resolve the uri, we will record the error and return it at
  // the next Call() invocation.
  void SetTarget(const std::string& uri);

 private:
  // No copying allowed
  void operator=(const PosixTCPCli&);
  PosixTCPCli(const PosixTCPCli& other);
  Status OpenAndConnect(int* fd);
  const uint64_t rpc_timeout_;  // In microseconds
  const size_t buf_sz_;
  PosixSocketAddr addr_;
  Status status_;
};

}  // namespace pdlfs
