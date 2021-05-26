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

#include "posix_net.h"

#include "pdlfs-common/port.h"
#include "pdlfs-common/rpc.h"

#include <netinet/in.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <vector>

namespace pdlfs {
// Base RPC impl providing infrastructure for background progressing. To be
// extended by subclasses.
class PosixSocketServer {
 public:
  explicit PosixSocketServer(const RPCOptions& options);
  virtual ~PosixSocketServer();

  virtual std::string GetUri() = 0;
  virtual Status OpenAndBind(const std::string& uri) = 0;
  Status BGStart(Env* env, int num_threads);
  Status BGStop();

  int GetPort();  // Return server port.
  // Return base uri of the server. Unlike a full uri, a base uri is not coupled
  // with a protocol (tcp, udp).
  std::string GetBaseUri();
  std::string GetUsageInfo();
  Status status();

 protected:
  // No copying allowed
  void operator=(const PosixSocketServer&);
  PosixSocketServer(const PosixSocketServer& other);
  // BGLoopWrapper() calls BGCall(), which calls BGLoop()
  static void BGLoopWrapper(void* arg);
  void BGCall();
  virtual Status BGLoop(int myid) = 0;  // To be implemented by subclasses...
  struct BGUsageInfo {
    double user;    // user CPU time in seconds
    double system;  // system CPU time
    double wall;    // wall time
  };

  // For options_.info_log, options_.fs, and other socket-specific options
  const RPCOptions& options_;
  port::Mutex mutex_;
  // bg state below is protected by mutex_
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;
  int bg_n_;        // Total number of background threads to run
  int bg_threads_;  // Number of threads currently running
  int bg_id_;
  Status bg_status_;
  std::vector<struct BGUsageInfo> bg_usage_;
  PosixSocketAddr* actual_addr_;
  PosixSocketAddr* addr_;
  int fd_;
};

// Posix RPC impl wrapper.
class PosixRPC : public RPC {
 public:
  explicit PosixRPC(const RPCOptions& options);
  virtual ~PosixRPC() { delete srv_; }

  virtual rpc::If* OpenStubFor(const std::string& uri);
  virtual Status Start();  // Open server the start background progressing
  virtual Status Stop();

  virtual int GetPort();
  virtual std::string GetUri();
  virtual std::string GetUsageInfo();
  virtual Status status();

 private:
  // No copying allowed
  void operator=(const PosixRPC& other);
  PosixRPC(const PosixRPC&);
  PosixSocketServer* srv_;  // NULL for client only mode
  RPCOptions options_;
  int tcp_;  // O for UDP, non-0 for TCP
};

}  // namespace pdlfs
