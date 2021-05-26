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
#include "posix_rpc.h"

#include "posix_rpc_tcp.h"
#include "posix_rpc_udp.h"

#include "pdlfs-common/mutexlock.h"

#include <stdio.h>
#include <unistd.h>

namespace pdlfs {

PosixSocketServer::PosixSocketServer(const RPCOptions& options)
    : options_(options),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      bg_n_(0),
      bg_threads_(0),
      bg_id_(0),
      actual_addr_(new PosixSocketAddr),
      addr_(new PosixSocketAddr),
      fd_(-1) {}

void PosixSocketServer::BGLoopWrapper(void* arg) {
  PosixSocketServer* const r = reinterpret_cast<PosixSocketServer*>(arg);
  r->BGCall();
}

#if defined(PDLFS_OS_LINUX)
namespace {
inline double TimevalToDouble(const struct timeval* tv) {
  double t = tv->tv_sec;
  t += tv->tv_usec * 1e-6;
  return t;
}
}  // namespace
#endif

void PosixSocketServer::BGCall() {
  MutexLock ml(&mutex_);
  int myid = bg_id_++;
  BGUsageInfo tmp;
  memset(&tmp, 0, sizeof(BGUsageInfo));
  bg_usage_.push_back(tmp);
  ++bg_threads_;
  if (bg_threads_ == bg_n_) {
    bg_cv_.SignalAll();
  }
  mutex_.Unlock();
#if defined(PDLFS_OS_LINUX)
  struct rusage start_usage;
  memset(&start_usage, 0, sizeof(start_usage));
  struct rusage usage;
  memset(&usage, 0, sizeof(usage));
  int r = getrusage(RUSAGE_THREAD, &start_usage);
  double start = CurrentMicros() * 1e-6;
#endif
  Status s = BGLoop(myid);  // Transfer ctrl to subclass
#if defined(PDLFS_OS_LINUX)
  if (r == 0) {
    r = getrusage(RUSAGE_THREAD, &usage);
  }
  if (r != 0) {
    Log(options_.info_log, 0, "Cannot get thread rusage: %s", strerror(errno));
  } else {
    BGUsageInfo* info = &bg_usage_[myid];
    info->system = TimevalToDouble(&usage.ru_stime) -
                   TimevalToDouble(&start_usage.ru_stime);
    info->user = TimevalToDouble(&usage.ru_utime) -
                 TimevalToDouble(&start_usage.ru_utime);
    info->wall = CurrentMicros() * 1e-6 - start;
  }
#endif
  mutex_.Lock();
  if (!s.ok() && bg_status_.ok()) {
    bg_status_ = s;
  }
  assert(bg_threads_ > 0);
  --bg_threads_;
  if (!bg_threads_) {
    bg_cv_.SignalAll();
  }
}

int PosixSocketServer::GetPort() {
  MutexLock ml(&mutex_);
  if (fd_ != -1) return actual_addr_->GetPort();
  return addr_->GetPort();
}

std::string PosixSocketServer::GetBaseUri() {
  MutexLock ml(&mutex_);
  if (fd_ != -1) return actual_addr_->GetUri();
  return addr_->GetUri();
}

std::string PosixSocketServer::GetUsageInfo() {
  MutexLock ml(&mutex_);
  std::string result;
  char tmp[200];
  snprintf(tmp, sizeof(tmp), "%6s %12s %12s %12s\n", "Thread", "User(sec)",
           "System(sec)", "Wall(sec)");
  result += tmp;
  result += "---------------------------------------------\n";
  for (size_t i = 0; i < bg_usage_.size(); i++) {
    snprintf(tmp, sizeof(tmp), "%-6d %12.3f %12.3f %12.3f\n", int(i),
             bg_usage_[i].user, bg_usage_[i].system, bg_usage_[i].wall);
    result += tmp;
  }
  return result;
}

Status PosixSocketServer::status() {
  MutexLock ml(&mutex_);
  return bg_status_;
}

Status PosixSocketServer::BGStart(Env* const env, int num_threads) {
  MutexLock ml(&mutex_);
  bg_n_ += num_threads;
  for (int i = 0; i < num_threads; i++) {
    env->StartThread(BGLoopWrapper, this);
  }
  while (bg_threads_ < bg_n_) {
    bg_cv_.Wait();
  }
  // All background threads have started and they may have already
  // encountered errors and have exited so now is a good time we report
  // any errors back to the user.
  return bg_status_;
}

Status PosixSocketServer::BGStop() {
  MutexLock ml(&mutex_);
  shutting_down_.Release_Store(this);
  while (bg_threads_) {
    bg_cv_.Wait();
  }
  return bg_status_;  // Report background error status
}

PosixSocketServer::~PosixSocketServer() {
  BGStop();  // Stop background progressing
  delete actual_addr_;
  delete addr_;
  if (fd_ != -1) {
    close(fd_);
  }
}

namespace {
inline PosixSocketServer* CreateServer(const RPCOptions& options, int tcp) {
  if (tcp) return new PosixTCPServer(options, options.rpc_timeout);
  return new PosixUDPServer(options);
}
}  // namespace

PosixRPC::PosixRPC(const RPCOptions& options)
    : srv_(NULL), options_(options), tcp_(0) {
  tcp_ = Slice(options_.uri).starts_with("tcp://");
  if (options_.mode == rpc::kServerClient) {
    srv_ = CreateServer(options_, tcp_);
  }
}

Status PosixRPC::Start() {
  Status status;
  if (srv_) {
    status = srv_->OpenAndBind(options_.uri);
    if (status.ok()) {
      // BGStart() will wait until all threads are up
      status = srv_->BGStart(options_.env, options_.num_rpc_threads);
    }
  }
  return status;
}

Status PosixRPC::Stop() {
  if (srv_) return srv_->BGStop();
  return Status::OK();
}

int PosixRPC::GetPort() {
  if (srv_) return srv_->GetPort();
  return RPC::GetPort();
}

std::string PosixRPC::GetUri() {
  if (srv_) return srv_->GetUri();
  return RPC::GetUri();
}

std::string PosixRPC::GetUsageInfo() {
  if (srv_) return srv_->GetUsageInfo();
  return RPC::GetUsageInfo();
}

Status PosixRPC::status() {
  if (srv_) return srv_->status();
  return Status::OK();
}

rpc::If* PosixRPC::OpenStubFor(const std::string& uri) {
  if (!tcp_) {
    PosixUDPCli* const cli =
        new PosixUDPCli(options_.rpc_timeout, options_.udp_max_expected_msgsz);
    cli->Open(uri);
    return cli;
  } else {
    PosixTCPCli* const cli = new PosixTCPCli(options_.rpc_timeout);
    cli->SetTarget(uri);
    return cli;
  }
}

}  // namespace pdlfs
