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
#include "posix_rpc_udp.h"

#include "pdlfs-common/env.h"
#include "pdlfs-common/mutexlock.h"

#include <errno.h>
#include <netdb.h>
#include <poll.h>
#include <string.h>
#include <unistd.h>

namespace pdlfs {

PosixUDPServer::PosixUDPServer(const RPCOptions& options)
    : PosixSocketServer(options),
      max_msgsz_(options.udp_max_unexpected_msgsz),
      bg_count_(0) {}

PosixUDPServer::~PosixUDPServer() {
  BGStop();  // Stop receiving new messages
  MutexLock ml(&mutex_);
  while (bg_count_ != 0) {  // Wait until all bg work items have been processed
    bg_cv_.Wait();
  }
  // More resources will be released by parent
}

Status PosixUDPServer::OpenAndBind(const std::string& uri) {
  MutexLock ml(&mutex_);
  if (fd_ != -1) {
    return Status::AssertionFailed("Socket already opened");
  }
  Status status = addr_->ResolvUri(uri);
  if (!status.ok()) {
    return status;
  }

  // Try opening the server. If we fail we will clean up so that we can try
  // again later.
  fd_ = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd_ == -1) {
    status = Status::IOError("Cannot create UDP socket", strerror(errno));
  } else {
    int rv = bind(fd_, reinterpret_cast<struct sockaddr*>(addr_->rep()),
                  sizeof(struct sockaddr_in));
    if (rv == -1) {
      status = Status::IOError("UDP bind", strerror(errno));
      close(fd_);
      fd_ = -1;
    }
  }

  if (options_.udp_srv_rcvbuf != -1) {
    int rv = setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &options_.udp_srv_rcvbuf,
                        sizeof(options_.udp_srv_rcvbuf));
    if (rv != 0) {
      Log(options_.info_log, 0, "Cannot set SO_RCVBUF=%d: %s",
          options_.udp_srv_rcvbuf, strerror(errno));
    }
  }

  if (options_.udp_srv_sndbuf != -1) {
    int rv = setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &options_.udp_srv_sndbuf,
                        sizeof(options_.udp_srv_sndbuf));
    if (rv != 0) {
      Log(options_.info_log, 0, "Cannot set SO_SNDBUF=%d: %s",
          options_.udp_srv_sndbuf, strerror(errno));
    }
  }

  if (status.ok()) {
    // Fetch the port that we have just bound to in case we have decided to have
    // the OS choose the port
    socklen_t tmp = sizeof(struct sockaddr_in);
    getsockname(fd_, reinterpret_cast<struct sockaddr*>(actual_addr_->rep()),
                &tmp);
  }

  return status;
}

inline PosixUDPServer::CallState* PosixUDPServer::CreateCallState() {
  CallState* const call = static_cast<CallState*>(
      malloc(sizeof(struct CallState) - 1 + max_msgsz_));
  call->parent_srv = this;
  return call;
}

Status PosixUDPServer::BGLoop(int myid) {
  CallState* call = CreateCallState();
  struct pollfd po;
  po.events = POLLIN;
  po.fd = fd_;

  int err = 0;
  while (!err && !shutting_down_.Acquire_Load()) {
    call->addrlen = sizeof(call->addrstor);
    // Try performing a quick non-blocking receive from peers before sinking
    // into poll.
    ssize_t rv = recvfrom(fd_, call->msg, max_msgsz_, MSG_DONTWAIT,
                          call->addrbuf(), &call->addrlen);
    if (rv > 0) {
      call->msgsz = rv;
      HandleIncomingCall(&call);
      continue;
    } else if (rv == 0) {  // Empty message
      continue;
    } else if (errno == EWOULDBLOCK) {
      rv = poll(&po, 1, 200);
    }

    // Either poll() or recvfrom() may have returned error
    if (rv == -1) {
      err = errno;
    }
  }

  free(call);

  Status status;
  if (err) {
    status = Status::IOError("UDP recvfrom/poll", strerror(err));
  }
  return status;
}

void PosixUDPServer::HandleIncomingCall(CallState** call) {
  if (options_.extra_workers) {
    mutex_.Lock();
    ++bg_count_;
    // XXX: senders/callers are implicitly rate-limited by not sending them
    // replies. Should we more explicitly rate-limit them? For example, when
    // bg_work_ is larger than a certain threshold, incoming calls are instantly
    // rejected with a special reply. This special reply is understood by
    // PosixUDPCli, which in turn returns a special Status to the caller.
    options_.extra_workers->Schedule(ProcessCallWrapper, *call);
    mutex_.Unlock();
    *call = CreateCallState();
  } else {
    ProcessCall(*call);
  }
}

void PosixUDPServer::ProcessCallWrapper(void* arg) {
  CallState* const call = reinterpret_cast<CallState*>(arg);
  PosixUDPServer* const srv = call->parent_srv;
  srv->ProcessCall(call);
  free(call);
  MutexLock ml(&srv->mutex_);
  assert(srv->bg_count_ > 0);
  --srv->bg_count_;
  if (!srv->bg_count_) {
    srv->bg_cv_.SignalAll();
  }
}

void PosixUDPServer::ProcessCall(CallState* const call) {
  rpc::If::Message in, out;
  in.contents = Slice(call->msg, call->msgsz);
  Status s = options_.fs->Call(in, out);
  if (!s.ok()) {
    Log(options_.info_log, 0, "Fail to handle incoming call: %s",
        s.ToString().c_str());
    return;
  }
  ssize_t nbytes = sendto(fd_, out.contents.data(), out.contents.size(), 0,
                          call->addrbuf(), call->addrlen);
  if (nbytes != out.contents.size()) {
#if VERBOSE >= 1
    const int errno_copy = errno;  // Store a copy before calling getnameinfo()
    char host[NI_MAXHOST];
    char port[NI_MAXSERV];
    getnameinfo(call->addrbuf(), call->addrlen, host, sizeof(host), port,
                sizeof(port), NI_NUMERICHOST | NI_NUMERICSERV);
    Log(options_.info_log, 1, "Fail to send rpc reply to client[%s:%s]: %s",
        host, port, strerror(errno_copy));
#else
    Log(options_.info_log, 0, "Error sending data to client: %s",
        strerror(errno));
#endif
  }
}

std::string PosixUDPServer::GetUri() {
  return std::string("udp://") + GetBaseUri();
}

PosixUDPCli::PosixUDPCli(uint64_t timeout, size_t max_msgsz)
    : rpc_timeout_(timeout), max_msgsz_(max_msgsz), fd_(-1) {}

void PosixUDPCli::Open(const std::string& uri) {
  PosixSocketAddr addr;
  status_ = addr.ResolvUri(uri);
  if (!status_.ok()) {
    return;
  }
  fd_ = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd_ == -1) {
    status_ = Status::IOError("Cannot create UDP socket", strerror(errno));
    return;
  }
  int rv = connect(fd_, reinterpret_cast<struct sockaddr*>(addr.rep()),
                   sizeof(struct sockaddr_in));
  if (rv == -1) {
    status_ = Status::IOError("UDP connect", strerror(errno));
    close(fd_);
    fd_ = -1;
  }
}

// We do a synchronous send, followed by one or more non-blocking receives
// so that we can easily check timeouts without waiting for the data
// indefinitely. We use a timed poll to check data availability.
Status PosixUDPCli::Call(Message& in, Message& out) RPCNOEXCEPT {
  if (!status_.ok()) {
    return status_;
  }
  Status status;
  ssize_t rv = send(fd_, in.contents.data(), in.contents.size(), 0);
  if (rv != in.contents.size()) {
    status = Status::IOError("UDP send", strerror(errno));
    return status;
  }
  const uint64_t start = CurrentMicros();
  std::string& buf = out.extra_buf;
  buf.reserve(max_msgsz_);
  buf.resize(1);
  struct pollfd po;
  memset(&po, 0, sizeof(struct pollfd));
  po.events = POLLIN;
  po.fd = fd_;
  while (true) {
    rv = recv(fd_, &buf[0], max_msgsz_, MSG_DONTWAIT);
    if (rv > 0) {
      out.contents = Slice(&buf[0], rv);
      break;
    } else if (rv == 0) {  // Empty reply
      out.contents = Slice();
      break;
    } else if (errno == EWOULDBLOCK) {
      // We wait for 0.2 second and therefore timeouts are only checked
      // roughly every that amount of time.
      rv = poll(&po, 1, 200);
    }

    // Either poll() or recv() may have returned an error
    if (rv == -1) {
      status = Status::IOError("UDP recv/poll", strerror(errno));
      break;
    } else if (rv == 1) {
      continue;
    } else if (CurrentMicros() - start >= rpc_timeout_) {
      status = Status::Disconnected("timeout");
      break;
    }
  }

  return status;
}

PosixUDPCli::~PosixUDPCli() {
  if (fd_ != -1) {
    close(fd_);
  }
}

}  // namespace pdlfs
