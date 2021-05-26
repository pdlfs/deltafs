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
#include "posix_rpc_tcp.h"

#include "pdlfs-common/mutexlock.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <string.h>
#include <unistd.h>

namespace pdlfs {
PosixTCPServer::PosixTCPServer(const RPCOptions& opts, uint64_t t, size_t s)
    : PosixSocketServer(opts), rpc_timeout_(t), buf_sz_(s) {}

Status PosixTCPServer::OpenAndBind(const std::string& uri) {
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
  fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (fd_ == -1) {
    status = Status::IOError(strerror(errno));
  } else {
    int rv = bind(fd_, reinterpret_cast<struct sockaddr*>(addr_->rep()),
                  sizeof(struct sockaddr_in));
    if (rv != -1) {
      rv = listen(fd_, 256);
    }
    if (rv == -1) {
      status = Status::IOError(strerror(errno));
      close(fd_);
      fd_ = -1;
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

namespace {
// Set or unset the O_NONBLOCK flag on a given file.
inline void SET_O_NONBLOCK(int fd, bool non_blocking) {
  int flags = fcntl(fd, F_GETFL, 0);
  flags = non_blocking ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
  fcntl(fd, F_SETFL, flags);
}
}  // namespace

Status PosixTCPServer::BGLoop(int myid) {
  SET_O_NONBLOCK(fd_, true);
  struct pollfd po;
  po.events = POLLIN;
  po.fd = fd_;
  CallState call;

  int err = 0;
  while (!err && !shutting_down_.Acquire_Load()) {
    call.addrlen = sizeof(call.addr);
    int rv = accept(fd_, reinterpret_cast<struct sockaddr*>(&call.addr),
                    &call.addrlen);
    if (rv != -1) {
      call.fd = rv;
      HandleIncomingCall(&call);
      close(rv);
      continue;
    } else if (errno == EWOULDBLOCK) {
      rv = poll(&po, 1, 200);
    }

    // Either poll() or accept() may have returned error
    if (rv == -1) {
      err = errno;
    }
  }

  Status status;
  if (err) {
    status = Status::IOError(strerror(err));
  }
  return status;
}

void PosixTCPServer::HandleIncomingCall(CallState* const call) {
  int err = 0;
  rpc::If::Message in, out;
  const uint64_t start = CurrentMicros();
  struct pollfd po;
  memset(&po, 0, sizeof(struct pollfd));
  po.events = POLLIN;
  po.fd = call->fd;
  char* const buf = new char[buf_sz_];
  while (true) {
    ssize_t rv = recv(call->fd, buf, buf_sz_, MSG_DONTWAIT);
    if (rv > 0) {
      in.extra_buf.append(buf, rv);
      continue;
    } else if (rv == 0) {  // End of message
      in.contents = in.extra_buf;
      break;
    } else if (errno == EWOULDBLOCK) {
      // We wait for 0.2 second and therefore timeouts are only checked
      // roughly every that amount of time.
      rv = poll(&po, 1, 200);
    }

    // Either recv or poll may have returned errors
    if (rv == -1) {
      err = errno;
      break;
    } else if (rv == 1) {
      continue;
    } else if (CurrentMicros() - start >= rpc_timeout_) {
      err = -1;
      break;
    }
  }

  delete[] buf;
  if (err) {
    //
    return;
  }

  options_.fs->Call(in, out);
  Slice remaining_out = out.contents;
  SET_O_NONBLOCK(call->fd, false);  // Force blocking semantics
  while (!remaining_out.empty()) {
    ssize_t nbytes =
        send(call->fd, remaining_out.data(), remaining_out.size(), 0);
    if (nbytes > 0) {
      remaining_out.remove_prefix(nbytes);
    } else {
      //
      return;
    }
  }

  shutdown(call->fd, SHUT_WR);
}

std::string PosixTCPServer::GetUri() {
  return std::string("tcp://") + GetBaseUri();
}

PosixTCPCli::PosixTCPCli(uint64_t timeout, size_t buf_sz)
    : rpc_timeout_(timeout), buf_sz_(buf_sz) {}

void PosixTCPCli::SetTarget(const std::string& uri) {
  status_ = addr_.ResolvUri(uri);
}

Status PosixTCPCli::OpenAndConnect(int* result) {
  int fd = (*result = socket(AF_INET, SOCK_STREAM, 0));
  if (fd == -1) {
    return Status::IOError(strerror(errno));
  }
  Status status;
  int rv = connect(fd, reinterpret_cast<struct sockaddr*>(addr_.rep()),
                   sizeof(struct sockaddr_in));
  if (rv == -1) {
    status = Status::IOError(strerror(errno));
    close(fd);
  }
  return status;
}

Status PosixTCPCli::Call(Message& in, Message& out) RPCNOEXCEPT {
  if (!status_.ok()) {
    return status_;
  }
  int fd;
  Status status = OpenAndConnect(&fd);
  if (!status.ok()) {
    return status;
  }
  Slice remaining_in = in.contents;
  SET_O_NONBLOCK(fd, false);  // Force blocking semantics
  while (!remaining_in.empty()) {
    ssize_t nbytes = send(fd, remaining_in.data(), remaining_in.size(), 0);
    if (nbytes > 0) {
      remaining_in.remove_prefix(nbytes);
    } else {
      status = Status::IOError(strerror(errno));
      close(fd);
      return status;
    }
  }
  shutdown(fd, SHUT_WR);
  const uint64_t start = CurrentMicros();
  struct pollfd po;
  memset(&po, 0, sizeof(struct pollfd));
  po.events = POLLIN;
  po.fd = fd;
  char* const buf = new char[buf_sz_];
  while (true) {
    ssize_t rv = recv(fd, buf, buf_sz_, MSG_DONTWAIT);
    if (rv > 0) {
      out.extra_buf.append(buf, rv);
      continue;
    } else if (rv == 0) {  // End of message
      out.contents = out.extra_buf;
      break;
    } else if (errno == EWOULDBLOCK) {
      // We wait for 0.2 second and therefore timeouts are only checked
      // roughly every that amount of time.
      rv = poll(&po, 1, 200);
    }

    // Either recv or poll may have returned errors
    if (rv == -1) {
      status = Status::IOError(strerror(errno));
      break;
    } else if (rv == 1) {
      continue;
    } else if (CurrentMicros() - start >= rpc_timeout_) {
      status = Status::Disconnected("timeout");
      break;
    }
  }

  delete[] buf;
  close(fd);
  return status;
}

}  // namespace pdlfs
