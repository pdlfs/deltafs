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

#include "pdlfs-common/rpc.h"
#include "pdlfs-common/logging.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/port.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#if defined(PDLFS_MARGO_RPC)
#include "margo_rpc.h"
#endif

#if defined(PDLFS_MERCURY_RPC)
#include "mercury_rpc.h"
#endif

namespace pdlfs {

RPCOptions::RPCOptions()
    : impl(rpc::kMercuryRPC),
      mode(rpc::kServerClient),
      rpc_timeout(5000000),
      num_rpc_threads(1),
      extra_workers(NULL),
      addr_cache_size(128),
      env(NULL),
      fs(NULL) {}

Status RPC::status() const {  ///
  return Status::OK();
}

RPC::~RPC() {}

namespace {

class SocketRPC {
 public:
  SocketRPC(bool listening, const RPCOptions& rpcopts)
      : listening_(listening), if_(rpcopts.fs), env_(rpcopts.env), fd_(-1) {}

  class ThreadedLooper;
  class Client;
  class Addr;

  // Bind to a requested address. This address may be a wildcard address.
  // Return OK on success, or a non-OK status on errors.
  Status Bind(const Addr& addr);

  ~SocketRPC() {
    if (fd_ != -1) {
      close(fd_);
    }
  }

 private:
  struct CallState {
    struct sockaddr addr;  // Location of the caller
    socklen_t addrlen;
    size_t msgsz;  // Payload size
    char msg[1];
  };
  void HandleIncomingCall(CallState* call);

  // No copying allowed
  void operator=(const SocketRPC& rpc);
  SocketRPC(const SocketRPC&);

  bool const listening_;
  rpc::If* const if_;
  Env* const env_;
  port::Mutex mutex_;
  Status status_;
  int fd_;
};

void SocketRPC::HandleIncomingCall(CallState* const call) {
  rpc::If::Message in, out;
  in.contents = Slice(call->msg, call->msgsz);
  if_->Call(in, out);
  int nbytes = sendto(fd_, out.contents.data(), out.contents.size(), 0,
                      &call->addr, call->addrlen);
  if (nbytes != out.contents.size()) {
    //
  }
}

class SocketRPC::Addr {
 public:
  explicit Addr(int port = 0) {
    memset(&addr, 0, sizeof(struct sockaddr_in));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    if (port < 0) {  // Have the OS select a port for us
      port = 0;
    }
    addr.sin_port = htons(port);
  }

  // Translate an address string into a socket address to which we can
  // bind or connect. Return OK on success, or a non-OK status on errors.
  Status Resolve(const std::string& host, bool is_host_numeric);
  const struct sockaddr_in* rep() const { return &addr; }
  struct sockaddr_in* rep() {
    return &addr;
  }

 private:
  struct sockaddr_in addr;

  // Copyable
};

Status SocketRPC::Addr::Resolve(const std::string& host, bool is_host_numeric) {
  if (host.empty() || strncmp("0.0.0.0", host.c_str(), host.size()) == 0) {
    addr.sin_addr.s_addr = INADDR_ANY;
  } else if (is_host_numeric) {
    in_addr_t in_addr = inet_addr(host.c_str());
    if (in_addr == INADDR_NONE) {
      return Status::InvalidArgument("ip addr", host.c_str());
    } else {
      addr.sin_addr.s_addr = in_addr;
    }
  } else {
    struct addrinfo *ai, hints;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = PF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = 0;
    int ret = getaddrinfo(host.c_str(), NULL, &hints, &ai);
    if (ret != 0) {
      return Status::IOError("getaddrinfo", gai_strerror(ret));
    } else {
      const struct sockaddr_in* const in =
          reinterpret_cast<struct sockaddr_in*>(ai->ai_addr);
      addr.sin_addr = in->sin_addr;
    }
  }

  return Status::OK();
}

class SocketRPC::ThreadedLooper {
 public:
  ThreadedLooper(SocketRPC* rpc, const RPCOptions& options)
      : num_threads_(options.num_rpc_threads),
        max_msgsz_(1432),
        rpc_(rpc),
        shutting_down_(NULL),
        bg_cv_(&rpc_->mutex_),
        bg_threads_(0),
        bg_id_(0) {}

  void Start() {
    MutexLock ml(&rpc_->mutex_);
    while (bg_threads_ < num_threads_) {
      rpc_->env_->StartThread(BGLoopWrapper, this);
      ++bg_threads_;
    }
  }

  void Stop() {
    MutexLock ml(&rpc_->mutex_);
    shutting_down_.Release_Store(this);
    while (bg_threads_) {
      bg_cv_.Wait();
    }
  }

  ~ThreadedLooper() {
    Stop();  // Release background threads
  }

 private:
  static void BGLoopWrapper(void* arg);
  void BGLoop();

  // Constant after construction
  int const num_threads_;  // Total num of threads to create
  size_t const max_msgsz_;
  SocketRPC* const rpc_;

  // State below is protected by rpc_->mutex_
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;
  int bg_threads_;
  int bg_id_;
};

void SocketRPC::ThreadedLooper::BGLoopWrapper(void* arg) {
  ThreadedLooper* const lo = reinterpret_cast<ThreadedLooper*>(arg);
  lo->BGLoop();
}

void SocketRPC::ThreadedLooper::BGLoop() {
  SocketRPC* const r = rpc_;
  struct pollfd po;
  po.events = POLLIN;
  r->mutex_.Lock();
  int const myid = bg_id_++;
  po.fd = r->fd_;
  r->mutex_.Unlock();
  CallState* const call = static_cast<CallState*>(
      malloc(sizeof(struct CallState) - 1 + max_msgsz_));
  int err = 0;

  while (true) {
    if (shutting_down_.Acquire_Load() || err) {
      MutexLock ml(&r->mutex_);
      if (err && r->status_.ok()) {
        r->status_ = Status::IOError(strerror(err));
      }
      assert(bg_threads_ > 0);
      --bg_threads_;
      if (!bg_threads_) {
        bg_cv_.SignalAll();
      }
      break;
    }

    int nret = recvfrom(po.fd, call->msg, max_msgsz_, MSG_DONTWAIT, &call->addr,
                        &call->addrlen);
    if (nret > 0) {
      call->msgsz = nret;
      r->HandleIncomingCall(call);
      continue;
    } else if (nret == 0) {  // Is this really gonna happen?
      continue;
    } else if (errno == EWOULDBLOCK) {
      nret = poll(&po, 1, 200);
    }

    if (nret == -1) {
      err = errno;
    }
  }

  free(call);
}

class SocketRPC::Client : public rpc::If {
 public:
  explicit Client(const RPCOptions& rpcopts)
      : rpc_timeout_(rpcopts.rpc_timeout),
        max_msgsz_(1432),
        env_(rpcopts.env),
        fd_(-1) {}

  virtual ~Client() {
    if (fd_ != -1) {
      close(fd_);
    }
  }

  // REQUIRES: Connect() has been called successfully.
  virtual Status Call(Message& in, Message& out) RPCNOEXCEPT;

  // Connect to the specified destination. This destination must not be a
  // wildcard address. Return OK on success, or a non-OK status on errors.
  Status Connect(const Addr& addr);

 private:
  // No copying allowed
  void operator=(const Client&);
  Client(const Client&);

  const uint64_t rpc_timeout_;  // In microseconds
  const size_t max_msgsz_;
  Env* const env_;

  int fd_;
};

// We do a synchronous send, followed by one or more non-blocking receives
// so that we can easily check timeouts without waiting for the data
// indefinitely. We use a timed poll to check data availability.
Status SocketRPC::Client::Call(Message& in, Message& out) RPCNOEXCEPT {
  Status status;
  int nret = send(fd_, in.contents.data(), in.contents.size(), 0);
  if (nret != in.contents.size()) {
    status = Status::IOError("send", strerror(errno));
  } else {
    const uint64_t start = env_->NowMicros();
    std::string& buf = out.extra_buf;
    buf.reserve(max_msgsz_);
    buf.resize(1);
    struct pollfd po;
    memset(&po, 0, sizeof(struct pollfd));
    po.events = POLLIN;
    po.fd = fd_;
    while (true) {
      nret = recv(fd_, &buf[0], max_msgsz_, MSG_DONTWAIT);
      if (nret > 0) {
        out.contents = Slice(buf.data(), nret);
        break;
      } else if (nret == 0) {  // Is this really possible though?
        out.contents = Slice();
        break;
      } else if (errno == EWOULDBLOCK) {
        // We wait for 0.2 second and therefore timeouts are only checked
        // roughly every that amount of time.
        nret = poll(&po, 1, 200);
      }

      // Either recv or poll may have returned errors
      if (nret == -1) {
        status = Status::IOError("recv or poll", strerror(errno));
        break;
      } else if (env_->NowMicros() - start >= rpc_timeout_) {
        status = Status::Disconnected("timeout");
        break;
      }
    }
  }

  return status;
}

Status SocketRPC::Client::Connect(const Addr& addr) {
  if ((fd_ = socket(PF_INET, SOCK_DGRAM, 0)) == -1)
    return Status::IOError(strerror(errno));
  int ret = connect(fd_, reinterpret_cast<const struct sockaddr*>(addr.rep()),
                    sizeof(struct sockaddr_in));
  if (ret == -1) {
    return Status::IOError(strerror(errno));
  }
  return Status::OK();
}

Status SocketRPC::Bind(const Addr& addr) {
  if ((fd_ = socket(PF_INET, SOCK_DGRAM, 0)) == -1)
    return Status::IOError(strerror(errno));
  int ret = bind(fd_, reinterpret_cast<const struct sockaddr*>(addr.rep()),
                 sizeof(struct sockaddr_in));
  if (ret == -1) {
    return Status::IOError(strerror(errno));
  }
  return Status::OK();
}

class RPCImpl : public RPC {
 public:
};

}  // namespace

RPCServer::~RPCServer() {
  std::vector<RPCInfo>::iterator it;
  for (it = rpcs_.begin(); it != rpcs_.end(); ++it) {
    delete it->rpc;
    delete it->pool;
  }
}

void RPCServer::AddChannel(const std::string& listening_uri, int workers) {
  RPCInfo info;
  RPCOptions options;
  options.env = env_;
  info.pool = ThreadPool::NewFixed(workers);
  options.extra_workers = info.pool;
  options.fs = fs_;
  options.uri = listening_uri;
  info.rpc = RPC::Open(options);
  rpcs_.push_back(info);
}

Status RPCServer::status() const {
  Status s;
  std::vector<RPCInfo>::const_iterator it;
  for (it = rpcs_.begin(); it != rpcs_.end(); ++it) {
    assert(it->rpc != NULL);
    s = it->rpc->status();
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

Status RPCServer::Start() {
  Status s;
  std::vector<RPCInfo>::iterator it;
  for (it = rpcs_.begin(); it != rpcs_.end(); ++it) {
    assert(it->rpc != NULL);
    s = it->rpc->Start();
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

Status RPCServer::Stop() {
  Status s;
  std::vector<RPCInfo>::iterator it;
  for (it = rpcs_.begin(); it != rpcs_.end(); ++it) {
    assert(it->rpc != NULL);
    s = it->rpc->Stop();
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

namespace rpc {

If::~If() {}

namespace {
#if defined(PDLFS_MARGO_RPC)
class MargoRPCImpl : public RPC {
  MargoRPC* rpc_;

 public:
  virtual Status Start() { return rpc_->Start(); }
  virtual Status Stop() { return rpc_->Stop(); }

  virtual If* OpenClientFor(const std::string& addr) {
    return new MargoRPC::Client(rpc_, addr);
  }

  MargoRPCImpl(const RPCOptions& options) {
    rpc_ = new MargoRPC(options.mode == kServerClient, options);
    rpc_->Ref();
  }

  virtual ~MargoRPCImpl() { rpc_->Unref(); }
};
#endif
}  // namespace

namespace {
#if defined(PDLFS_MERCURY_RPC)
class MercuryRPCImpl : public RPC {
  MercuryRPC::LocalLooper* looper_;
  MercuryRPC* rpc_;

 public:
  virtual Status status() const { return rpc_->status(); }
  virtual Status Start() { return looper_->Start(); }
  virtual Status Stop() { return looper_->Stop(); }

  virtual If* OpenStubFor(const std::string& addr) {
    return new MercuryRPC::Client(rpc_, addr);
  }

  MercuryRPCImpl(const RPCOptions& options) {
    rpc_ = new MercuryRPC(options.mode == kServerClient, options);
    looper_ = new MercuryRPC::LocalLooper(rpc_, options);
    rpc_->Ref();
  }

  virtual ~MercuryRPCImpl() {
    rpc_->Unref();
    delete looper_;
  }
};
#endif
}  // namespace

}  // namespace rpc

RPC* RPC::Open(const RPCOptions& raw_options) {
  assert(raw_options.uri.size() != 0);
  assert(raw_options.mode != rpc::kServerClient || raw_options.fs != NULL);
  RPCOptions options(raw_options);
  if (options.env == NULL) {
    options.env = Env::Default();
  }
#if VERBOSE >= 1
  Verbose(__LOG_ARGS__, 1, "rpc.uri -> %s", options.uri.c_str());
  Verbose(__LOG_ARGS__, 1, "rpc.timeout -> %llu (microseconds)",
          (unsigned long long)options.rpc_timeout);
  Verbose(__LOG_ARGS__, 1, "rpc.num_io_threads -> %d", options.num_rpc_threads);
  Verbose(__LOG_ARGS__, 1, "rpc.extra_workers -> [%s]",
          options.extra_workers != NULL
              ? options.extra_workers->ToDebugString().c_str()
              : "NULL");
#endif
  RPC* rpc = NULL;
#if defined(PDLFS_MARGO_RPC)
  if (options.impl == kMargoRPC) {
    rpc = new rpc::MargoRPCImpl(options);
  }
#endif
#if defined(PDLFS_MERCURY_RPC)
  if (options.impl == rpc::kMercuryRPC) {
    rpc = new rpc::MercuryRPCImpl(options);
  }
#endif
  if (rpc == NULL) {
#ifndef NDEBUG
    char msg[] = "No rpc implementation is available\n";
    fwrite(msg, 1, sizeof(msg), stderr);
    abort();
#else
    Error(__LOG_ARGS__, "No rpc implementation is available");
    exit(EXIT_FAILURE);
#endif
  } else {
    return rpc;
  }
}

}  // namespace pdlfs
