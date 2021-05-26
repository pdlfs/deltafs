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

#include <net/if.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <vector>

namespace pdlfs {

// A simple wrapper class over struct sockaddr_in.
class PosixSocketAddr {
 public:
  PosixSocketAddr() { Reset(); }
  void Reset();
  // The returned uri may not be used for clients to connect to the address.
  // While both the address and the port will be numeric, the address may be
  // "0.0.0.0" and the port may be "0".
  std::string GetUri() const;
  Status ResolvUri(const std::string& uri);
  int GetPort() const { return ntohs(addr_.sin_port); }
  const struct sockaddr_in* rep() const { return &addr_; }
  struct sockaddr_in* rep() {
    return &addr_;
  }

 private:
  void SetPort(const char* p) {
    int port = -1;
    if (p && p[0]) port = atoi(p);
    if (port < 0) {
      // Have the OS pick up a port for us
      port = 0;
    }
    addr_.sin_port = htons(port);
  }
  // Translate a human-readable host name into a binary internet address to
  // which we can bind or connect. Return OK on success, or a non-OK status on
  // errors.
  Status Resolv(const char* host, bool is_numeric);
  struct sockaddr_in addr_;

  // Copyable
};

// Posix server UDP socket.
class PosixServerUDPSocket : public ServerUDPSocket {
 public:
  PosixServerUDPSocket();
  virtual ~PosixServerUDPSocket();

  virtual Status OpenAndBind(const std::string& uri);
  virtual Status Recv(Slice* msg, char* scratch, size_t n);

 private:
  int fd_;
};

// Posix UDP.
class PosixUDPSocket : public UDPSocket {
 public:
  PosixUDPSocket();
  virtual ~PosixUDPSocket();

  virtual Status Connect(const std::string& uri);
  virtual Status Send(const Slice& msg);

 private:
  int fd_;
};

struct Ifr {
  // Interface name (such as eth0)
  char name[IF_NAMESIZE];
  // Network address (in the form of 11.22.33.44)
  char ip[INET_ADDRSTRLEN];
  // MTU number (such as 1500, 65520)
  int mtu;
};

// Low-level access to network devices.
class PosixIf {
 public:
  PosixIf() : fd_(-1) {
    ifconf_.ifc_buf = reinterpret_cast<char*>(ifr_);
    ifconf_.ifc_len = 0;
  }

  ~PosixIf();
  Status IfConf(std::vector<Ifr>* results);
  Status Open();

 private:
  struct ifreq ifr_[64];  // Record buffer; statically allocated
  struct ifconf ifconf_;
  // No copying allowed
  void operator=(const PosixIf& other);
  PosixIf(const PosixIf&);

  int fd_;
};

}  // namespace pdlfs
