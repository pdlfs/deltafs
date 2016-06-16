#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <string>
#include <vector>

#include "posix_env.h"

namespace pdlfs {

class PosixSock {
 public:
  PosixSock() : fd_(-1) {
    ifconf_.ifc_len = sizeof(ifr_);
    ifconf_.ifc_buf = reinterpret_cast<char*>(ifr_);
  }

  ~PosixSock() { close(fd_); }

  Status OpenSocket() {
    Status s;

    fd_ = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    if (fd_ == -1) {
      s = IOError("socket", errno);
    }

    return s;
  }

  Status LoadSocketConfig();
  Status GetHostIPAddresses(std::vector<std::string>* ips);

 private:
  int fd_;
  struct ifconf ifconf_;
  struct ifreq ifr_[64];

  // No copying allowed
  PosixSock(const PosixSock&);
  void operator=(const PosixSock&);
};

inline Status PosixSock::LoadSocketConfig() {
  Status s;

  int r = ioctl(fd_, SIOCGIFCONF, &ifconf_);
  if (r == -1) {
    s = IOError("ioctl", errno);
  }

  return s;
}

inline Status PosixSock::GetHostIPAddresses(std::vector<std::string>* ips) {
  Status s;
  char ip[INET_ADDRSTRLEN];

  for (size_t i = 0; i < ifconf_.ifc_len / sizeof(ifr_[0]); ++i) {
    sockaddr_in* s_in = reinterpret_cast<sockaddr_in*>(&ifr_[i].ifr_addr);
    const char* p = inet_ntop(AF_INET, &s_in->sin_addr, ip, INET_ADDRSTRLEN);

    if (p == NULL) {
      s = IOError("inet_ntop", errno);
      break;
    } else {
      ips->push_back(ip);
    }
  }

  return s;
}

}  // namespace pdlfs
