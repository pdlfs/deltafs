/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "posix_netdev.h"
#include "posix_env.h"

#include <arpa/inet.h>
#include <errno.h>

namespace pdlfs {

Status PosixIf::IfConf(std::vector<Ifr>* results) {
  Ifr ifr;

  results->clear();
  for (size_t i = 0; i < ifconf_.ifc_len / sizeof(ifr_[0]); ++i) {
    struct sockaddr_in* s_in =
        reinterpret_cast<sockaddr_in*>(&ifr_[i].ifr_addr);

    const char* ip_addr =
        inet_ntop(AF_INET, &s_in->sin_addr, ifr.ip, INET_ADDRSTRLEN);
    // Skip if addr is unresolvable
    if (ip_addr != NULL) {
      strncpy(ifr.name, ifr_[i].ifr_name, sizeof(ifr.name));
      ifr.mtu = ifr_[i].ifr_mtu;

      results->push_back(ifr);
    }
  }

  return Status::OK();
}

Status PosixIf::Open() {
  Status s;
  fd_ = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);  // Ignore ipv6
  if (fd_ == -1) {
    s = IOError("socket", errno);
    return s;
  }

  ifconf_.ifc_len = sizeof(ifr_);
  int r = ioctl(fd_, SIOCGIFCONF, &ifconf_);
  if (r == -1) {
    s = IOError("ioctl", errno);
    ifconf_.ifc_len = 0;
  }

  return s;
}

}  // namespace pdlfs
