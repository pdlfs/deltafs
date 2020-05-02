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

#include "posix_net.h"
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
    s = PosixError("socket", errno);
    return s;
  }

  ifconf_.ifc_len = sizeof(ifr_);
  int r = ioctl(fd_, SIOCGIFCONF, &ifconf_);
  if (r == -1) {
    s = PosixError("ioctl", errno);
    ifconf_.ifc_len = 0;
  }

  return s;
}

// Return all network addresses associated with us.
Status FetchHostIPAddrs(std::vector<std::string>* ips) {
  PosixIf sock;
  std::vector<Ifr> results;
  Status s = sock.Open();
  if (s.ok()) {
    sock.IfConf(&results);
    for (size_t i = 0; i < results.size(); i++) {
      ips->push_back(results[i].ip);
    }
  }
  return s;
}

// Return name of the machine.
Status FetchHostname(std::string* hostname) {
  char buf[PDLFS_HOST_NAME_MAX];
  if (gethostname(buf, sizeof(buf)) == -1) {
    return PosixError("gethostname", errno);
  } else {
    *hostname = buf;
    return Status::OK();
  }
}

}  // namespace pdlfs
