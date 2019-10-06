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

#include "deltafs/deltafs_api.h"
#include "deltafs/deltafs_config.h"
#include "pdlfs-common/pdlfs_config.h"

#if defined(PDLFS_GFLAGS)
#include <gflags/gflags.h>
#endif

#if defined(PDLFS_GLOG)
#include <glog/logging.h>
#endif

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

namespace {
void DOIT_getaddrinfo(const char* node, const char* serv) {
  struct addrinfo *ai, hints;
  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = PF_INET;
  hints.ai_socktype = SOCK_DGRAM;
  // We always request AI_CANONNAME and AI_PASSIVE. We understand that
  // AI_CANONNAME is ignored when node is given as a numeric string (such as
  // "8.8.8.8") instead of a human-readable hostname, and that AI_PASSIVE is
  // ignored when node is not NULL. We also understand that when node is NULL
  // but AI_PASSIVE is OFF, the returned addresses will be bound to
  // INADDR_LOOPBACK instead of INADDR_ANY. Either node or serv, but not both,
  // may be NULL. To achieve the same effect while keeping getaddrinfo happy,
  // give node as "0.0.0.0" and serv as "0" simultaneously.
  hints.ai_flags = AI_CANONNAME | AI_PASSIVE;
  int ret = getaddrinfo(node, serv, &hints, &ai);
  if (ret != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(ret));
  } else {
    if (ai->ai_canonname)  ///
      fprintf(stderr, "%s\n", ai->ai_canonname);
    char name[INET_ADDRSTRLEN];
    struct sockaddr_in* addr;
    struct addrinfo* rp;
    int port;
    for (rp = ai; rp; rp = rp->ai_next) {
      addr = reinterpret_cast<struct sockaddr_in*>(rp->ai_addr);
      inet_ntop(AF_INET, &addr->sin_addr, name, sizeof(name));
      port = ntohs(addr->sin_port);
      fprintf(stderr, "%s:%d\n", name, port);
    }
    freeaddrinfo(ai);
  }
}
}  // namespace

int main(int argc, char* argv[]) {
#if defined(PDLFS_GLOG)
  FLAGS_logtostderr = true;
#endif
#if defined(PDLFS_GFLAGS)
  std::string usage("Sample usage: ");
  usage += argv[0];
  google::SetUsageMessage(usage);
  google::SetVersionString(PDLFS_COMMON_VERSION);
  google::ParseCommandLineFlags(&argc, &argv, true);
#endif
#if defined(PDLFS_GLOG)
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
#endif

  deltafs_print_sysinfo();
  return 0;
}
