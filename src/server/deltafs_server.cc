/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <signal.h>
#include <stdlib.h>

#include "../libdeltafs/deltafs_mds.h"

#if defined(GFLAGS)
#include <gflags/gflags.h>
#endif
#if defined(GLOG)
#include <glog/logging.h>
#endif

static pdlfs::MetadataServer* srv = NULL;

static void Shutdown() {
  if (srv != NULL) {
    srv->Interrupt();
  }
}
static void HandleSignal(int signal) {
  if (signal == SIGINT) {
    pdlfs::Info(__LOG_ARGS__, "SIGINT received");
  }
  Shutdown();
}

int main(int argc, char* argv[]) {
#if defined(GFLAGS)
  google::ParseCommandLineFlags(&argc, &argv, true);
#endif
#if defined(GLOG)
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
#endif
  pdlfs::Info(__LOG_ARGS__, "Deltafs is initializing ...");
  pdlfs::Status s = pdlfs::MetadataServer::Open(&srv);
  if (s.ok()) {
    signal(SIGINT, HandleSignal);
    s = srv->RunTillInterruption();
    srv->Dispose();
  }
  delete srv;
  if (!s.ok()) {
    pdlfs::Error(__LOG_ARGS__, "Failed - %s", s.ToString().c_str());
    return 1;
  } else {
    pdlfs::Info(__LOG_ARGS__, "Bye!");
    return 0;
  }
}
