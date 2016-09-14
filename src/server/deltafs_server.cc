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

#include "pdlfs-common/pdlfs_config.h"

#if defined(PDLFS_WITH_MPI)
#include <mpi.h>
#endif

#include "../libdeltafs/deltafs_mds.h"

#if defined(GFLAGS)
#include <gflags/gflags.h>
#endif
#if defined(GLOG)
#include <glog/logging.h>
#endif

static pdlfs::MetadataServer* srv = NULL;
#if defined(PDLFS_WITH_MPI)
static int srv_id = 0;
static int num_srvs = 1;
#endif

static void Shutdown() {
  if (srv != NULL) {
    srv->Interrupt();
  }
}
static void HandleSignal(int signal) {
  if (signal == SIGINT) {
    pdlfs::Info(__LOG_ARGS__, "SIGINT received");
    Shutdown();
  }
}

int main(int argc, char* argv[]) {
#if defined(GFLAGS)
  google::ParseCommandLineFlags(&argc, &argv, true);
#endif
#if defined(GLOG)
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
#endif
#if defined(PDLFS_WITH_MPI)
  int ignored_return;
  int r = MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &ignored_return);
  if (r == MPI_SUCCESS) {
    r = MPI_Comm_rank(MPI_COMM_WORLD, &srv_id);
    if (r == MPI_SUCCESS) {
      r = MPI_Comm_size(MPI_COMM_WORLD, &num_srvs);
    }
    MPI_Finalize();  // MPI no longer needed
  }
  if (r == MPI_SUCCESS) {
    char tmp[20];
    snprintf(tmp, sizeof(tmp), "%d", srv_id);
    setenv("DELTAFS_InstanceId", tmp, 0);
    snprintf(tmp, sizeof(tmp), "%d", num_srvs);
    setenv("DELTAFS_NumOfMetadataSrvs", tmp, 0);
  } else {
    fprintf(stderr, "MPI_init failed\n");
    abort();
  }
#endif
  pdlfs::Info(__LOG_ARGS__, "Deltafs is initializing ...");
  pdlfs::Status s = pdlfs::MetadataServer::Open(&srv);
  if (s.ok()) {
    signal(SIGINT, HandleSignal);
    s = srv->RunTillInterruptionOrError();
    srv->Dispose();
  }
  delete srv;
  if (!s.ok()) {
    pdlfs::Error(__LOG_ARGS__, s);
    return 1;
  } else {
    pdlfs::Info(__LOG_ARGS__, "Bye!");
    return 0;
  }
}
