/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs/deltafs_config.h"

#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/pdlfs_platform.h"
#include "pdlfs-common/port.h"

#include "../libdeltafs/deltafs_mds.h"

#include "pdlfs-common/logging.h"

#if defined(PDLFS_GFLAGS)
#include <gflags/gflags.h>
#endif

#if defined(PDLFS_GLOG)
#include <glog/logging.h>
#endif

#if defined(DELTAFS_MPI)
#include <mpi.h>
#endif

#include <signal.h>
#include <stdlib.h>

static void CheckSnappyCompression() {
  std::string compressed;
  // See if snappy is working by attempting to compress a compressible string
  const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
  if (!pdlfs::port::Snappy_Compress(text, sizeof(text), &compressed)) {
    pdlfs::Warn(__LOG_ARGS__, "WARNING: Snappy compression is not enabled");
  } else if (compressed.size() >= sizeof(text)) {
    pdlfs::Warn(__LOG_ARGS__, "WARNING: Snappy compression is not effective");
  }
}

static void PrintWarnings() {
// Check optimization if GCC
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
  const char msg1[] =
      "WARNING: Optimization is disabled; code unnecessarily slow";
  pdlfs::Warn(__LOG_ARGS__, msg1);
#endif

// Check NDEBUG
#ifndef NDEBUG
  const char msg2[] =
      "WARNING: Assertions are enabled; code unnecessarily slow";
  pdlfs::Warn(__LOG_ARGS__, msg2);
#endif

// Check GFLAGS
#ifndef PDLFS_GFLAGS
  const char msg3[] =
      "WARNING: Gflags is not enabled; command line arguments are ignored";
  pdlfs::Warn(__LOG_ARGS__, msg3);
#endif

// Check GLOG
#ifndef PDLFS_GLOG
  const char msg4[] =
      "WARNING: Glog is not enabled; will log to stderr instead";
  pdlfs::Warn(__LOG_ARGS__, msg4);
#endif

// Check MPI
#ifndef DELTAFS_MPI
  const char msg5[] = "WARNING: MPI is not enabled; no auto bootstrapping";
  pdlfs::Warn(__LOG_ARGS__, msg5);
#endif
}

static pdlfs::MetadataServer* srv = NULL;
#if defined(DELTAFS_MPI)
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
#if defined(PDLFS_GFLAGS)
  google::ParseCommandLineFlags(&argc, &argv, true);
#endif
#if defined(PDLFS_GLOG)
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
#endif
  PrintWarnings();
  CheckSnappyCompression();
#if defined(DELTAFS_MPI)
  int ignored_return;
  int r = MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &ignored_return);
  if (r == MPI_SUCCESS) {
    r = MPI_Comm_rank(MPI_COMM_WORLD, &srv_id);
    if (r == MPI_SUCCESS) {
      r = MPI_Comm_size(MPI_COMM_WORLD, &num_srvs);
    }
    // MPI no longer needed
    MPI_Finalize();
  }
  if (r == MPI_SUCCESS) {
    char tmp[20];
    snprintf(tmp, sizeof(tmp), "%d", srv_id);
    setenv("DELTAFS_InstanceId", tmp, 0);
    snprintf(tmp, sizeof(tmp), "%d", num_srvs);
    setenv("DELTAFS_NumOfMetadataSrvs", tmp, 0);
  } else {
    pdlfs::Error(__LOG_ARGS__, "MPI_init failed\n");
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
    pdlfs::Error(__LOG_ARGS__, "%s", s.ToString().c_str());
    return 1;
  } else {
    pdlfs::Info(__LOG_ARGS__, "Bye!");
    return 0;
  }
}
