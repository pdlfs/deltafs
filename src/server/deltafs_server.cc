/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
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
#include <string.h>

static void CheckSnappyCompression() {
  std::string compressed;
  // See if snappy is working by attempting to compress a compressible string
  const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
  if (!pdlfs::port::Snappy_Compress(text, sizeof(text), &compressed)) {
    pdlfs::Warn(__LOG_ARGS__, "Snappy compression is not enabled");
  } else if (compressed.size() >= sizeof(text)) {
    pdlfs::Warn(__LOG_ARGS__, "Snappy compression is not effective");
  }
}

static void CheckCrossCompile() {
  if (strcmp(PDLFS_OS, PDLFS_TARGET_OS) != 0) {
    pdlfs::Warn(__LOG_ARGS__, "Cross compile detected; code compiled for %s %s",
                PDLFS_TARGET_OS, PDLFS_TARGET_OS_VERSION);
  }
}

static void PrintWarnings() {
// Check optimization if CC is gcc or clang
// This also works for icc and craycc at least on linux
#if (defined(__GNUC__) || defined(__clang__)) && !defined(__OPTIMIZE__)
  const char msg1[] = "Optimization is disabled; code unnecessarily slow";
  pdlfs::Warn(__LOG_ARGS__, msg1);
#endif

// Check NDEBUG
#ifndef NDEBUG
  const char msg2[] = "Assertions are enabled; code unnecessarily slow";
  pdlfs::Warn(__LOG_ARGS__, msg2);
#endif

// Check GFLAGS
#ifndef PDLFS_GFLAGS
  const char msg3[] = "Gflags is not enabled; some cmd arguments are ignored";
  pdlfs::Warn(__LOG_ARGS__, msg3);
#endif

// Check GLOG
#ifndef PDLFS_GLOG
  const char msg4[] = "Glog is not enabled; will use stderr instead";
  pdlfs::Warn(__LOG_ARGS__, msg4);
#endif

// Check MPI
#ifndef DELTAFS_MPI
  const char msg5[] = "MPI is not enabled; no auto bootstrapping";
  pdlfs::Warn(__LOG_ARGS__, msg5);
#endif
}

namespace pdlfs {
extern void PrintSysInfo();
}

static pdlfs::MetadataServer* srv = NULL;

#if defined(DELTAFS_MPI)
static char procname[MPI_MAX_PROCESSOR_NAME];
static char mpiver[MPI_MAX_LIBRARY_VERSION_STRING];
static int nprocs = 1;
static int rank = 0;
#endif

static inline void MaybeInterruptServer() {
  if (srv != NULL) {
    srv->Interrupt();
  }
}

static void HandleSignal(int signal) {
  if (signal == SIGINT) {
    fprintf(stderr, "\n>>> SIGINT <<<\n");  // Send feedback to terminal
    pdlfs::Info(__LOG_ARGS__, "SIGINT received");
    MaybeInterruptServer();
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
  CheckCrossCompile();
  pdlfs::PrintSysInfo();
#if defined(DELTAFS_MPI)
  int ignored_return;
  int r = MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &ignored_return);
  if (r == MPI_SUCCESS) {
    r = MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (r == MPI_SUCCESS) {
      r = MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
      if (r == MPI_SUCCESS) {
        r = MPI_Get_processor_name(procname, &ignored_return);
      }
    }
  }
  if (r == MPI_SUCCESS) {
    char tmp[20];
    snprintf(tmp, sizeof(tmp), "%d", rank);
    setenv("DELTAFS_InstanceId", tmp, 0);
    snprintf(tmp, sizeof(tmp), "%d", nprocs);
    setenv("DELTAFS_NumOfMetadataSrvs", tmp, 0);
#if VERBOSE >= 1
    MPI_Get_library_version(mpiver, &ignored_return);
    char* slashn = strchr(mpiver, '\n');
    if (slashn != NULL) {
      *slashn = 0;
    }
    char* slashr = strchr(mpiver, '\r');
    if (slashr != NULL) {
      *slashr = 0;
    }
    pdlfs::Verbose(__LOG_ARGS__, 1, "mpi.ver -> %s", mpiver);
    pdlfs::Verbose(__LOG_ARGS__, 1, "mpi.proc -> %s (hostname)", procname);
    pdlfs::Verbose(__LOG_ARGS__, 1, "mpi.nprocs -> %d", nprocs);
    pdlfs::Verbose(__LOG_ARGS__, 1, "mpi.rank -> %d", rank);
#endif
    // MPI no longer needed
    MPI_Finalize();
  } else {
    pdlfs::Error(__LOG_ARGS__, "MPI init failed");
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
