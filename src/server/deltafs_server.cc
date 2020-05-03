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
#include "../libdeltafs/deltafs_mds.h"
#include "../libdeltafs/util/logging.h"
#include "deltafs/deltafs_config.h"

#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/pdlfs_platform.h"
#include "pdlfs-common/port.h"

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

namespace {
void CheckSnappyCompression() {
  std::string compressed;
  // See if snappy is working by attempting to compress a compressible string
  const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
  if (!pdlfs::port::Snappy_Compress(text, sizeof(text), &compressed)) {
    pdlfs::Warn(__LOG_ARGS__, "Snappy compression is not enabled");
  } else if (compressed.size() >= sizeof(text)) {
    pdlfs::Warn(__LOG_ARGS__, "Snappy compression is not effective");
  }
}

void CheckCrossCompile() {
  if (strcmp(PDLFS_HOST_OS, PDLFS_TARGET_OS) != 0) {
    pdlfs::Warn(__LOG_ARGS__, "Cross compile detected; code compiled for %s %s",
                PDLFS_TARGET_OS, PDLFS_TARGET_OS_VERSION);
  }
}

void PrintWarnings() {
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
  const char msg3[] = "gflags is not enabled; some cmd arguments are ignored";
  pdlfs::Warn(__LOG_ARGS__, msg3);
#endif

// Check GLOG
#ifndef PDLFS_GLOG
  const char msg4[] = "glog is not enabled; will use stderr instead";
  pdlfs::Warn(__LOG_ARGS__, msg4);
#endif

// Check MPI
#ifndef DELTAFS_MPI
  const char msg5[] = "MPI is not enabled; no auto bootstrapping";
  pdlfs::Warn(__LOG_ARGS__, msg5);
#endif
}
}  // namespace

namespace pdlfs {
extern void PrintSysInfo();
}

namespace {
pdlfs::MetadataServer* srv = NULL;

#if defined(DELTAFS_MPI)
char procname[MPI_MAX_PROCESSOR_NAME];
#if VERBOSE >= 1
#if MPI_VERSION >= 3
char mpistr[MPI_MAX_LIBRARY_VERSION_STRING];
#endif  // MPI_VERSION
int mpiver = 0;
int mpisubver = 0;
#endif  // VERBOSE
int nprocs = 1;
int rank = 0;
#endif

inline void MaybeInterruptServer() {
  if (srv != NULL) {
    srv->Interrupt();
  }
}

void HandleSignal(int signal) {
  if (signal == SIGINT) {
    fprintf(stderr, "\n>>> SIGINT <<<\n");  // Send feedback to terminal
    pdlfs::Info(__LOG_ARGS__, "SIGINT received");
    MaybeInterruptServer();
  }
}
}  // namespace

int main(int argc, char* argv[]) {
#if defined(PDLFS_GFLAGS)
  google::SetUsageMessage("Deltafs metadata server");
  google::SetVersionString(PDLFS_COMMON_VERSION);
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
    MPI_Get_version(&mpiver, &mpisubver);
#if MPI_VERSION >= 3
    MPI_Get_library_version(mpistr, &ignored_return);
    char* slashn = strchr(mpistr, '\n');
    if (slashn != NULL) {
      *slashn = 0;
    }
    char* slashr = strchr(mpistr, '\r');
    if (slashr != NULL) {
      *slashr = 0;
    }
    pdlfs::Verbose(__LOG_ARGS__, 1, "mpi.ver -> MPI %d.%d / %s", mpiver,
                   mpisubver, mpistr);
#else
    pdlfs::Verbose(__LOG_ARGS__, 1, "mpi.ver -> MPI %d.%d", mpiver, mpisubver);
#endif  // MPI_VERSION
    pdlfs::Verbose(__LOG_ARGS__, 1, "mpi.name -> %s (MPI_Get_processor_name)",
                   procname);
    pdlfs::Verbose(__LOG_ARGS__, 1, "mpi.commsz -> %d", nprocs);
    pdlfs::Verbose(__LOG_ARGS__, 1, "mpi.rank -> %d", rank);
#endif               // VERBOSE
    MPI_Finalize();  // MPI NO LONGER NEEDED
  } else {
    pdlfs::Error(__LOG_ARGS__, "MPI_Init failed");
    abort();
  }
#endif  // DELTAFS_MPI
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
