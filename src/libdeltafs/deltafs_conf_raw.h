#pragma once

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

#include <string>

namespace pdlfs {
namespace config {

// Return the number of physcial metadata server.
// e.g. 16
extern std::string NumOfMetadataSrvs();
// Return the number of virtual metadata server.
// e.g. 256
extern std::string NumOfVirMetadataSrvs();
// Return the instance id of myself.
// e.g. 0
extern std::string InstanceId();
// Return the name of the RPC proto.
// e.g. bmi+tcp, cci, mpi
extern std::string RPCProto();
// Indicate if deltafs should trace calls to metadata server.
// e.g. true, yes
extern std::string MDSTracing();
// Return an ordered array of server addrs. Addrs are separated by ','.
// e.g. 10.0.0.1:10000,10.0.0.1:20000
extern std::string MetadataSrvAddrs();
// Return the max number of files that could be opened per client process.
// e.g. 1024
extern std::string MaxNumOfOpenFiles();
// Return the size of lease table at each metadata server.
// e.g. 4096, 16k
extern std::string SizeOfSrvLeaseTable();
// Return the size of directory table at each metadata server.
// e.g. 4096, 16k
extern std::string SizeOfSrvDirTable();
// Return the size of lookup cache at each metadata client.
// e.g. 4096, 16k
extern std::string SizeOfCliLookupCache();
// Return the size of directory index cache at each metadata client.
// e.g. 4096, 16k
extern std::string SizeOfCliIndexCache();
// Indicate if deltafs should ensure atomic pathname resolutions.
// e.g. true, yes
extern std::string AtomicPathRes();
// Indicate if deltafs should perform paranoid checks.
// e.g. true, yes
extern std::string ParanoidChecks();
// Indicate if deltafs should always verify checksums.
// e.g. true, yes
extern std::string VerifyChecksums();
// Set the size of leveldb write buffer that holds metadata updates.
// e.g. 8M, 32M
extern std::string SizeOfMetadataWriteBuffer();
// Set the size of leveldb tables that store metadata.
// e.g. 8M, 32M
extern std::string SizeOfMetadataTables();
// True if all background compaction of metadata tables should be disabled.
// e.g. true, yes
extern std::string DisableMetadataCompaction();
// Return the name of the Env implementation to use.
// XXX: support running deltafs on multiple Env instances.
// e.g. rados, hdfs
extern std::string EnvName();
// Return the conf string that should be passed to Env loaders.
// e.g. "rados_conf=/etc/ceph.conf&pool_name=metadata"
extern std::string EnvConf();
// Return the name of the Fio implementation to use.
// XXX: support running deltafs on multiple Fio instances.
// e.g. rados, hdfs
extern std::string FioName();
// Return the conf string that should be passed to Fio loaders.
// e.g. "rados_conf=/etc/ceph.conf&pool_name=data"
extern std::string FioConf();
// Return the conf string for input snapshots
extern std::string Inputs();
// Return the conf string for output snapshots
extern std::string Outputs();
// Return the name of the run directory.
// e.g. "/tmp/deltafs_run", "/var/run/deltafs"
extern std::string RunDir();

}  // namespace config
}  // namespace pdlfs
