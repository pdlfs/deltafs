#pragma once

/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
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
// Indicate if deltafs should trace every RPC message.
// e.g. true, yes
extern std::string RPCTracing();
// Return an ordered array of server addrs. Addrs associated with
// different servers are separated by semicolons; addrs associated with
// a same server are separated by commas. We are assuming each metadata
// server may potentially have multiple addrs.
// e.g. 10.0.0.1:10000;10.0.0.1:20000
extern std::string MetadataSrvAddrs();
// Return the size of lookup cache at each metadata client.
// e.g. 4096, 16k
extern std::string SizeOfCliLookupCache();
// Return the size of directory index cache at each metadata client.
// e.g. 4096, 16k
extern std::string SizeOfCliIndexCache();
// Indicate if deltafs should ensure atomic pathname resolutions.
// e.g. true, yes
extern std::string AtomicPathResolution();
// Indicate if deltafs should perform paranoid checks.
// e.g. true, yes
extern std::string ParanoidChecks();
// Indicate if deltafs should always verify checksums.
// e.g. true, yes
extern std::string VerifyChecksums();
// Return the name of the underlying storage provider.
// TODO: maybe there will be a need to access multiple
// underlying storage providers within a single run.
// e.g. posix, rados, hdfs
extern std::string EnvName();
// Return the conf string that should be passed to Env loaders.
// e.g. "rados_conf=/etc/ceph.conf;rados_osd_timeouts=5"
extern std::string EnvConf();
// Return the location info of input delta sets.
// e.g. "/tmp/deltafs/run3"
extern std::string InputInfo();
// Return the location info of output deltas.
// e.g. "/tmp/deltafs/run4"
extern std::string OutputInfo();

}  // namespace config
}  // namespace pdlfs
