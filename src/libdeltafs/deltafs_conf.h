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
// Return an ordered array of server addrs. Addrs associated with
// different servers are seperated by semicolon; addrs associated with
// a same server are seperated by comma. We are assuming each metadata
// server may have multiple addrs.
// e.g. bmi+tcp://10.0.0.1:10000;bmi+tcp://10.0.0.1:20000
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

}  // namespace config
}  // namespace pdlfs
