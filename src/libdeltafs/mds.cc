/*
* Copyright (c) 2014-2016 Carnegie Mellon University.
*
* All rights reserved.
*
* Use of this source code is governed by a BSD-style license that can be
* found in the LICENSE file. See the AUTHORS file for names of contributors.
*/

#include "mds_api.h"
#include "mds_cli.h"
#include "mds_srv.h"

namespace pdlfs {

Slice MDS::EncodeId(const DirId& id, char* scratch) {
  char* p = scratch;
  p = EncodeVarint64(p, id.reg);
  p = EncodeVarint64(p, id.snap);
  p = EncodeVarint64(p, id.ino);
  return Slice(scratch, p - scratch);
}

// Deterministically map directories to their zeroth servers.
int MDS::PickupServer(const DirId& id) {
  char tmp[30];
  Slice encoding = EncodeId(id, tmp);
  int zserver = DirIndex::RandomServer(encoding, 0);
  return zserver;
}

MDSOptions::MDSOptions()
    : mds_env(NULL),
      mdb(NULL),
      dir_table_size(4096),
      lease_table_size(4096),
      lease_duration(1000 * 1000),
      snap_id(0),
      reg_id(0),
      paranoid_checks(false),
      num_virtual_servers(1),
      num_servers(1),
      srv_id(0) {}

MDS::SRV::SRV(const MDSOptions& options)
    : mds_env_(options.mds_env),
      mdb_(options.mdb),
      paranoid_checks_(options.paranoid_checks),
      lease_duration_(options.lease_duration),
      snap_id_(options.snap_id),
      reg_id_(options.reg_id),
      srv_id_(options.srv_id),
      loading_cv_(&mutex_),
      ino_(0) {
  giga_.num_servers = options.num_servers;
  giga_.num_virtual_servers = options.num_virtual_servers;
  giga_.paranoid_checks = options.paranoid_checks;

  LeaseOptions lease_options;
  lease_options.max_lease_duration = options.lease_duration;
  lease_options.max_num_leases = options.lease_table_size;
  leases_ = new LeaseTable(lease_options);

  dirs_ = new DirTable(options.dir_table_size);

  assert(srv_id_ >= 0);
  uint64_t tmp = srv_id_;
  tmp <<= 32;
  ino_ = tmp;
}

MDS::SRV::~SRV() {
  delete leases_;
  delete dirs_;
}

MDS* MDS::Open(const MDSOptions& options) {
  assert(options.mds_env != NULL && options.mds_env->env != NULL);
  assert(options.mdb != NULL);
  // TODO: print options
  MDS* mds = new SRV(options);
  return mds;
}

MDSFactory::~MDSFactory() {}

MDSCliOptions::MDSCliOptions()
    : env(NULL),
      factory(NULL),
      index_cache_size(4096),
      lookup_cache_size(4096),
      paranoid_checks(false),
      atomic_path_resolution(false),
      max_redirects_allowed(20),
      num_virtual_servers(1),
      num_servers(1),
      cli_id(0),
      uid(0),
      gid(0) {}

MDS::CLI::CLI(const MDSCliOptions& options)
    : env_(options.env),
      factory_(options.factory),
      paranoid_checks_(options.paranoid_checks),
      atomic_path_resolution_(options.atomic_path_resolution),
      max_redirects_allowed_(options.max_redirects_allowed),
      cli_id_(options.cli_id),
      uid_(options.uid),
      gid_(options.gid) {
  giga_.num_servers = options.num_servers;
  giga_.num_virtual_servers = options.num_virtual_servers;
  giga_.paranoid_checks = options.paranoid_checks;

  lookup_cache_ = new LookupCache(options.lookup_cache_size);
  index_cache_ = new IndexCache(options.index_cache_size);
}

MDS::CLI::~CLI() {
  delete index_cache_;
  delete lookup_cache_;
}

MDS::CLI* MDS::CLI::Open(const MDSCliOptions& raw_options) {
  assert(raw_options.factory != NULL);
  MDSCliOptions options(raw_options);
  if (options.env == NULL) {
    options.env = Env::Default();
  }

  MDS::CLI* cli = new MDS::CLI(options);
  return cli;
}

}  // namespace pdlfs
