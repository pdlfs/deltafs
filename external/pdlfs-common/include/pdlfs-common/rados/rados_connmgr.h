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
#pragma once

#include "pdlfs-common/env.h"
#include "pdlfs-common/osd.h"
#include "pdlfs-common/status.h"

#include <string>

namespace pdlfs {
namespace rados {

struct RadosConn;  // Opaque handle to a rados cluster
struct RadosConnOptions {
  RadosConnOptions();
  // Timeouts (in seconds) excised when bootstrapping ceph rados. Set to -1 to
  // skip this configuration.
  // Default: -1
  int client_mount_timeout;

  // Timeouts (in seconds) excised when communicating with ceph mon.
  // Default: -1
  int mon_op_timeout;

  // Timeouts (in seconds) excised when communicating with ceph osd.
  // Default: -1
  int osd_op_timeout;
};

// Options for constructing a RadosConnMgr.
struct RadosConnMgrOptions {
  RadosConnMgrOptions();
  // Logger object for information.
  // Default: NULL
  Logger* info_log;
};

// Options for creating a rados db env wrapper.
struct RadosDbEnvOptions {
  RadosDbEnvOptions();

  // Write buffer size for db write ahead logs.
  // Default: 128KB
  uint64_t write_ahead_log_buf_size;

  // Write buffer size for db table files.
  // Default: 128KB
  uint64_t table_file_buf_size;

  // Logger for env internal/error information.
  // Default: NULL
  Logger* info_log;
};

// Options for constructing a rados env.
struct RadosEnvOptions {
  RadosEnvOptions();
  // Rados mount point. All files and directories beneath it will sink into
  // rados and be stored as plain data objects and special directory (fileset)
  // objects. The portion of a file or a directory path beyond the mount point
  // will be used to name a rados object or a rados fileset. Calling files or
  // directories out of the mount point results in errors.
  // Default: "/"
  std::string rados_root;
  // Perform an additional sync on the log of a directory (fileset) when the
  // directory is unmounted.
  // Default: false
  bool sync_directory_log_on_unmount;
  // Logger for env internal/error information.
  // Default: NULL
  Logger* info_log;
};

// Options for constructing a rados osd instance.
struct RadosOptions {
  RadosOptions();
  // Disable async i/o. All write operations are done synchronously.
  // Default: false
  bool force_syncio;
};

// The primary interface an external user uses to obtain rados env objects.
// Creating a rados env is a 3-step process. A user first opens a rados
// connection. Next, the user uses the connection to create a rados osd object.
// The user then uses the rados osd object to obtain a rados env.
class RadosConnMgr {
 public:
  explicit RadosConnMgr(const RadosConnMgrOptions& options);
  ~RadosConnMgr();

  // Open a rados connection. Return OK on success, or a non-OK status on
  // errors. The returned rados connection instance shall be released through
  // the connection manager when it is no longer needed.
  //
  // REQUIRES: user_name must be specified if ceph auth (cephx) is enabled; not
  // sure about cluster_name (it can be NULL or even be randomly set sometimes)
  // but conf_file is allowed to be NULL. When conf_file is set to NULL, ceph
  // will try to locate a config file at a list of fixed locations including
  // $CEPH_CONF (environment variable), "/etc/ceph/ceph.conf", "~/.ceph/config",
  // and "./ceph.conf". A keyring must be configured in the ceph config file
  // (conf_file) for the specified user (user_name) when ceph auth (cephx) is
  // enabled. In addition to that, at least one mon address shall be listed in
  // the ceph config file.
  //
  // For more information, see
  // https://docs.ceph.com/docs/master/rados/api/librados-intro/.
  Status OpenConn(const char* cluster_name, const char* user_name,
                  const char* conf_file, const RadosConnOptions& options,
                  RadosConn** conn);

  // Create a rados osd instance backed by an open rados connection. Return OK
  // on success, or a non-OK status on errors. The returned osd instance shall
  // be deleted when it is no longer needed.
  Status OpenOsd(RadosConn* conn, const char* pool_name,
                 const RadosOptions& options, Osd** osd);

  // Create a rados env instance backed by an open osd instance. The returned
  // env instance shall be deleted when it is no longer needed. For
  // testing/debugging purposes, a non-rados osd instance may be used to create
  // a ceph rados env.
  //
  // The resulting env provides a virtual filesystem namespace tree mounted on
  // the caller's local namespace at options.rados_root. File and directory
  // operations are emulated atop rados such that each directory is regarded as
  // a fileset mapped to a remote rados object storing the members of the
  // fileset, and each file under that set is mapped to an object that stores
  // the contents of that file.
  //
  // For example, if "rados_root" is set to "/", directory "/a/b/c" will be
  // mapped to a remote object named "_a_b_c", and file "/a/b/c/d" will be
  // mapped to "_a_b_c_d". If "rados_root" is "/a", directory "/a/b/c" will then
  // be mapped to "_b_c". And if "rados_root" is "/a/b/c", directory "/a/b/c"
  // will then be mapped to "_".
  //
  // REQUIRES: osd is not NULL; if owns_osd is false, osd must remain alive
  // while the returned env is being used.
  //
  // This operation does not incur remote rados operations and will not fail.
  static Env* OpenEnv(Osd* osd, bool owns_osd, const RadosEnvOptions& options);

  // Create a wrapper env object atop a given rados env so that a db may run on
  // top of it. 3 types of db files are stored locally (in a locally-mirrored
  // directory) instead of rados: db info log files, db LOCK files, and tmp
  // files to be renamed to special db CURRENT files.
  //
  // REQUIRES: rados_env is one returned by OpenEnv(); if owns_env is false,
  // rados_env must remain alive while the returned env is being used; if
  // base_env is NULL, Env::Default() is used.
  //
  // This operation does not incur remote rados operations and will not fail.
  static Env* CreateDbEnvWrapper(Env* rados_env, bool owns_env,
                                 const RadosDbEnvOptions& options,
                                 Env* base_env = NULL);

  void Release(RadosConn* conn);

 private:
  // No copying allowed
  void operator=(const RadosConnMgr& other);
  RadosConnMgr(const RadosConnMgr&);

  void Unref(RadosConn* conn);

  class Rep;
  Rep* rep_;
};

}  // namespace rados
}  // namespace pdlfs
