/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/dbfiles.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/fio.h"
#include "pdlfs-common/osd.h"
#include "pdlfs-common/port.h"

#include <errno.h>

namespace pdlfs {
namespace rados {

struct RadosOptions {
  RadosOptions();

  // Path to the ceph configuration file.
  // Default: "/tmp/ceph.conf"
  std::string conf_path;

  // Timeouts (in seconds) excised when bootstrapping ceph rados.
  // Default: 5 sec
  int client_mount_timeout;

  // Timeouts (in seconds) excised when communicating with ceph mon.
  // Default: 5 sec
  int mon_op_timeout;

  // Timeouts (in seconds) excised when communicating with ceph osd.
  // Default: 5 sec
  int osd_op_timeout;
};

class RadosConn {
 public:
  explicit RadosConn() : cluster_(NULL) {}
  ~RadosConn();

  // Connect to a Rados cluster instance. Return OK on success.
  Status Open(const RadosOptions& options);

  // Return an env object backed by an open Rados connection.
  // The returned instance should be deleted when it is no longer needed.
  //
  // The resulting env provides a file-system namespace tree mounted on the
  // local file system at "rados_root", such that each directory is
  // regarded as a file set mapped to a remote Rados object
  // storing the members of the file set, and each file under that set
  // is mapped to an object that stores the contents of that file.
  //
  // For example, if "rados_root" is set to "/", directory "/a/b/c" is
  // mapped to a remote object named "_a_b_c", and file "/a/b/c/d" is mapped
  // to "_a_b_c_d". If "rados_root" is "/a", directory "/a/b/c" is mapped to
  // "_b_c". If "rados_root" is "/a/b/c", directory "/a/b/c" is mapped to "_".
  //
  // If "osd" is not specified, the result of OpenOsd(pool_name) will be used
  // Callers may optionally specify a different osd instance, mostly
  // for debugging or testing purposes.
  // The "osd" must remain alive when the returned env is being used.
  //
  // If "base_env" is not specified, the result of Env::Default() will
  // be used. Callers may optionally specify a different env instance as
  // the base, mostly for debugging or testing purposes.
  // The "*base_env" must remain alive when the returned env is being used.
  Status OpenEnv(Env** result, const std::string& rados_root = "/",
                 const std::string& pool_name = "metadata", OSD* osd = NULL,
                 Env* base_env = NULL);

  // *result should be deleted when it is no longer needed.
  Status OpenOsd(OSD** result, const std::string& pool_name = "metadata",
                 bool force_sync = false);

  // *result should be deleted when it is no longer needed.
  Status OpenFio(Fio** result, const std::string& pool_name = "data",
                 bool force_sync = false);

 private:
  // No copying allowed
  void operator=(const RadosConn&);
  RadosConn(const RadosConn&);

  port::Mutex mutex_;
  void* cluster_;
};

}  // namespace rados
}  // namespace pdlfs
