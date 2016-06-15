#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/env.h"
#include "pdlfs-common/osd.h"

namespace pdlfs {
namespace rados {

struct RadosOptions {
  RadosOptions();

  // Path to the ceph configuration file.
  // Default: "/tmp/ceph.conf"
  const char* conf_path;

  // Name of the rados pool for object storage.
  // Default: "metadata"
  const char* pool_name;

  // Client write buffer size.
  // Default: 128KB
  int write_buffer;

  // Whether write requests should be sent asynchronously.
  // Default: true
  bool async_io;

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

// Return an object device backed by a Ceph rados cluster.
// The returned instance should be deleted when it is no longer needed.
extern OSD* NewRadosOSD(const RadosOptions& options);

// Return an Env backed by a Ceph rados OSD.
// The returned instance should be deleted when it is no longer needed.
//
// A Ceph rados Env provides a file-system namespace tree mounted on the
// local file system at "rados_root", such that each directory is
// regarded as a file set mapped to a remote Ceph rados object
// storing the members of the file set, and each file under that set
// is mapped to an object that stores the contents of that file.
//
// For example, if "rados_root" is set to "/", directory "/a/b/c" is
// mapped to a remote object named "_a_b_c", and file "/a/b/c/d" is mapped
// to "_a_b_c_d". If "rados_root" is "/a", directory "/a/b/c" is mapped to
// "_b_c". If "rados_root" is "/a/b/c", directory "/a/b/c" is mapped to "_".
//
// If "osd" is not specified, the result of NewRadosOSD() will be used.
// Callers may optionally specify a different osd instance, mostly
// for debugging or testing purposes.
// The "osd" must remain alive when this Env is being used.
//
// If "base_env" is not specified, the result of Env::Default() will
// be used. Callers may optionally specify a different env instance as
// the base, mostly for debugging or testing purposes.
// The "*base_env" must remain alive when this Env is being used.
extern Env* NewRadosEnv(const RadosOptions& options,
                        const std::string& rados_root = "/", OSD* osd = NULL,
                        Env* base_env = NULL);

// Load a directory (usually created by others) from Ceph rados.
// Loaded directories can only be accessed read-only.
//
// Please use Env::CreateDir() to open a directory with read-write access.
extern Status SoftCreateDir(Env* rados_env, const Slice& dir);

// Unload a directory that is previously loaded.
// Future access to the directory from this env will return NotFound,
// but the directory unloaded is not physically removed.
//
// Please use Env::DeleteDir() to permanently remove a directory.
extern Status SoftDeleteDir(Env* rados_env, const Slice& dir);

}  // namespace rados
}  // namespace pdlfs
