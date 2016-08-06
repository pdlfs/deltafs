#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#if defined(__cplusplus)
extern "C" {
#endif

// Return a Rados env instance or NULL on errors.
// The conf string is something like:
//   "rados_root=/;"
//   "conf_path=/tmp/ceph.conf;"
//   "client_mount_timeout=5;"
//   "mon_op_timeout=5;"
//   "osd_op_timeout=5;"
//   "pool_name=metadata;"
extern void* pdlfs_load_rados_env(const char* conf_str);

// Return a Rados fio instance or NULL on errors.
// The conf string is something like:
//   "conf_path=/tmp/ceph.conf;"
//   "client_mount_timeout=5;"
//   "mon_op_timeout=5;"
//   "osd_op_timeout=5;"
//   "pool_name=data;"
//   "sync=false;"
extern void* pdlfs_load_rados_fio(const char* conf_str);

#if defined(__cplusplus)
}
#endif
