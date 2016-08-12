#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#ifdef __cplusplus
extern "C" {
#endif

// Return a pdlfs::Env instance or NULL on errors.
//   The conf string is something like:
//     "rados_root=/;"
//     "conf_path=/tmp/ceph.conf;"
//     "client_mount_timeout=5;"
//     "mon_op_timeout=5;"
//     "osd_op_timeout=5;"
//     "pool_name=metadata;"
// The result should be deleted when it is no longer needed.
extern void* PDLFS_Load_rados_env(const char* conf_str);

// Return a Rados fio instance or NULL on errors.
//   The conf string is something like:
//     "conf_path=/tmp/ceph.conf;"
//     "client_mount_timeout=5;"
//     "mon_op_timeout=5;"
//     "osd_op_timeout=5;"
//     "pool_name=data;"
//     "sync=false;"
// The result should be deleted when it is no longer needed.
extern void* PDLFS_Load_rados_fio(const char* conf_str);

#ifdef __cplusplus
}
#endif
