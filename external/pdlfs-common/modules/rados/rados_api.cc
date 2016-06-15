/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "rados_api.h"

namespace pdlfs {
namespace rados {

RadosOptions::RadosOptions()
    : conf_path("/tmp/ceph.conf"),
      pool_name("metadata"),
      write_buffer(1 << 17),
      async_io(true),
      client_mount_timeout(5),
      mon_op_timeout(5),
      osd_op_timeout(5) {}

}  // namespace rados
}  // namespace pdlfs
