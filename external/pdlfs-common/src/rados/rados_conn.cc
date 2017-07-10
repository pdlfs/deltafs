/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "rados_conn.h"
#include "rados_common.h"
#include "rados_env.h"
#include "rados_fio.h"
#include "rados_osd.h"

#include "pdlfs-common/ofs.h"

namespace pdlfs {
namespace rados {

RadosOptions::RadosOptions()
    : conf_path("/tmp/ceph.conf"),
      client_mount_timeout(5),
      mon_op_timeout(5),
      osd_op_timeout(5) {}

RadosConn::~RadosConn() {
  if (cluster_ != NULL) {
    rados_shutdown(cluster_);
  }
}

static void rados_conf_set_(rados_t cluster, const char* opt, int val) {
  char tmp[20];
  snprintf(tmp, sizeof(tmp), "%d", val);
  rados_conf_set(cluster, opt, tmp);
}

Status RadosConn::Open(const RadosOptions& options) {
  int r = rados_create(&cluster_, NULL);
  if (r == 0) {
    r = rados_conf_read_file(cluster_, options.conf_path.c_str());
    if (r == 0) {
      rados_conf_set_(cluster_, "rados_mon_op_timeout", options.mon_op_timeout);
      rados_conf_set_(cluster_, "rados_osd_op_timeout", options.osd_op_timeout);
      rados_conf_set_(cluster_, "client_mount_timeout",
                      options.client_mount_timeout);
      r = rados_connect(cluster_);
    }
  }

  if (r != 0) {
    return RadosError("rados_init", r);
  } else {
    return Status::OK();
  }
}

Status RadosConn::OpenEnv(Env** result, const std::string& rados_root,
                          const std::string& pool_name, Osd* osd,
                          Env* base_env) {
  Status s;
  if (base_env == NULL) {
    base_env = Env::Default();
  }
  bool owns_osd = false;
  if (osd == NULL) {
    s = OpenOsd(&osd, pool_name);
    owns_osd = true;
  }
  if (s.ok()) {
    RadosEnv* env = new RadosEnv(base_env);
    env->rados_root_ = rados_root;
    env->wal_buf_size_ = 1 << 17;  // 128 kB
    env->owns_osd_ = owns_osd;
    env->ofs_ = new Ofs(osd);
    env->osd_ = osd;
    *result = env;
  }
  return s;
}

Status RadosConn::OpenOsd(Osd** result, const std::string& pool_name,
                          bool force_sync) {
  rados_ioctx_t ioctx;
  Status s;
  int r = rados_ioctx_create(cluster_, pool_name.c_str(), &ioctx);
  if (r != 0) {
    s = RadosError("rados_ioctx_create", r);
  } else {
    RadosOsd* osd = new RadosOsd;
    osd->pool_name_ = pool_name;
    osd->force_sync_ = force_sync;
    osd->mutex_ = &mutex_;
    osd->cluster_ = cluster_;
    osd->ioctx_ = ioctx;
    *result = osd;
  }

  return s;
}

Status RadosConn::OpenFio(Fio** result, const std::string& pool_name,
                          bool force_sync) {
  rados_ioctx_t ioctx;
  Status s;
  int r = rados_ioctx_create(cluster_, pool_name.c_str(), &ioctx);
  if (r != 0) {
    s = RadosError("rados_ioctx_create", r);
  } else {
    RadosFio* fio = new RadosFio(&mutex_);
    fio->pool_name_ = pool_name;
    fio->force_sync_ = force_sync;
    fio->cluster_ = cluster_;
    fio->ioctx_ = ioctx;
    *result = fio;
  }

  return s;
}

}  // namespace rados
}  // namespace pdlfs
