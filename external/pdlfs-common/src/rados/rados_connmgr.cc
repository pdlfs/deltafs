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
#include "pdlfs-common/rados/rados_connmgr.h"

#include "rados_db_env.h"
#include "rados_osd.h"

#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"

namespace pdlfs {
namespace rados {

RadosConnOptions::RadosConnOptions()
    : client_mount_timeout(5), mon_op_timeout(5), osd_op_timeout(30) {}

RadosConnMgrOptions::RadosConnMgrOptions() : info_log(NULL) {}

RadosDbEnvOptions::RadosDbEnvOptions()
    : write_ahead_log_buf_size(1 << 17),
      table_file_buf_size(1 << 17),
      info_log(NULL) {}

RadosEnvOptions::RadosEnvOptions()
    : rados_root("/"), sync_directory_log_on_unmount(false), info_log(NULL) {}

RadosOptions::RadosOptions() : force_syncio(false) {}

class RadosConnMgr::Rep {
 public:
  RadosConnMgrOptions options;
  port::Mutex mutex_;
  // State below protected by mutex_
  RadosConn list;  // Dummy list header

  explicit Rep(const RadosConnMgrOptions& options) : options(options) {
    list.prev = &list;
    list.next = &list;

    if (!this->options.info_log) {
      this->options.info_log = Logger::Default();
    }
  }
};

RadosConnMgr::RadosConnMgr(const RadosConnMgrOptions& options)
    : rep_(new Rep(options)) {}

RadosConnMgr::~RadosConnMgr() {
  {
    MutexLock ml(&rep_->mutex_);
    assert(&rep_->list == rep_->list.prev);
    assert(&rep_->list == rep_->list.next);
  }

  delete rep_;
}

namespace {
inline void RadosConfSet(rados_t cluster, const char* opt, int val) {
  if (val != -1) {
    char tmp[20];
    snprintf(tmp, sizeof(tmp), "%d", val);
    rados_conf_set(cluster, opt, tmp);
  }
}

Status RadosConfAndConnect(  ///
    rados_t cluster, const char* const conf_file,
    const RadosConnOptions& options) {
  int rv = rados_conf_read_file(cluster, conf_file);
  if (rv == 0) {
    RadosConfSet(cluster, "rados_mon_op_timeout", options.mon_op_timeout);
    RadosConfSet(cluster, "rados_osd_op_timeout", options.osd_op_timeout);
    RadosConfSet(cluster, "client_mount_timeout", options.client_mount_timeout);
    rv = rados_connect(cluster);
  }
  if (rv < 0) {
    return RadosError("Cannot conf/connect to rados", rv);
  } else {
    return Status::OK();
  }
}
}  // namespace

Status RadosConnMgr::OpenConn(  ///
    const char* cluster_name, const char* user_name, const char* conf_file,
    const RadosConnOptions& options, RadosConn** conn) {
  rados_t cluster;
  int rv = rados_create2(&cluster, cluster_name, user_name, 0);
  if (rv < 0) {
    return RadosError("Error creating hdl", rv);
  }
  Status status = RadosConfAndConnect(cluster, conf_file, options);
  if (!status.ok()) {
    rados_shutdown(cluster);
    return status;
  }
  MutexLock ml(&rep_->mutex_);
  RadosConn* const new_conn =
      static_cast<RadosConn*>(malloc(sizeof(RadosConn)));
  RadosConn* list = &rep_->list;
  new_conn->next = list;
  new_conn->prev = list->prev;
  new_conn->prev->next = new_conn;
  new_conn->next->prev = new_conn;
  rados_cluster_fsid(  ///
      cluster, new_conn->cluster_fsid, sizeof(new_conn->cluster_fsid));
  new_conn->cluster = cluster;
  new_conn->nrefs = 1;
#if VERBOSE >= 1
  Log(rep_->options.info_log, 1, "Connected to cluster %s ...",
      new_conn->cluster_fsid);
#endif
  *conn = new_conn;
  return status;
}

void RadosConnMgr::Unref(RadosConn* const conn) {
  rep_->mutex_.AssertHeld();
  assert(conn->nrefs > 0);
  --conn->nrefs;
  if (!conn->nrefs) {
    conn->next->prev = conn->prev;
    conn->prev->next = conn->next;
#if VERBOSE >= 1
    Log(rep_->options.info_log, 1, "Closing connection to cluster %s ...",
        conn->cluster_fsid);
#endif
    rados_shutdown(conn->cluster);
    free(conn);
  }
}

void RadosConnMgr::Release(RadosConn* conn) {
  MutexLock ml(&rep_->mutex_);
  if (conn) {
    Unref(conn);
  }
}

Status RadosConnMgr::OpenOsd(  ///
    RadosConn* conn, const char* pool_name, const RadosOptions& options,
    Osd** result) {
  Status status;
  rados_ioctx_t ioctx;
  int rv = rados_ioctx_create(conn->cluster, pool_name, &ioctx);
  if (rv < 0) {
    status = RadosError("Cannot create ioctx", rv);
  } else {
    MutexLock ml(&rep_->mutex_);
    RadosOsd* const osd = new RadosOsd;
    osd->connmgr_ = this;
    osd->conn_ = conn;
    ++conn->nrefs;
    osd->pool_name_ = pool_name;
    osd->force_syncio_ = options.force_syncio;
    osd->ioctx_ = ioctx;
    *result = osd;
  }
  return status;
}

Env* RadosConnMgr::OpenEnv(  ///
    Osd* osd, bool owns_osd, const RadosEnvOptions& options) {
  RadosEnv* const env = new RadosEnv(options);
  env->owns_osd_ = owns_osd;
  OfsOptions oopts;
  oopts.sync_log_on_close = options.sync_directory_log_on_unmount;
  oopts.info_log = options.info_log;
  env->ofs_ = new Ofs(oopts, osd);
  env->osd_ = osd;
  return env;
}

Env* RadosConnMgr::CreateDbEnvWrapper(  ///
    Env* rados_env, bool owns_env, const RadosDbEnvOptions& options,
    Env* base_env) {
  if (!base_env) {
    base_env = Env::Default();
  }
  RadosEnv* env = dynamic_cast<RadosEnv*>(rados_env);
  RadosDbEnvWrapper* const wrapper = new RadosDbEnvWrapper(options, base_env);
  wrapper->rados_root_ = &env->options_.rados_root;
  wrapper->owns_env_ = owns_env;
  wrapper->env_ = env;
  return wrapper;
}

}  // namespace rados
}  // namespace pdlfs
