/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <map>
#include <vector>

#include "pdlfs-common/logging.h"
#include "pdlfs-common/map.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/rados/rados_ld.h"
#include "pdlfs-common/strutil.h"

#include "rados_conn.h"

#if defined(__cplusplus)
extern "C" {
#endif

typedef pdlfs::rados::RadosOptions options_t;
typedef pdlfs::rados::RadosConn conn_t;
typedef pdlfs::port::Mutex mutex_t;

static pdlfs::HashMap<conn_t> conn_table;
static mutex_t mutex;

static void ApplyOptions(const std::map<std::string, std::string>& raw_options,
                         options_t* options) {
  std::map<std::string, std::string>::const_iterator it;
  for (it = raw_options.begin(); it != raw_options.end(); ++it) {
    pdlfs::Slice key = it->first;
    if (key == "conf_path") {
      options->conf_path = it->second;
    } else if (key == "client_mount_timeout") {
      options->client_mount_timeout = atoi(it->second.c_str());
    } else if (key == "mon_op_timeout") {
      options->mon_op_timeout = atoi(it->second.c_str());
    } else if (key == "osd_op_timeout") {
      options->osd_op_timeout = atoi(it->second.c_str());
    }
  }
}

static conn_t* OpenRadosConn(
    const std::map<std::string, std::string>& raw_options) {
  pdlfs::MutexLock ml(&mutex);
  options_t options;
  ApplyOptions(raw_options, &options);
  conn_t* conn = conn_table.Lookup(options.conf_path);
  if (conn == NULL) {
    conn = new conn_t;
    pdlfs::Status s = conn->Open(options);
    if (!s.ok()) {
      pdlfs::Error(__LOG_ARGS__, "cannot open connection to rados: %s",
                   s.ToString().c_str());
      delete conn;
      return NULL;
    } else {
      conn_table.Insert(options.conf_path, conn);
      return conn;
    }
  } else {
    return conn;
  }
}

static void ParseOptions(std::map<std::string, std::string>* options,
                         const char* input) {
  std::vector<std::string> confs;
  pdlfs::SplitString(&confs, input);
  for (std::vector<std::string>::iterator it = confs.begin(); it != confs.end();
       ++it) {
    std::vector<std::string> pair;
    pdlfs::SplitString(&pair, it->c_str(), '=');
    if (pair.size() == 2) {
      (*options)[pair[0]] = pair[1];
    }
  }
}

void* PDLFS_Load_rados_env(const char* conf_str) {
  pdlfs::Env* env = NULL;
  std::map<std::string, std::string> options;
  ParseOptions(&options, conf_str);
  conn_t* conn = OpenRadosConn(options);
  if (conn != NULL) {
    std::string rados_root = "/";
    std::string pool_name = "metadata";
    for (std::map<std::string, std::string>::iterator it = options.begin();
         it != options.end(); ++it) {
      pdlfs::Slice key = it->first;
      if (key == "rados_root") {
        rados_root = it->second;
      } else if (key == "pool_name") {
        pool_name = it->second;
      }
    }
    pdlfs::Status s = conn->OpenEnv(&env, rados_root, pool_name);
    if (!s.ok()) {
      pdlfs::Error(__LOG_ARGS__, "cannot open rados env: %s",
                   s.ToString().c_str());
      env = NULL;
    }
  }
  return env;
}

void* PDLFS_Load_rados_fio(const char* conf_str) {
  pdlfs::Fio* fio = NULL;
  std::map<std::string, std::string> options;
  ParseOptions(&options, conf_str);
  conn_t* conn = OpenRadosConn(options);
  if (conn != NULL) {
    std::string pool_name = "data";
    for (std::map<std::string, std::string>::iterator it = options.begin();
         it != options.end(); ++it) {
      pdlfs::Slice key = it->first;
      if (key == "pool_name") {
        pool_name = it->second;
      }
    }
    pdlfs::Status s = conn->OpenFio(&fio, pool_name);
    if (!s.ok()) {
      pdlfs::Error(__LOG_ARGS__, "cannot open rados fio: %s",
                   s.ToString().c_str());
      fio = NULL;
    }
  }
  return fio;
}

#if defined(__cplusplus)
}
#endif
