#pragma once

/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "mds_srv.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/rpc.h"

namespace pdlfs {

class MetadataServer {
  typedef PseudoConcurrentMDSMonitor MDSMonitor;
  typedef MDS::RPC::SRV RPCWrapper;

 public:
  static Status Open(MetadataServer**);
  ~MetadataServer();
  Status Dispose();

  Status RunTillInterruptionOrError();
  void Interrupt();

 private:
  class Builder;

  // No copying allowed
  void operator=(const MetadataServer&);
  MetadataServer(const MetadataServer&);

  MetadataServer() : interrupted_(NULL), cv_(&mutex_), running_(false) {}
  static void PrintStatus(const Status&, const MDSMonitor*);
  MDSEnv* myenv_;
  port::AtomicPointer interrupted_;
  port::Mutex mutex_;
  port::CondVar cv_;
  bool running_;

  RPCServer* rpc_;
  RPCWrapper* wrapper_;

  MDS* mds_;
  MDSMonitor* mdsmon_;
  MDB* mdb_;
  DB* db_;
};

}  // namespace pdlfs
