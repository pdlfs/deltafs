/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "../libdeltafs/mds_srv.h"
#if defined(GFLAGS)
#include <gflags/gflags.h>
#endif
#if defined(GLOG)
#include <glog/logging.h>
#endif
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

namespace pdlfs {

class ServerDaemon {
 public:
  ServerDaemon() : interrupted_(NULL), cv_(&mu_) {}

  int Init() {
    Status s;
    Env* env = Env::Default();
    mds_env_.env = env;
    std::string fshome = "/tmp/deltafs_server";
    env->CreateDir(fshome);
    
    DBOptions dbopts;
    dbopts.env = env;
    dbopts.create_if_missing = true;
    s = DB::Open(dbopts, fshome, &db_);
    if (s.ok()) {
      MDBOptions mdbopts;
      mdbopts.db = db_;
      mdb_ = new MDB(mdbopts);
      MDSOptions mdsopts;
      mdsopts.mds_env = &mds_env_;
      mdsopts.mdb = mdb_;
      mds_ = MDS::Open(mdsopts);

      rpc_if_ = new MDS::RPC::SRV(mds_);
      rpc_ = new RPCServer(rpc_if_, env);
      rpc_->AddChannel("bmi+tcp://localhost:10101", 4);
    } else {
      db_ = NULL;
      mdb_ = NULL;
      mds_ = NULL;
      rpc_if_ = NULL;
      rpc_ = NULL;
    }

    return s.err_code();
  }

  int RunTillInterrupt() {
#if defined(GLOG)
    VLOG(1) << "Deltafs is starting ...";
#endif
    Status s = rpc_->Start();
    if (!s.ok()) {
      return s.err_code();
    } else {
      mu_.Lock();
      while (!interrupted_.Acquire_Load()) {
        cv_.Wait();
      }
      mu_.Unlock();
#if defined(GLOG)
      VLOG(1) << "Deltafs is shutting down ...";
#endif
      s = rpc_->Stop();
      return s.err_code();
    }
  }

  void Interrupt() {
    interrupted_.Release_Store(this);
    cv_.SignalAll();
  }

  void Dispose() {
    delete rpc_;
    delete rpc_if_;
    delete mds_;
    delete mdb_;
    delete db_;
  }

 private:
  port::AtomicPointer interrupted_;
  port::Mutex mu_;
  port::CondVar cv_;

  DB* db_;
  MDB* mdb_;
  MDS* mds_;
  MDSEnv mds_env_;
  rpc::If* rpc_if_;
  RPCServer* rpc_;
};

}  // namespace pdlfs

static ::pdlfs::ServerDaemon deltafs;

static void Shutdown() { deltafs.Interrupt(); }
static void HandleSignal(int signal) {
#if defined(GLOG)
  if (signal == SIGINT) {
    VLOG(1) << "SIGINT received";
  } else if (signal == SIGTERM) {
    VLOG(1) << "SIGTERM received";
  }
#endif
  Shutdown();
}

int main(int argc, char* argv[]) {
#if defined(GFLAGS)
  ::google::ParseCommandLineFlags(&argc, &argv, true);
#endif
#if defined(GLOG)
  ::google::InitGoogleLogging(argv[0]);
  ::google::InstallFailureSignalHandler();
  VLOG(1) << "Deltafs is initializing ...";
#endif
  int r = deltafs.Init();
  if (r != 0) {
    exit(r);
  }
  signal(SIGINT, HandleSignal);
  signal(SIGTERM, HandleSignal);
  r = deltafs.RunTillInterrupt();
  deltafs.Dispose();
#if defined(GLOG)
  VLOG(1) << "Bye!";
#endif
  return r;
}
