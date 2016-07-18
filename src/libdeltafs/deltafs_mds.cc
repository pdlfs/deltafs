/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_mds.h"
#include "deltafs_conf_loader.h"
#include "mds_factory.h"
#include "pdlfs-common/logging.h"
#include "pdlfs-common/mutexlock.h"

namespace pdlfs {

// REQUIRES: can only be called by the main thread.
MetadataServer::~MetadataServer() {
  Interrupt();
  mutex_.Lock();
  while (running_) {
    cv_.Wait();
  }
  mutex_.Unlock();
  Dispose();
}

// REQUIRES: can only be called by the main thread.
Status MetadataServer::Dispose() {
  Status s;
  if (rpc_ != NULL) {
    delete rpc_;
    rpc_ = NULL;
  }
  if (wrapper_ != NULL) {
    delete wrapper_;
    wrapper_ = NULL;
  }
  if (mds_ != NULL) {
    delete mds_;
    mds_ = NULL;
  }
  if (myenv_ != NULL) {
    delete myenv_;
    myenv_ = NULL;
  }
  if (mdb_ != NULL) {
    delete mdb_;
    mdb_ = NULL;
  }
  if (db_ != NULL) {
    FlushOptions options;
    options.wait = true;
    s = db_->FlushMemTable(options);
    delete db_;
    db_ = NULL;
  }
  return s;
}

Status MetadataServer::RunTillInterruption() {
  Status s;
  MutexLock ml(&mutex_);
  if (!running_ && rpc_ != NULL) {
    Info(__LOG_ARGS__, "Deltafs is starting ...");
    s = rpc_->Start();
    if (s.ok()) {
      running_ = true;
      while (!interrupted_.Acquire_Load()) {
        cv_.Wait();
      }
      if (rpc_ != NULL) {
        Info(__LOG_ARGS__, "Deltafs is shutting down ...");
        s = rpc_->Stop();
      }
    }
    if (running_) {
      running_ = false;
      cv_.SignalAll();
    }
  }
  return s;
}

void MetadataServer::Interrupt() {
  interrupted_.Release_Store(this);
  cv_.SignalAll();
}

class MetadataServer::Builder {
 public:
  explicit Builder()
      : myenv_(NULL),
        wrapper_(NULL),
        rpc_(NULL),
        db_(NULL),
        mdb_(NULL),
        mds_(NULL) {}
  ~Builder() {}

  Status status() const { return status_; }
  MetadataServer* BuildServer();

  void LoadIds();
  void LoadMDSTopology();
  void LoadMDSEnv();
  void OpenDB();
  void OpenMDS();
  void OpenRPC();

 private:
  Status status_;
  bool ok() const { return status_.ok(); }
  MDSEnv* myenv_;
  MDSTopology mdstopo_;
  RPCWrapper* wrapper_;
  RPCServer* rpc_;
  DBOptions dbopts_;
  DB* db_;
  MDBOptions mdbopts_;
  MDB* mdb_;
  MDSOptions mdsopts_;
  MDS* mds_;
  uint64_t snap_id_;  // snapshot id
  uint64_t reg_id_;   // registry id
  int srv_id_;
};

void MetadataServer::Builder::LoadIds() {
  uint64_t instance_id;

  if (ok()) {
    status_ = config::LoadInstanceId(&instance_id);
    if (ok()) {
      srv_id_ = static_cast<int>(instance_id);
    }
  }

  if (ok()) {
    snap_id_ = 0;  // FIXME
    reg_id_ = 0;
  }
}

void MetadataServer::Builder::LoadMDSTopology() {
  uint64_t num_vir_srvs;
  uint64_t num_srvs;

  if (ok()) {
    status_ = config::LoadNumOfVirMetadataSrvs(&num_vir_srvs);
    if (ok()) {
      status_ = config::LoadNumOfMetadataSrvs(&num_srvs);
      if (ok()) {
        std::string addrs = config::MetadataSrvAddrs();
        size_t num_addrs = SplitString(addrs, ';', &mdstopo_.srv_addrs);
        if (num_addrs < num_srvs) {
          status_ = Status::InvalidArgument("Not enough addrs");
        } else if (num_addrs > num_srvs) {
          status_ = Status::InvalidArgument("Too many addrs");
        } else if (srv_id_ >= num_addrs) {
          status_ = Status::InvalidArgument("Bad srv id");
        }
      }
    }
  }

  if (ok()) {
    status_ = config::LoadRPCTracing(&mdstopo_.rpc_tracing);
  }

  if (ok()) {
    mdstopo_.rpc_proto = config::RPCProto();
    num_vir_srvs = std::max(num_vir_srvs, num_srvs);
    mdstopo_.num_vir_srvs = num_vir_srvs;
    mdstopo_.num_srvs = num_srvs;
  }
}

void MetadataServer::Builder::LoadMDSEnv() {
  myenv_ = new MDSEnv;

  if (ok()) {
    myenv_->env_name = config::EnvName();
    myenv_->env_conf = config::EnvConf();
    myenv_->env = Env::Default();  // FIXME
  }

  if (ok()) {
    myenv_->outputs = config::Outputs();
    myenv_->inputs = config::Inputs();
  }
}

// REQUIRES: both LoadIds() and LoadMDSEnv() have been called.
void MetadataServer::Builder::OpenDB() {
  std::string output_root;

  if (ok()) {
    output_root = myenv_->outputs;
    // ignore error since the directory may already exist
    myenv_->env->CreateDir(output_root);
  }

  if (ok()) {
    status_ = config::LoadVerifyChecksums(&mdbopts_.verify_checksums);
  }

  if (ok()) {
    dbopts_.create_if_missing = true;
    dbopts_.compression = kSnappyCompression;
    dbopts_.disable_compaction = true;
    dbopts_.env = myenv_->env;
  }

  if (ok()) {
    std::string dbhome = output_root;
    char tmp[30];
    snprintf(tmp, sizeof(tmp), "/meta_%d", srv_id_);
    dbhome += tmp;
    status_ = DB::Open(dbopts_, dbhome, &db_);
    if (ok()) {
      mdbopts_.db = db_;
      mdb_ = new MDB(mdbopts_);
    }
  }
}

// REQUIRES: both LoadMDSTopology() and OpenDB() have been called.
void MetadataServer::Builder::OpenMDS() {
  uint64_t lease_table_size;
  uint64_t dir_table_size;

  if (ok()) {
    status_ = config::LoadSizeOfSrvLeaseTable(&lease_table_size);
    if (ok()) {
      status_ = config::LoadSizeOfSrvDirTable(&dir_table_size);
    }
  }

  if (ok()) {
    status_ = config::LoadParanoidChecks(&mdsopts_.paranoid_checks);
  }

  if (ok()) {
    mdsopts_.mdb = mdb_;
    mdsopts_.mds_env = myenv_;
    mdsopts_.lease_table_size = lease_table_size;
    mdsopts_.dir_table_size = dir_table_size;
    mdsopts_.num_virtual_servers = mdstopo_.num_vir_srvs;
    mdsopts_.num_servers = mdstopo_.num_srvs;
    mdsopts_.snap_id = snap_id_;
    mdsopts_.reg_id = reg_id_;
    mdsopts_.srv_id = srv_id_;
  }

  if (ok()) {
    mds_ = MDS::Open(mdsopts_);
  }
}

// REQUIRES: OpenMDS() has been called.
void MetadataServer::Builder::OpenRPC() {
  std::string uri;

  if (ok()) {
    Slice srv_addr = mdstopo_.srv_addrs[srv_id_];
    Slice proto = mdstopo_.rpc_proto;
    if (!srv_addr.starts_with(proto)) {
      AppendSliceTo(&uri, proto);
      uri += "://";
    }
    AppendSliceTo(&uri, srv_addr);
  }

  if (ok()) {
    wrapper_ = new RPCWrapper(mds_);
    rpc_ = new RPCServer(wrapper_);
    rpc_->AddChannel(uri, 4);  // FIXME
  }
}

MetadataServer* MetadataServer::Builder::BuildServer() {
  LoadIds();
  LoadMDSTopology();
  LoadMDSEnv();
  OpenDB();
  OpenMDS();
  OpenRPC();

  if (ok()) {
    MetadataServer* srv = new MetadataServer;
    srv->rpc_ = rpc_;
    srv->wrapper_ = wrapper_;
    srv->mds_ = mds_;
    srv->myenv_ = myenv_;
    srv->mdb_ = mdb_;
    srv->db_ = db_;
    return srv;
  } else {
    delete rpc_;
    delete wrapper_;
    delete mds_;
    delete myenv_;
    delete mdb_;
    delete db_;
    return NULL;
  }
}

Status MetadataServer::Open(MetadataServer** srvptr) {
  Builder builder;
  *srvptr = builder.BuildServer();
  return builder.status();
}

}  // namespace pdlfs
