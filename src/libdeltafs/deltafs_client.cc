/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_client.h"
#include "deltafs_conf.h"
#if defined(PDLFS_PLATFORM_POSIX)
#include <sys/types.h>
#include <unistd.h>
#endif

#include "pdlfs-common/rpc.h"
#include "pdlfs-common/strutil.h"

namespace pdlfs {

Client::~Client() {
  delete mdscli_;
  delete mdsfty_;
}

// Open a file for writing. Return OK on success.
// If the file already exists, it is not truncated.
// If the file doesn't exist, it will be created.
Status Client::Wopen(const Slice& path, int mode, int* fd) {
  Status s;
  Fentry ent;
  s = mdscli_->Fcreat(path, mode, &ent);
  if (s.IsAlreadyExists()) {
    s = mdscli_->Fstat(path, &ent);
  }

  if (s.ok()) {
    s = blk_db_->Open(ent.pid, ent.nhash, ent.stat,
                      true,   // create if missing
                      false,  // error if exists
                      fd);
#if 0
    if (s.ok()) {
      if (blk_db_->GetStream(fd)->size != ent.stat.FileSize()) {
        // FIXME
      }
    }
#endif
  }
  return s;
}

// Write a chunk of data at a specified offset. Return OK on success.
Status Client::Pwrite(int fd, const Slice& data, uint64_t off) {
  return blk_db_->Pwrite(fd, data, off);
}

// Open a file for reading. Return OK on success.
// If the file doesn't exist, it won't be created.
Status Client::Ropen(const Slice& path, int* fd) {
  Status s;
  Fentry ent;
  s = mdscli_->Fstat(path, &ent);

  if (s.ok()) {
    // Create the file if missing since the MDS says it exists.
    s = blk_db_->Open(ent.pid, ent.nhash, ent.stat,
                      true,   // create if missing
                      false,  // error if exists
                      fd);
#if 0
    if (s.ok()) {
      if (blk_db_->GetStream(fd)->size != ent.stat.FileSize()) {
        // FIXME
      }
    }
#endif
  }
  return s;
}

// Read a chunk of data at a specified offset. Return OK on success.
Status Client::Pread(int fd, Slice* result, uint64_t off, uint64_t size,
                     char* buf) {
  return blk_db_->Pread(fd, result, off, size, buf);
}

Status Client::Close(int fd) {
  Status s = blk_db_->Sync(fd);
  if (s.ok()) {
    return blk_db_->Close(fd);
  } else {
    return s;
  }
}

Status Client::Mkfile(const Slice& path, int mode) {
  Status s;
  Fentry ent;
  s = mdscli_->Fcreat(path, mode, &ent);
  return s;
}

Status Client::Mkdir(const Slice& path, int mode) {
  Status s;
  s = mdscli_->Mkdir(path, mode);
  return s;
}

class Client::Loader {
 public:
  static int FetchUid() {
#if defined(PDLFS_PLATFORM_POSIX)
    return getuid();
#else
    return 0;
#endif
  }

  static int FetchGid() {
#if defined(PDLFS_PLATFORM_POSIX)
    return getgid();
#else
    return 0;
#endif
  }

  void LoadEnv();
  void LoadMDSTopology();
  void LoadMDSCliOptions();

  Status status_;
  bool ok() const { return status_.ok(); }
  Env* env_;
  MDSTopology mdstopo_;
  MDSFactoryImpl* mdsfty_;
  MDSCliOptions mdscliopts_;
  MDSClient* mdscli_;
  int cli_id_;
  int uid_;
  int gid_;
};

#define DEF_LOADER_UINT64(conf)                          \
  static Status Load##conf(uint64_t* dst) {              \
    std::string str_##conf = config::conf();             \
    if (!ParsePrettyNumber(str_##conf, dst)) {           \
      return Status::InvalidArgument(#conf, str_##conf); \
    } else {                                             \
      return Status::OK();                               \
    }                                                    \
  }
#define DEF_LOADER_BOOL(conf)                            \
  static Status Load##conf(bool* dst) {                  \
    std::string str_##conf = config::conf();             \
    if (!ParsePrettyBool(str_##conf, dst)) {             \
      return Status::InvalidArgument(#conf, str_##conf); \
    } else {                                             \
      return Status::OK();                               \
    }                                                    \
  }

DEF_LOADER_UINT64(NumOfVirMetadataSrvs)
DEF_LOADER_UINT64(NumOfMetadataSrvs)
DEF_LOADER_UINT64(SizeOfCliLookupCache)
DEF_LOADER_UINT64(SizeOfCliIndexCache)

DEF_LOADER_BOOL(AtomicPathResolution)
DEF_LOADER_BOOL(ParanoidChecks)
DEF_LOADER_BOOL(RPCTracing)

void Client::Loader::LoadEnv() {
  env_ = Env::Default();
  uid_ = FetchUid();
  gid_ = FetchGid();
  cli_id_ = 0;
}

// REQUIRES: LoadEnv() has been called.
void Client::Loader::LoadMDSTopology() {
  uint64_t num_vir_srvs;
  uint64_t num_srvs;

  if (ok()) {
    status_ = LoadNumOfVirMetadataSrvs(&num_vir_srvs);
    if (ok()) {
      status_ = LoadNumOfMetadataSrvs(&num_srvs);
      if (ok()) {
        std::string addrs = config::MetadataSrvAddrs();
        size_t num_addrs = SplitString(addrs, ';', &mdstopo_.srv_addrs);
        if (num_addrs < num_srvs) {
          status_ = Status::InvalidArgument("Not enough addrs");
        } else if (num_addrs > num_srvs) {
          status_ = Status::InvalidArgument("Too many addrs");
        }
      }
    }
  }

  if (ok()) {
    status_ = LoadRPCTracing(&mdstopo_.rpc_tracing);
  }

  if (ok()) {
    mdstopo_.rpc_proto = config::RPCProto();
    num_vir_srvs = std::max(num_vir_srvs, num_srvs);
    mdstopo_.num_vir_srvs = num_vir_srvs;
    mdstopo_.num_srvs = num_srvs;
  }

  if (ok()) {
    MDSFactoryImpl* fty = new MDSFactoryImpl(env_);
    status_ = fty->Init(mdstopo_);
    if (ok()) {
      status_ = fty->Start();
    }
    if (ok()) {
      mdsfty_ = fty;
    } else {
      delete fty;
    }
  }
}

// REQUIRES: LoadMDSTopology() has been called.
void Client::Loader::LoadMDSCliOptions() {
  uint64_t idx_cache_sz;
  uint64_t lookup_cache_sz;

  if (ok()) {
    status_ = LoadSizeOfCliIndexCache(&idx_cache_sz);
    if (ok()) {
      status_ = LoadSizeOfCliLookupCache(&lookup_cache_sz);
    }
  }

  if (ok()) {
    status_ = LoadParanoidChecks(&mdscliopts_.paranoid_checks);
    if (ok()) {
      status_ = LoadAtomicPathResolution(&mdscliopts_.atomic_path_resolution);
    }
  }

  if (ok()) {
    mdscliopts_.env = env_;
    mdscliopts_.factory = mdsfty_;
    mdscliopts_.index_cache_size = idx_cache_sz;
    mdscliopts_.lookup_cache_size = lookup_cache_sz;
    mdscliopts_.num_virtual_servers = mdstopo_.num_vir_srvs;
    mdscliopts_.num_servers = mdstopo_.num_srvs;
    mdscliopts_.cli_id = cli_id_;
    mdscliopts_.uid = uid_;
    mdscliopts_.gid = gid_;
  }

  if (ok()) {
    mdscli_ = MDSClient::Open(mdscliopts_);
  }
}

Status Client::Open(Client** cliptr) {
  *cliptr = NULL;

  Loader loader;
  loader.LoadEnv();
  loader.LoadMDSTopology();
  loader.LoadMDSCliOptions();
  if (!loader.ok()) {
    return loader.status_;
  }

  Client* cli = new Client;
  assert(loader.mdsfty_ != NULL);
  cli->mdsfty_ = loader.mdsfty_;
  assert(loader.mdscli_ != NULL);
  cli->mdscli_ = loader.mdscli_;

  *cliptr = cli;
  return Status::OK();
}

}  // namespace pdlfs
