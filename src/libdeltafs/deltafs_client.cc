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
  explicit Loader()
      : env_(NULL), mdsfty_(NULL), mdscli_(NULL), db_(NULL), blkdb_(NULL) {}
  ~Loader() {}

  Status status() const { return status_; }
  Client* OpenClient();

 private:
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

  void LoadIds();
  void LoadMDSTopology();
  void OpenSession();
  void OpenDB();
  void OpenMDSCli();

  Status status_;
  bool ok() const { return status_.ok(); }
  Env* env_;
  MDSTopology mdstopo_;
  MDSFactoryImpl* mdsfty_;
  MDSCliOptions mdscliopts_;
  MDSClient* mdscli_;
  DBOptions dbopts_;
  DB* db_;
  BlkDBOptions blkdbopts_;
  BlkDB* blkdb_;
  int cli_id_;
  int session_id_;
  int uid_;
  int gid_;
};

#define DEF_LOADER_UI64(conf)                            \
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

DEF_LOADER_UI64(InstanceId)
DEF_LOADER_UI64(NumOfVirMetadataSrvs)
DEF_LOADER_UI64(NumOfMetadataSrvs)
DEF_LOADER_UI64(SizeOfCliLookupCache)
DEF_LOADER_UI64(SizeOfCliIndexCache)

DEF_LOADER_BOOL(AtomicPathResolution)
DEF_LOADER_BOOL(ParanoidChecks)
DEF_LOADER_BOOL(VerifyChecksums)
DEF_LOADER_BOOL(RPCTracing)

void Client::Loader::LoadIds() {
  uid_ = FetchUid();
  gid_ = FetchGid();

  uint64_t cli_id;
  status_ = LoadInstanceId(&cli_id);
  if (ok()) {
    cli_id_ = cli_id;
  }
}

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
    MDSFactoryImpl* fty = new MDSFactoryImpl;
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

// REQUIRES: both LoadIds() and LoadMDSTopology() have been called.
void Client::Loader::OpenSession() {
  std::string env_name;
  std::string env_conf;

  if (ok()) {
    assert(mdsfty_ != NULL);
    MDS* mds = mdsfty_->Get(cli_id_ % mdstopo_.num_srvs);
    assert(mds != NULL);
    MDS::OpensessionOptions options;
    MDS::OpensessionRet ret;
    status_ = mds->Opensession(options, &ret);
    if (ok()) {
      session_id_ = ret.session_id;
      env_name = ret.env_name;
      env_conf = ret.env_conf;
    }
  }

  if (ok()) {
    env_ = Env::Default();  // FIXME
  }
}

// REQUIRES: OpenSession() has been called.
void Client::Loader::OpenDB() {
  std::string output_root;

  if (ok()) {
    assert(mdsfty_ != NULL);
    MDS* mds = mdsfty_->Get(session_id_ % mdstopo_.num_srvs);
    assert(mds != NULL);
    MDS::GetoutputOptions options;
    MDS::GetoutputRet ret;
    status_ = mds->Getoutput(options, &ret);
    if (ok()) {
      output_root = ret.info;
    }
  }

  if (ok()) {
    status_ = LoadVerifyChecksums(&blkdbopts_.verify_checksum);
  }

  if (ok()) {
    dbopts_.compression = kNoCompression;
    dbopts_.disable_compaction = true;
    dbopts_.env = env_;
  }

  if (ok()) {
    std::string dbhome = output_root;
    char tmp[30];
    snprintf(tmp, sizeof(tmp), "/data_%d", session_id_);
    dbhome += tmp;
    status_ = DB::Open(dbopts_, dbhome, &db_);
    if (ok()) {
      blkdbopts_.db = db_;
      blkdb_ = new BlkDB(blkdbopts_);
    }
  }
}

// REQUIRES: OpenSession() has been called.
void Client::Loader::OpenMDSCli() {
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
    mdscliopts_.cli_id = session_id_;
    mdscliopts_.uid = uid_;
    mdscliopts_.gid = gid_;
  }

  if (ok()) {
    mdscli_ = MDSClient::Open(mdscliopts_);
  }
}

Client* Client::Loader::OpenClient() {
  LoadIds();
  LoadMDSTopology();
  OpenSession();
  OpenDB();
  OpenMDSCli();

  if (ok()) {
    Client* cli = new Client;
    cli->mdscli_ = mdscli_;
    cli->mdsfty_ = mdsfty_;
    cli->blk_db_ = blkdb_;
    cli->db_ = db_;
    return cli;
  } else {
    delete mdscli_;
    delete mdsfty_;
    delete blkdb_;
    delete db_;
    return NULL;
  }
}

Status Client::Open(Client** cliptr) {
  Loader ld;
  *cliptr = ld.OpenClient();
  return ld.status();
}

}  // namespace pdlfs
