/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include "deltafs_client.h"
#include "deltafs_conf_loader.h"
#include "pdlfs-common/blkdb.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/rpc.h"
#include "pdlfs-common/strutil.h"

namespace pdlfs {

static const int kMaxOpenFiles = 1000;

static inline Status BadDescriptor() {
  return Status::InvalidFileDescriptor(Slice());
}

Client::Client() {
  max_open_files_ = kMaxOpenFiles;
  open_files_ = new File*[max_open_files_ + 1];
  num_open_files_ = 0;
  next_file_ = 0;
}

Client::~Client() {
  delete mdscli_;
  delete mdsfty_;
  delete fio_;
}

// REQUIRES: less than "max_open_files_" files have been opened.
// REQUIRES: mutex_ has been locked.
size_t Client::Append(File* file) {
  size_t result = next_file_;
  assert(open_files_[next_file_] == NULL);
  assert(num_open_files_ < max_open_files_);
  open_files_[next_file_] = file;
  num_open_files_++;
  while (open_files_[next_file_++] != NULL) {
    next_file_ %= max_open_files_ + 1;
  }
  return result;
}

// REQUIRES: mutex_ has been locked.
void Client::Remove(size_t index) {
  assert(open_files_[index] != NULL);
  open_files_[index] = NULL;
  assert(num_open_files_ > 0);
  num_open_files_--;
}

// REQUIRES: less than "max_open_files_" files have been opened.
// REQUIRES: mutex_ has been locked.
size_t Client::OpenFile(const Slice& encoding, Fio::Handle* fh) {
  uint32_t hash = Hash(encoding.data(), encoding.size(), 0);
  File* file = file_table_.Lookup(encoding, hash);
  if (file == NULL) {
    file = static_cast<File*>(malloc(sizeof(File) + encoding.size() - 1));
    memcpy(file->key_data, encoding.data(), encoding.size());
    file->key_length = encoding.size();
    file->hash = hash;
    file->seq_write = 0;
    file->seq_flush = 0;
    file->refs = 1;
    file_table_.Insert(file);
    file->fh = fh;
  } else {
    assert(file->fh == fh);
    assert(file->refs > 0);
    file->refs++;
  }
  return Append(file);
}

// REQUIRES: mutex_ has been locked.
Fio::Handle* Client::FetchFileHandle(const Slice& encoding) {
  uint32_t hash = Hash(encoding.data(), encoding.size(), 0);
  File* file = file_table_.Lookup(encoding, hash);
  if (file != NULL) {
    return file->fh;
  } else {
    return NULL;
  }
}

// Return true iff the file has been released.
// REQUIRES: mutex_ has been locked.
bool Client::Unref(File* file) {
  assert(file->refs > 0);
  file->refs--;
  if (file->refs == 0) {
    file_table_.Remove(file->key(), file->hash);
    free(file);
    return true;
  } else {
    return false;
  }
}

// Open a file for I/O operations. Return OK on success.
// If O_CREAT is specified and the file does not exist, it will be created.
// If both O_CREAT and O_EXCL are specified and the file exists,
// error is returned.
// If O_TRUNC is specified and the file already exists and is a regular
// file and the open mode allows writing, it will be truncated to length 0.
// In addition to opening the file, a file descriptor is allocated
// to represent the file. The current length of the file is also returned along
// with the file descriptor.
// Only a fixed amount of file can be opened simultaneously.
// It is also possible that a process may open a same file multiple times
// and obtain multiple file descriptors.
Status Client::Fopen(const Slice& path, int flags, int mode, FileInfo* info) {
  Status s;
  MutexLock ml(&mutex_);
  if (num_open_files_ >= max_open_files_) {
    s = Status::TooManyOpens(Slice());
  } else {
    mutex_.Unlock();
    Fentry fentry;
    uint64_t my_time = Env::Default()->NowMicros();
    if ((flags & O_CREAT) == O_CREAT) {
      const bool error_if_exists = (flags & O_EXCL) == O_EXCL;
      s = mdscli_->Fcreat(path, error_if_exists, mode, &fentry);
    } else {
      s = mdscli_->Fstat(path, &fentry);
    }

    mutex_.Lock();
    char tmp[100];
    Fio::Handle* fh = NULL;
    uint64_t mtime = 0;
    uint64_t size = 0;
    Slice fentry_encoding;
    if (s.ok()) {
      if (num_open_files_ >= max_open_files_) {
        s = Status::TooManyOpens(Slice());
      } else {
        fentry_encoding = fentry.EncodeTo(tmp);
        fh = FetchFileHandle(fentry_encoding);
      }

      if (s.ok()) {
        mutex_.Unlock();
        if (fh != NULL) {
          bool dirty;
          s = fio_->GetInfo(fentry_encoding, fh, &dirty, &mtime, &size);
          fh = NULL;
        } else {
          if (fentry.stat.ChangeTime() >= my_time) {
            // File doesn't exist before so we explicit create it
            s = fio_->Creat(fentry_encoding, &fh);
          } else {
            const bool create_if_missing = true;  // Allow lazy object creation
            const bool truncate_if_exists =
                (flags & O_ACCMODE) != O_RDONLY && (flags & O_TRUNC) == O_TRUNC;
            s = fio_->Open(fentry_encoding, create_if_missing,
                           truncate_if_exists, &mtime, &size, &fh);
          }
#if 0
          if (s.ok()) {
            if (size != fentry.stat.FileSize()) {
              // FIXME
            }
          }
#endif
        }
        mutex_.Lock();
        if (s.ok()) {
          if (num_open_files_ >= max_open_files_) {
            s = Status::TooManyOpens(Slice());
            if (fh != NULL) {
              fio_->Close(fentry_encoding, fh);
            }
          } else {
            info->fd = OpenFile(fentry_encoding, fh);
            info->size = size;
          }
        }
      }
    }
  }
  return s;
}

// REQUIRES: mutex_ has been locked.
Client::File* Client::FetchFile(int fd) {
  size_t index = fd;
  if (index <= max_open_files_) {
    return open_files_[index];
  } else {
    return NULL;
  }
}

Status Client::Pwrite(int fd, const Slice& data, uint64_t off) {
  MutexLock ml(&mutex_);
  File* file = FetchFile(fd);
  if (file == NULL) {
    return BadDescriptor();
  } else {
    assert(file->fh != NULL);
    assert(file->refs > 0);
    mutex_.Unlock();
    Status s = fio_->Pwrite(file->fentry_encoding(), file->fh, data, off);
    mutex_.Lock();
    if (s.ok()) {
      file->seq_write++;
    }
    return s;
  }
}

Status Client::Write(int fd, const Slice& data) {
  MutexLock ml(&mutex_);
  File* file = FetchFile(fd);
  if (file == NULL) {
    return BadDescriptor();
  } else {
    assert(file->fh != NULL);
    assert(file->refs > 0);
    mutex_.Unlock();
    Status s = fio_->Write(file->fentry_encoding(), file->fh, data);
    mutex_.Lock();
    if (s.ok()) {
      file->seq_write++;
    }
    return s;
  }
}

Status Client::Fdatasync(int fd) {
  MutexLock ml(&mutex_);
  File* file = FetchFile(fd);
  if (file == NULL) {
    return BadDescriptor();
  } else {
    assert(file->fh != NULL);
    assert(file->refs > 0);
    uint32_t seq_write = file->seq_write;
    uint32_t seq_flush = file->seq_flush;
    mutex_.Unlock();
    uint64_t mtime;
    uint64_t size;
    bool dirty;
    Status s =
        fio_->GetInfo(file->fentry_encoding(), file->fh, &dirty, &mtime, &size);
    if (s.ok()) {
      const bool force_sync = true;
      s = fio_->Flush(file->fentry_encoding(), file->fh, force_sync);
      if (s.ok()) {
        if (seq_flush < seq_write) {
          Fentry fentry;
          Slice encoding = file->fentry_encoding();
          if (fentry.DecodeFrom(&encoding)) {
            s = mdscli_->Ftruncate(fentry, mtime, size);
          } else {
            s = Status::Corruption(Slice());
          }
        }
      }
    }
    mutex_.Lock();
    if (s.ok()) {
      if (seq_write > file->seq_flush) {
        file->seq_flush = seq_write;
      }
    }
    return s;
  }
}

Status Client::Pread(int fd, Slice* r, uint64_t off, uint64_t size,
                     char* scratch) {
  mutex_.Lock();
  File* file = FetchFile(fd);
  mutex_.Unlock();
  if (file != NULL) {
    assert(file->fh != NULL);
    assert(file->refs > 0);
    return fio_->Pread(file->fentry_encoding(), file->fh, r, off, size,
                       scratch);
  } else {
    return BadDescriptor();
  }
}

Status Client::Read(int fd, Slice* r, uint64_t size, char* scratch) {
  mutex_.Lock();
  File* file = FetchFile(fd);
  mutex_.Unlock();
  if (file != NULL) {
    assert(file->fh != NULL);
    assert(file->refs > 0);
    return fio_->Read(file->fentry_encoding(), file->fh, r, size, scratch);
  } else {
    return BadDescriptor();
  }
}

Status Client::Flush(int fd) {
  MutexLock ml(&mutex_);
  File* file = FetchFile(fd);
  if (file == NULL) {
    return BadDescriptor();
  } else {
    assert(file->fh != NULL);
    assert(file->refs > 0);
    uint32_t seq_write = file->seq_write;
    uint32_t seq_flush = file->seq_flush;
    mutex_.Unlock();
    uint64_t mtime;
    uint64_t size;
    bool dirty;
    Status s =
        fio_->GetInfo(file->fentry_encoding(), file->fh, &dirty, &mtime, &size);
    if (s.ok()) {
      if (dirty) {
        s = fio_->Flush(file->fentry_encoding(), file->fh);
      }
      if (s.ok()) {
        if (seq_flush < seq_write) {
          Fentry fentry;
          Slice encoding = file->fentry_encoding();
          if (fentry.DecodeFrom(&encoding)) {
            s = mdscli_->Ftruncate(fentry, mtime, size);
          } else {
            s = Status::Corruption(Slice());
          }
        }
      }
    }
    mutex_.Lock();
    if (s.ok()) {
      if (seq_write > file->seq_flush) {
        file->seq_flush = seq_write;
      }
    }
    return s;
  }
}

Status Client::Close(int fd) {
  MutexLock ml(&mutex_);
  File* file = FetchFile(fd);
  if (file == NULL) {
    return BadDescriptor();
  } else {
    assert(file->fh != NULL);
    assert(file->refs > 0);
    while (file->seq_flush < file->seq_write) {
      mutex_.Unlock();
      Status s = Flush(fd);
      mutex_.Lock();
      if (!s.ok()) {
        break;
      }
    }
    Fio::Handle* fh = file->fh;
    std::string fentry_encoding = file->fentry_encoding().ToString();
    Remove(fd);
    if (Unref(file)) {
      mutex_.Unlock();
      fio_->Close(fentry_encoding, fh);
      mutex_.Lock();
    }
    return Status::OK();
  }
}

Status Client::Mkfile(const Slice& path, int mode) {
  Status s;
  Fentry ent;
  const bool error_if_exists = true;
  s = mdscli_->Fcreat(path, error_if_exists, mode, &ent);
  return s;
}

Status Client::Mkdir(const Slice& path, int mode) {
  Status s;
  s = mdscli_->Mkdir(path, mode);
  return s;
}

class Client::Builder {
 public:
  explicit Builder()
      : env_(NULL), mdsfty_(NULL), mdscli_(NULL), db_(NULL), blkdb_(NULL) {}
  ~Builder() {}

  Status status() const { return status_; }
  Client* BuildClient();

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

void Client::Builder::LoadIds() {
  uid_ = FetchUid();
  gid_ = FetchGid();

  uint64_t cli_id;
  status_ = config::LoadInstanceId(&cli_id);
  if (ok()) {
    cli_id_ = cli_id;
  }
}

void Client::Builder::LoadMDSTopology() {
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
void Client::Builder::OpenSession() {
  std::string env_name;
  std::string env_conf;

  if (ok()) {
    assert(mdsfty_ != NULL);
    MDS* mds = mdsfty_->Get(cli_id_ % mdstopo_.num_srvs);
    assert(mds != NULL);
    MDS::OpensessionOptions options;
    options.dir_id = DirId(0, 0, 0);
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
void Client::Builder::OpenDB() {
  std::string output_root;

  if (ok()) {
    assert(mdsfty_ != NULL);
    MDS* mds = mdsfty_->Get(session_id_ % mdstopo_.num_srvs);
    assert(mds != NULL);
    MDS::GetoutputOptions options;
    options.dir_id = DirId(0, 0, 0);
    MDS::GetoutputRet ret;
    status_ = mds->Getoutput(options, &ret);
    if (ok()) {
      output_root = ret.info;
    }
  }

  if (ok()) {
    status_ = config::LoadVerifyChecksums(&blkdbopts_.verify_checksum);
  }

  if (ok()) {
    dbopts_.create_if_missing = true;
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
      blkdbopts_.uniquefier = session_id_;
      blkdbopts_.owns_db = true;
      blkdb_ = new BlkDB(blkdbopts_);
      db_ = NULL;
    }
  }
}

// REQUIRES: OpenSession() has been called.
void Client::Builder::OpenMDSCli() {
  uint64_t idx_cache_sz;
  uint64_t lookup_cache_sz;

  if (ok()) {
    status_ = config::LoadSizeOfCliIndexCache(&idx_cache_sz);
    if (ok()) {
      status_ = config::LoadSizeOfCliLookupCache(&lookup_cache_sz);
    }
  }

  if (ok()) {
    status_ = config::LoadAtomicPathRes(&mdscliopts_.atomic_path_resolution);
    if (ok()) {
      status_ = config::LoadParanoidChecks(&mdscliopts_.paranoid_checks);
    }
  }

  if (ok()) {
    mdscliopts_.env = env_;
    mdscliopts_.factory = mdsfty_;
    mdscliopts_.index_cache_size = idx_cache_sz;
    mdscliopts_.lookup_cache_size = lookup_cache_sz;
    mdscliopts_.num_virtual_servers = mdstopo_.num_vir_srvs;
    mdscliopts_.num_servers = mdstopo_.num_srvs;
    mdscliopts_.session_id = session_id_;
    mdscliopts_.cli_id = cli_id_;
    mdscliopts_.uid = uid_;
    mdscliopts_.gid = gid_;
  }

  if (ok()) {
    mdscli_ = MDSClient::Open(mdscliopts_);
  }
}

Client* Client::Builder::BuildClient() {
  LoadIds();
  LoadMDSTopology();
  OpenSession();
  OpenDB();
  OpenMDSCli();

  if (ok()) {
    Client* cli = new Client;
    cli->mdscli_ = mdscli_;
    cli->mdsfty_ = mdsfty_;
    cli->fio_ = blkdb_;
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
  Builder builder;
  *cliptr = builder.BuildClient();
  return builder.status();
}

}  // namespace pdlfs
