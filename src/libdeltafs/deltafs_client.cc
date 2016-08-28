/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "deltafs_client.h"
#include "deltafs_conf_loader.h"
#include "pdlfs-common/blkdb.h"
#include "pdlfs-common/lazy_env.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/rpc.h"
#include "pdlfs-common/strutil.h"

namespace pdlfs {

static const int kMaxOpenFileDescriptors = 1000;

static inline Status BadDescriptor() {
  return Status::InvalidFileDescriptor(Slice());
}

static inline Status PermissionDenied() {
  return Status::AccessDenied(Slice());
}

Client::Client() {
  dummy_.prev = &dummy_;
  dummy_.next = &dummy_;
  max_open_fds_ = kMaxOpenFileDescriptors;
  fds_ = new File*[max_open_fds_]();
  num_open_fds_ = 0;
  fd_cursor_ = 0;
}

Client::~Client() {
  delete[] fds_;
  delete mdscli_;
  delete mdsfty_;
  delete fio_;
}

// REQUIRES: less than "max_open_files_" files have been opened.
// REQUIRES: mutex_ has been locked.
size_t Client::Alloc(File* f) {
  assert(num_open_fds_ < max_open_fds_);
  while (fds_[fd_cursor_] != NULL) {
    fd_cursor_ = (1 + fd_cursor_) % max_open_fds_;
  }
  fds_[fd_cursor_] = f;
  num_open_fds_++;
  return fd_cursor_;
}

// REQUIRES: mutex_ has been locked.
Client::File* Client::Free(size_t index) {
  File* f = fds_[index];
  assert(f != NULL);
  fds_[index] = NULL;
  assert(num_open_fds_ > 0);
  num_open_fds_--;
  return f;
}

// REQUIRES: less than "max_open_files_" files have been opened.
// REQUIRES: mutex_ has been locked.
size_t Client::Open(const Slice& encoding, int flags, const Stat& stat,
                    Fio::Handle* fh) {
  File* file = static_cast<File*>(malloc(sizeof(File) + encoding.size() - 1));
  memcpy(file->encoding_data, encoding.data(), encoding.size());
  file->encoding_length = encoding.size();
  file->mode = stat.FileMode();
  file->gid = stat.GroupId();
  file->uid = stat.UserId();
  file->next = &dummy_;
  file->prev = dummy_.prev;
  file->prev->next = file;
  file->next->prev = file;
  file->seq_write = 0;
  file->seq_flush = 0;
  file->flags = flags;
  file->refs = 1;
  file->fh = fh;
  return Alloc(file);
}

// REQUIRES: mutex_ has been locked.
void Client::Unref(File* f) {
  assert(f->refs > 0);
  f->refs--;
  if (f->refs == 0) {
    f->next->prev = f->prev;
    f->prev->next = f->next;
    mutex_.Unlock();
    fio_->Close(f->fentry_encoding(), f->fh);
    free(f);
    mutex_.Lock();
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
  if (num_open_fds_ >= max_open_fds_) {
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

#if VERBOSE >= 10
    if (s.ok()) {
      Verbose(__LOG_ARGS__, 10, "deltafs_open: %s -> [%llu:%llu:%llu]",
              path.c_str(), (unsigned long long)fentry.stat.RegId(),
              (unsigned long long)fentry.stat.SnapId(),
              (unsigned long long)fentry.stat.InodeNo());
    }
#endif

    Fio::Handle* fh = NULL;
    uint64_t mtime = fentry.stat.ModifyTime();
    uint64_t size = fentry.stat.FileSize();
    Slice fentry_encoding;
    char tmp[100];
    if (s.ok()) {
      fentry_encoding = fentry.EncodeTo(tmp);
      if (fentry.stat.ChangeTime() >= my_time) {
        // File doesn't exist before so we explicit create it
        s = fio_->Creat(fentry_encoding, &fh);
      } else {
        const bool create_if_missing = true;  // Allow lazy object creation
        const bool truncate_if_exists =
            (flags & O_ACCMODE) != O_RDONLY && (flags & O_TRUNC) == O_TRUNC;
        s = fio_->Open(fentry_encoding, create_if_missing, truncate_if_exists,
                       &mtime, &size, &fh);
      }
    }
    mutex_.Lock();
    if (s.ok()) {
      if (num_open_fds_ >= max_open_fds_) {
        s = Status::TooManyOpens(Slice());
        if (fh != NULL) {
          fio_->Close(fentry_encoding, fh);
        }
      } else {
        fentry.stat.SetFileSize(size);
        fentry.stat.SetModifyTime(mtime);
        info->fd = Open(fentry_encoding, flags, fentry.stat, fh);
        info->stat = fentry.stat;
      }
    }
  }
  return s;
}

bool Client::IsReadOk(const File* f) {
  if ((f->flags & O_ACCMODE) == O_WRONLY) {
    return false;
  } else if (mdscli_->uid() == 0) {
    return true;
  } else if (mdscli_->uid() == f->uid && (f->mode & S_IRUSR) == S_IRUSR) {
    return true;
  } else if (mdscli_->gid() == f->gid && (f->mode & S_IRGRP) == S_IRGRP) {
    return true;
  } else if ((f->mode & S_IROTH) == S_IROTH) {
    return true;
  } else {
    return false;
  }
}

bool Client::IsWriteOk(const File* f) {
  if ((f->flags & O_ACCMODE) == O_RDONLY) {
    return false;
  } else if (mdscli_->uid() == 0) {
    return true;
  } else if (mdscli_->uid() == f->uid && (f->mode & S_IWUSR) == S_IWUSR) {
    return true;
  } else if (mdscli_->gid() == f->gid && (f->mode & S_IWGRP) == S_IWGRP) {
    return true;
  } else if ((f->mode & S_IWOTH) == S_IWOTH) {
    return true;
  } else {
    return false;
  }
}

// REQUIRES: mutex_ has been locked.
Client::File* Client::FetchFile(int fd) {
  size_t index = fd;
  if (index < max_open_fds_) {
    return fds_[index];
  } else {
    return NULL;
  }
}

Status Client::Fstat(int fd, Stat* statbuf) {
  MutexLock ml(&mutex_);
  File* file = FetchFile(fd);
  if (file == NULL) {
    return BadDescriptor();
  } else {
    Status s;
    Fentry ent;
    Slice encoding = file->fentry_encoding();
    if (!ent.DecodeFrom(&encoding)) {
      s = Status::Corruption(Slice());
    } else {
      ent.stat.SetFileMode(file->mode);
      ent.stat.SetGroupId(file->gid);
      ent.stat.SetUserId(file->uid);
      assert(file->fh != NULL);
      file->refs++;
      mutex_.Unlock();
      uint64_t mtime;
      uint64_t size;
      s = fio_->Stat(file->fentry_encoding(), file->fh, &mtime, &size);
      mutex_.Lock();
      if (s.ok()) {
        ent.stat.SetModifyTime(mtime);
        ent.stat.SetFileSize(size);
        *statbuf = ent.stat;
      }
      Unref(file);
    }
    return s;
  }
}

Status Client::Pwrite(int fd, const Slice& data, uint64_t off) {
  MutexLock ml(&mutex_);
  File* file = FetchFile(fd);
  if (file == NULL) {
    return BadDescriptor();
  } else if (!IsWriteOk(file)) {
    return PermissionDenied();
  } else {
    Status s;
    assert(file->fh != NULL);
    file->refs++;
    mutex_.Unlock();
    s = fio_->Pwrite(file->fentry_encoding(), file->fh, data, off);
    mutex_.Lock();
    if (s.ok()) {
      file->seq_write++;
    }
    Unref(file);
    return s;
  }
}

Status Client::Write(int fd, const Slice& data) {
  MutexLock ml(&mutex_);
  File* file = FetchFile(fd);
  if (file == NULL) {
    return BadDescriptor();
  } else if (!IsWriteOk(file)) {
    return PermissionDenied();
  } else {
    Status s;
    assert(file->fh != NULL);
    file->refs++;
    mutex_.Unlock();
    s = fio_->Write(file->fentry_encoding(), file->fh, data);
    mutex_.Lock();
    if (s.ok()) {
      file->seq_write++;
    }
    Unref(file);
    return s;
  }
}

Status Client::Ftruncate(int fd, uint64_t len) {
  MutexLock ml(&mutex_);
  File* file = FetchFile(fd);
  if (file == NULL) {
    return BadDescriptor();
  } else if (!IsWriteOk(file)) {
    return PermissionDenied();
  } else {
    Status s;
    assert(file->fh != NULL);
    file->refs++;
    mutex_.Unlock();
    s = fio_->Truncate(file->fentry_encoding(), file->fh, len);
    mutex_.Lock();
    if (s.ok()) {
      file->seq_write++;
    }
    Unref(file);
    return s;
  }
}

Status Client::Fdatasync(int fd) {
  Status s;
  MutexLock ml(&mutex_);
  File* file = FetchFile(fd);
  if (file == NULL) {
    s = BadDescriptor();
  } else if ((file->flags & O_ACCMODE) != O_RDONLY) {
    assert(file->fh != NULL);
    uint32_t seq_write = file->seq_write;
    uint32_t seq_flush = file->seq_flush;
    file->refs++;
    mutex_.Unlock();
    const bool force_sync = true;
    s = fio_->Flush(file->fentry_encoding(), file->fh, force_sync);
    if (s.ok() && seq_flush < seq_write) {
      uint64_t mtime;
      uint64_t size;
      const bool skip_cache = true;
      s = fio_->Stat(file->fentry_encoding(), file->fh, &mtime, &size,
                     skip_cache);
      if (s.ok()) {
        Fentry fentry;
        Slice encoding = file->fentry_encoding();
        if (fentry.DecodeFrom(&encoding)) {
          s = mdscli_->Ftruncate(fentry, mtime, size);
        } else {
          s = Status::Corruption(Slice());
        }
      }
    }
    mutex_.Lock();
    if (s.ok()) {
      if (seq_write > file->seq_flush) {
        file->seq_flush = seq_write;
      }
    }
    Unref(file);
  }
  return s;
}

Status Client::Pread(int fd, Slice* result, uint64_t off, uint64_t size,
                     char* scratch) {
  MutexLock ml(&mutex_);
  File* file = FetchFile(fd);
  if (file == NULL) {
    return BadDescriptor();
  } else if (!IsReadOk(file)) {
    return PermissionDenied();
  } else {
    Status s;
    assert(file->fh != NULL);
    file->refs++;
    mutex_.Unlock();
    Slice e = file->fentry_encoding();
    s = fio_->Pread(e, file->fh, result, off, size, scratch);
    mutex_.Lock();
    Unref(file);
    return s;
  }
}

Status Client::Read(int fd, Slice* result, uint64_t size, char* scratch) {
  MutexLock ml(&mutex_);
  File* file = FetchFile(fd);
  if (file == NULL) {
    return BadDescriptor();
  } else if (!IsReadOk(file)) {
    return PermissionDenied();
  } else {
    Status s;
    assert(file->fh != NULL);
    file->refs++;
    mutex_.Unlock();
    Slice e = file->fentry_encoding();
    s = fio_->Read(e, file->fh, result, size, scratch);
    mutex_.Lock();
    Unref(file);
    return s;
  }
}

Status Client::Flush(int fd) {
  Status s;
  MutexLock ml(&mutex_);
  File* file = FetchFile(fd);
  if (file == NULL) {
    s = BadDescriptor();
  } else if ((file->flags & O_ACCMODE) != O_RDONLY) {
    assert(file->fh != NULL);
    uint32_t seq_write = file->seq_write;
    uint32_t seq_flush = file->seq_flush;
    file->refs++;
    mutex_.Unlock();
    s = fio_->Flush(file->fentry_encoding(), file->fh);
    if (s.ok() && seq_flush < seq_write) {
      uint64_t mtime;
      uint64_t size;
      s = fio_->Stat(file->fentry_encoding(), file->fh, &mtime, &size);
      if (s.ok()) {
        Fentry fentry;
        Slice encoding = file->fentry_encoding();
        if (fentry.DecodeFrom(&encoding)) {
          s = mdscli_->Ftruncate(fentry, mtime, size);
        } else {
          s = Status::Corruption(Slice());
        }
      }
    }
    mutex_.Lock();
    if (s.ok()) {
      if (seq_write > file->seq_flush) {
        file->seq_flush = seq_write;
      }
    }
    Unref(file);
  }
  return s;
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
    Free(fd);
    Unref(file);
    return Status::OK();
  }
}

Status Client::Getattr(const Slice& path, Stat* statbuf) {
  Status s;
  Fentry ent;
  s = mdscli_->Fstat(path, &ent);
  if (s.ok()) {
    *statbuf = ent.stat;
  }
  return s;
}

Status Client::Mkfile(const Slice& path, int mode) {
  Status s;
  const bool error_if_exists = true;
  s = mdscli_->Fcreat(path, error_if_exists, mode, NULL);
  return s;
}

Status Client::Mkdir(const Slice& path, int mode) {
  Status s;
  s = mdscli_->Mkdir(path, mode, NULL);
  return s;
}

Status Client::Chmod(const Slice& path, int mode) {
  Status s;
  s = mdscli_->Chmod(path, mode, NULL);
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
  Fio* fio_;
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
          status_ = Status::InvalidArgument("not enough addrs");
        } else if (num_addrs > num_srvs) {
          status_ = Status::InvalidArgument("too many addrs");
        }
      }
    }
  }

  if (ok()) {
    status_ = config::LoadMDSTracing(&mdstopo_.mds_tracing);
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
      env_ = new LazyEnv(ret.env_name, ret.env_conf);
      fio_ = Fio::Open(ret.fio_name, ret.fio_conf);
      if (fio_ == NULL) {
        status_ = Status::IOError("cannot open fio");
      }
    }
  }
}

// REQUIRES: OpenSession() has been called.
void Client::Builder::OpenDB() {
  blkdb_ = NULL;
  db_ = NULL;
#if 0
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
#endif
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
    cli->fio_ = fio_;
    return cli;
  } else {
    delete mdscli_;
    delete mdsfty_;
    delete fio_;
    delete blkdb_;
    delete db_;
    delete env_;
    return NULL;
  }
}

Status Client::Open(Client** cliptr) {
  Builder builder;
  *cliptr = builder.BuildClient();
  return builder.status();
}

}  // namespace pdlfs
