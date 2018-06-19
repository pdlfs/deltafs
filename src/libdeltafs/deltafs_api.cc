/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs/deltafs_api.h"
#include "deltafs/deltafs_config.h"

#include "deltafs_client.h"
#include "deltafs_envs.h"
#include "deltafs_plfsio.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/dbfiles.h"
#include "pdlfs-common/env_files.h"
#include "pdlfs-common/logging.h"
#include "pdlfs-common/murmur.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/spooky.h"
#include "pdlfs-common/status.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>

#include <string>

#ifndef EHOSTUNREACH
#define EHOSTUNREACH ENODEV
#endif

#ifndef ENOSYS
#define ENOSYS EPERM
#endif

#ifndef ENOBUFS
#define ENOBUFS ENOMEM
#endif

static void SetErrno(const pdlfs::Status& s);
static pdlfs::Status bg_status;
static pdlfs::port::OnceType once = PDLFS_ONCE_INIT;
static pdlfs::Client* client = NULL;
static char* MakeChar(uint64_t i) {
  char tmp[20];
  snprintf(tmp, sizeof(tmp), "%llu", static_cast<unsigned long long>(i));
  return strdup(tmp);
}
static inline int NoClient() {
  if (!bg_status.ok()) {
    SetErrno(bg_status);
  } else {
    errno = ENODEV;
  }
  return -1;
}

static void InitClient() {
  if (client == NULL) {
    pdlfs::Status s = pdlfs::Client::Open(&client);
    if (!s.ok()) {
      Error(__LOG_ARGS__, "Fail to open deltafs client: %s",
            s.ToString().c_str());
      bg_status = s;
      client = NULL;
    } else {
#if VERBOSE >= 2
      Verbose(__LOG_ARGS__, 2, "Deltafs ready");
#endif
    }
  }
}

static pdlfs::Status BadArgs() {
  return pdlfs::Status::InvalidArgument(pdlfs::Slice());
}

static void SetErrno(const pdlfs::Status& s) {
  if (s.ok()) {
    errno = 0;
  } else if (s.IsNotFound()) {
    errno = ENOENT;
  } else if (s.IsAlreadyExists()) {
    errno = EEXIST;
  } else if (s.IsFileExpected()) {
    errno = EISDIR;
  } else if (s.IsDirExpected()) {
    errno = ENOTDIR;
  } else if (s.IsInvalidFileDescriptor()) {
    errno = EBADF;
  } else if (s.IsTooManyOpens()) {
    errno = EMFILE;
  } else if (s.IsAccessDenied()) {
    errno = EACCES;
  } else if (s.IsAssertionFailed()) {
    errno = EPERM;
  } else if (s.IsReadOnly()) {
    errno = EROFS;
  } else if (s.IsNotSupported()) {
    errno = ENOSYS;
  } else if (s.IsInvalidArgument()) {
    errno = EINVAL;
  } else if (s.IsDisconnected()) {
    errno = EPIPE;
  } else if (s.IsBufferFull()) {
    errno = ENOBUFS;
  } else if (s.IsRange()) {
    errno = ERANGE;
  } else {
    errno = EIO;
  }
}

extern "C" {
char* deltafs_getcwd(char* __buf, size_t __sz) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      NoClient();
      return NULL;
    }
  }
  pdlfs::Status s;
  s = client->Getcwd(__buf, __sz);
  if (s.ok()) {
    return __buf;
  } else {
    SetErrno(s);
    return NULL;
  }
}

int deltafs_chroot(const char* __path) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }
  pdlfs::Status s;
  s = client->Chroot(__path);
  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_chdir(const char* __path) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }
  pdlfs::Status s;
  s = client->Chdir(__path);
  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

mode_t deltafs_umask(mode_t __mode) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      NoClient();
      return 0;
    }
  }

  return client->Umask(__mode);
}

int deltafs_openat(int fd, const char* __path, int __oflags, mode_t __mode) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::FileInfo info;
  pdlfs::Status s;
  s = client->Fopenat(fd, __path, __oflags, __mode, &info);
  if (s.ok()) {
    return info.fd;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_openstat(const char* __path, int __oflags, mode_t __mode,
                     struct stat* __buf) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::FileInfo info;
  pdlfs::Status s;
  s = client->Fopen(__path, __oflags, __mode, &info);
  if (s.ok()) {
    if (__buf != NULL) {
      pdlfs::__cpstat(info.stat, __buf);
    }
    return info.fd;
  } else {
    SetErrno(s);
    return -1;
  }
}

ssize_t deltafs_pread(int __fd, void* __buf, size_t __sz, off_t __off) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::Slice result;
  pdlfs::Status s;
  s = client->Pread(__fd, &result, __off, __sz, (char*)__buf);
  if (s.ok()) {
    return result.size();
  } else {
    SetErrno(s);
    return -1;
  }
}

ssize_t deltafs_read(int __fd, void* __buf, size_t __sz) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::Slice result;
  pdlfs::Status s;
  s = client->Read(__fd, &result, __sz, (char*)__buf);
  if (s.ok()) {
    return result.size();
  } else {
    SetErrno(s);
    return -1;
  }
}

ssize_t deltafs_pwrite(int __fd, const void* __buf, size_t __sz, off_t __off) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::Status s;
  s = client->Pwrite(__fd, pdlfs::Slice((const char*)__buf, __sz), __off);
  if (s.ok()) {
    return __sz;
  } else {
    SetErrno(s);
    return -1;
  }
}

ssize_t deltafs_write(int __fd, const void* __buf, size_t __sz) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::Status s;
  s = client->Write(__fd, pdlfs::Slice((const char*)__buf, __sz));
  if (s.ok()) {
    return __sz;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_fstat(int __fd, struct stat* __buf) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::Stat stat;
  pdlfs::Status s;
  s = client->Fstat(__fd, &stat);
  if (s.ok()) {
    pdlfs::__cpstat(stat, __buf);
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_ftruncate(int __fd, off_t __len) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::Status s;
  s = client->Ftruncate(__fd, __len);
  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_fdatasync(int __fd) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::Status s;
  s = client->Fdatasync(__fd);
  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_epoch_flush(int __fd, void* __arg) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::Status s;
  s = client->Flush(__fd);
  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_close(int __fd) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
  }
  if (client == NULL) {
    return NoClient();
  } else {
    client->Close(__fd);
    return 0;
  }
}

int deltafs_access(const char* __path, int __mode) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }
  pdlfs::Status s;
  s = client->Access(__path, __mode);
  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_accessdir(const char* __path, int __mode) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }
  pdlfs::Status s;
  s = client->Accessdir(__path, __mode);
  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_listdir(const char* __path, deltafs_filler_t __filler,
                    void* __arg) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }
  std::vector<std::string> names;
  pdlfs::Status s;
  s = client->Listdir(__path, &names);
  if (s.ok()) {
    for (std::vector<std::string>::iterator iter = names.begin();
         iter != names.end(); ++iter) {
      if (__filler(iter->c_str(), __arg) != 0) {
        break;
      }
    }
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_getattr(const char* __path, struct stat* __buf) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::Stat stat;
  pdlfs::Status s;
  s = client->Getattr(__path, &stat);
  if (s.ok()) {
    pdlfs::__cpstat(stat, __buf);
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_stat(const char* __path, struct stat* __buf) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::Stat stat;
  pdlfs::Status s;
  s = client->Lstat(__path, &stat);
  if (s.ok()) {
    pdlfs::__cpstat(stat, __buf);
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_mkfile(const char* __path, mode_t __mode) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }
  pdlfs::Status s;
  s = client->Mkfile(__path, __mode);
  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_mkdirs(const char* __path, mode_t __mode) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }
  pdlfs::Status s;
  s = client->Mkdirs(__path, __mode);
  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_mkdir(const char* __path, mode_t __mode) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }
  pdlfs::Status s;
  s = client->Mkdir(__path, __mode);
  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_chmod(const char* __path, mode_t __mode) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::Status s;
  s = client->Chmod(__path, __mode);
  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_chown(const char* __path, uid_t __usr, gid_t __grp) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::Status s;
  s = client->Chown(__path, __usr, __grp);
  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_unlink(const char* __path) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::Status s;
  s = client->Unlink(__path);
  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_truncate(const char* __path, off_t __len) {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  pdlfs::Status s;
  s = client->Truncate(__path, __len);
  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

// XXX: Not inlined so it has a name in *.so which can be dlopened by others
int deltafs_creat(const char* __path, mode_t __mode) {
  return deltafs_open(__path, O_CREAT | O_WRONLY | O_TRUNC, __mode);
}

int deltafs_open(const char* __path, int __oflags, mode_t __mode) {
  return deltafs_openstat(__path, __oflags, __mode, NULL);
}

int deltafs_nonop() {
  if (client == NULL) {
    pdlfs::port::InitOnce(&once, InitClient);
    if (client == NULL) {
      return NoClient();
    }
  }

  return 0;
}

void deltafs_print_sysinfo() {
  // Print to system logger, usually stderr or glog
  pdlfs::PrintSysInfo();
}

// -------------
// Version query
// -------------
int deltafs_version_major() { return DELTAFS_VERSION_MAJOR; }
int deltafs_version_minor() { return DELTAFS_VERSION_MINOR; }
int deltafs_version_patch() { return DELTAFS_VERSION_PATCH; }

}  // extern C

// -------------------------
// Light-weight plfsdir api
// -------------------------
namespace {

// Dir options
typedef pdlfs::plfsio::DirOptions DirOptions;
#define PLFSIO_HASH_USE_SPOOKY  // Any prefix of a hash is still a hash

// Dir writer
typedef pdlfs::plfsio::DirWriter DirWriter;
// Dir Reader
typedef pdlfs::plfsio::DirReader DirReader;

// Dir mode
typedef pdlfs::plfsio::DirMode DirMode;

// Default dir mode
inline DirMode DefaultDirMode() {  // Assuming unique keys
  return pdlfs::plfsio::kDmUniqueKey;
}

// Default system env.
inline pdlfs::Env* DefaultDirEnv() {
#if defined(PDLFS_PLATFORM_POSIX)
  return pdlfs::port::PosixGetUnBufferedIOEnv();  // Avoid double-buffering
#else
  return pdlfs::Env::Default();
#endif
}

// Default thread pool impl.
inline pdlfs::ThreadPool* CreateThreadPool(int num_threads) {
  return pdlfs::ThreadPool::NewFixed(num_threads, true);
}

inline DirOptions ParseOptions(const char* conf) {
  if (conf != NULL) {
    return pdlfs::plfsio::ParseDirOptions(conf);
  } else {
    return DirOptions();
  }
}

}  // namespace

extern "C" {
struct deltafs_env {
  pdlfs::Env* env;
  // True iff the env is backed by a posix file system
  bool is_pfs;
  // True iff env belongs to the system
  // and should not be deleted
  bool sys;
};

deltafs_env_t* deltafs_env_init(int __argc, void** __argv) {
  pdlfs::Env* env;
  bool sys;
  env = pdlfs::TryOpenEnv(__argc, __argv, &sys);

  if (env != NULL) {
    deltafs_env_t* result =
        static_cast<deltafs_env_t*>(malloc(sizeof(deltafs_env_t)));
    result->is_pfs = true;  // FIXME
    result->sys = sys;
    result->env = env;
    return result;
  } else {
    SetErrno(BadArgs());
    return NULL;
  }
}

int deltafs_env_is_system(deltafs_env_t* __env) {
  if (__env != NULL) {
    return int(__env->sys);
  } else {
    return 0;
  }
}

int deltafs_env_close(deltafs_env_t* __env) {
  if (__env != NULL) {
    if (!__env->sys) {
      delete __env->env;
    }
    free(__env);
    return 0;
  } else {
    return 0;
  }
}

struct deltafs_tp {
  pdlfs::ThreadPool* pool;
};

deltafs_tp_t* deltafs_tp_init(int __size) {
  pdlfs::ThreadPool* pool = CreateThreadPool(__size);
  if (pool != NULL) {
    deltafs_tp_t* result =
        static_cast<deltafs_tp_t*>(malloc(sizeof(deltafs_tp_t)));
    result->pool = pool;
    return result;
  } else {
    SetErrno(BadArgs());
    return NULL;
  }
}

int deltafs_tp_pause(deltafs_tp_t* __tp) {
  if (__tp != NULL) {
    pdlfs::ThreadPool* pool = __tp->pool;
    pool->Pause();
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_tp_rerun(deltafs_tp_t* __tp) {
  if (__tp != NULL) {
    pdlfs::ThreadPool* pool = __tp->pool;
    pool->Resume();
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_tp_close(deltafs_tp_t* __tp) {
  if (__tp != NULL) {
    if (__tp->pool != NULL) {
      delete __tp->pool;
    }
    free(__tp);
    return 0;
  } else {
    return 0;
  }
}

}  // extern C

namespace {
class DirEnvWrapper : public pdlfs::EnvWrapper {
  static uint64_t GetNumFromEnv(const char* name) {
    const char* value = getenv(name);
    if (value && value[0]) {
      return strtoull(value, NULL, 10);
    }
    return 0;
  }

  static bool IsEnvSet(const char* name) {
    const char* value = getenv(name);
    if (value && value[0]) {
      return strcmp(value, "0") != 0;
    }
    return false;
  }

 public:
  explicit DirEnvWrapper(Env* base) : EnvWrapper(base) {
    rate_ = GetNumFromEnv("DELTAFS_TC_RATE");  // Traffic control
    sync_on_close_ = IsEnvSet("DELTAFS_TC_SYNCONCLOSE");
    ignore_sync_ = IsEnvSet("DELTAFS_TC_IGNORESYNC");
    drop_ = IsEnvSet("DELTAFS_TC_DROPDATA");
  }

  template <typename T>
  static void CleanUpRepo(std::vector<T*>* v) {
    typename std::vector<T*>::iterator it = v->begin();
    for (; it != v->end(); ++it) {
      delete *it;
    }
  }

  virtual ~DirEnvWrapper() {
    CleanUpRepo(&sequentialfile_repo_);
    CleanUpRepo(&randomaccessfile_repo_);
    CleanUpRepo(&writablefile_repo_);
  }

  virtual pdlfs::Status NewSequentialFile(const char* f,
                                          pdlfs::SequentialFile** r);
  virtual pdlfs::Status NewRandomAccessFile(const char* f,
                                            pdlfs::RandomAccessFile** r);
  virtual pdlfs::Status NewWritableFile(const char* f, pdlfs::WritableFile** r);

  template <typename T>
  static uint64_t SumUpOps(const std::vector<T*>* v) {
    uint64_t result = 0;
    typename std::vector<T*>::const_iterator it = v->begin();
    for (; it != v->end(); ++it) {
      result += (*it)->TotalOps();
    }
    return result;
  }

  uint64_t TotalRandomSeeks() const {
    pdlfs::MutexLock l(&mu_);
    return SumUpOps(&randomaccessfile_repo_);
  }

  template <typename T>
  static uint64_t SumUpBytes(const std::vector<T*>* v) {
    uint64_t result = 0;
    typename std::vector<T*>::const_iterator it = v->begin();
    for (; it != v->end(); ++it) {
      result += (*it)->TotalBytes();
    }
    return result;
  }

  size_t TotalFilesOpenedForRead() const {
    pdlfs::MutexLock l(&mu_);
    return sequentialfile_repo_.size() + randomaccessfile_repo_.size();
  }

  uint64_t TotalBytesRead() const {
    pdlfs::MutexLock l(&mu_);
    uint64_t result = 0;
    result += SumUpBytes(&sequentialfile_repo_);
    result += SumUpBytes(&randomaccessfile_repo_);
    return result;
  }

  size_t TotalFilesOpenedForWrite() const {
    pdlfs::MutexLock l(&mu_);
    return writablefile_repo_.size();
  }

  uint64_t TotalBytesWritten() const {
    pdlfs::MutexLock l(&mu_);
    return SumUpBytes(&writablefile_repo_);
  }

 private:
  std::vector<pdlfs::SequentialFileStats*> sequentialfile_repo_;
  std::vector<pdlfs::RandomAccessFileStats*> randomaccessfile_repo_;
  std::vector<pdlfs::WritableFileStats*> writablefile_repo_;
  mutable pdlfs::port::Mutex mu_;

  // No copying allowed
  void operator=(const DirEnvWrapper& wrapper);
  DirEnvWrapper(const DirEnvWrapper&);

  // Max write speed
  uint64_t rate_;  // Bytes per second
  bool sync_on_close_;
  bool ignore_sync_;
  bool drop_;

  class TrafficControlledWritableFile : public pdlfs::WritableFile {
   public:
    TrafficControlledWritableFile(DirEnvWrapper* wrapper, WritableFile* base)
        : maxbw_(wrapper->rate_),
          sync_on_close_(wrapper->sync_on_close_),
          ignore_sync_(wrapper->ignore_sync_),
          drop_(wrapper->drop_),
          base_(base),
          env_(wrapper) {}

    virtual ~TrafficControlledWritableFile() {
      delete base_;  // Owned by us
    }

    virtual pdlfs::Status Append(const pdlfs::Slice& data) {
      pdlfs::Status status;
      uint64_t io_start = 0;
      if (maxbw_ != 0) io_start = env_->NowMicros();
      if (!drop_ && base_ != NULL) {
        status = base_->Append(data);
      }
      if (maxbw_ != 0 && status.ok()) {
        uint64_t delay = data.size() * 1000 * 1000 / maxbw_;
        uint64_t now = env_->NowMicros();
        if (delay > 10 && now - io_start < delay - 10) {
          int sleep = static_cast<int>(delay + io_start - now - 10);
          env_->SleepForMicroseconds(sleep);
        }
      }
      return status;
    }

    virtual pdlfs::Status Close() {
      pdlfs::Status status;
      if (base_ != NULL) {
        if (!drop_ && sync_on_close_) {
          status = base_->Sync();
        }
        if (status.ok()) {
          status = base_->Close();
        }
      }
      return status;
    }

    virtual pdlfs::Status Flush() {
      pdlfs::Status status;
      if (!drop_ && base_ != NULL) {
        status = base_->Flush();
      }
      return status;
    }

    virtual pdlfs::Status Sync() {
      pdlfs::Status status;
      if (!drop_ && !ignore_sync_ && base_ != NULL) {
        status = base_->Sync();
      }
      return status;
    }

   private:
    // Constant after construction
    const uint64_t maxbw_;  // Bytes per second.  Set 0 to disable.
    const bool sync_on_close_;
    const bool ignore_sync_;
    const bool drop_;
    WritableFile* const base_;  // May be NULL
    Env* const env_;
  };
};

pdlfs::Status DirEnvWrapper::NewSequentialFile(const char* f,
                                               pdlfs::SequentialFile** r) {
  pdlfs::SequentialFile* file;
  pdlfs::Status s = target()->NewSequentialFile(f, &file);
  if (s.ok()) {
    pdlfs::MutexLock ml(&mu_);
    pdlfs::SequentialFileStats* stats = new pdlfs::SequentialFileStats;
    *r = new pdlfs::MeasuredSequentialFile(stats, file);
    sequentialfile_repo_.push_back(stats);
  } else {
    *r = NULL;
  }
  return s;
}

pdlfs::Status DirEnvWrapper::NewRandomAccessFile(const char* f,
                                                 pdlfs::RandomAccessFile** r) {
  pdlfs::RandomAccessFile* file;
  pdlfs::Status s = target()->NewRandomAccessFile(f, &file);
  if (s.ok()) {
    pdlfs::MutexLock ml(&mu_);
    pdlfs::RandomAccessFileStats* stats = new pdlfs::RandomAccessFileStats;
    *r = new pdlfs::MeasuredRandomAccessFile(stats, file);
    randomaccessfile_repo_.push_back(stats);
  } else {
    *r = NULL;
  }
  return s;
}

pdlfs::Status DirEnvWrapper::NewWritableFile(const char* f,
                                             pdlfs::WritableFile** r) {
  pdlfs::WritableFile* file;
  pdlfs::Status s = target()->NewWritableFile(f, &file);
  if (s.ok()) {
    pdlfs::MutexLock ml(&mu_);
    pdlfs::WritableFile* tc = new TrafficControlledWritableFile(this, file);
    pdlfs::WritableFileStats* stats = new pdlfs::WritableFileStats;
    *r = new pdlfs::MeasuredWritableFile(stats, tc);
    writablefile_repo_.push_back(stats);
  } else {
    *r = NULL;
  }
  return s;
}

}  // namespace

extern "C" {
struct deltafs_plfsdir {
  pdlfs::Env* env;          // Not owned by us
  pdlfs::ThreadPool* pool;  // Not owned by us
  const pdlfs::FilterPolicy* db_filter;
  bool db_drain_compactions;
  uint32_t db_epoch;
  pdlfs::DB* db;
  pdlfs::SynchronizableFile* io_buf;
  pdlfs::WritableFile* io_writer;
  DirWriter* writer;
  pdlfs::RandomAccessFile* io_reader;
  DirReader* reader;
  bool unordered;  // If the unordered mode should be used
  // If the multi-map mode should be used
  bool multi;
  bool is_env_pfs;
  bool opened;  // If deltafs_plfsdir_open() has been called
  bool enable_io_measurement;
  bool enable_side_io;
  size_t side_io_buf_size;
  DirOptions* io_options;
  deltafs_printer_t printer;  // Error printer
  void* printer_arg;
  DirEnvWrapper* io_env;
  int io_engine;
  // O_WRONLY or O_RDONLY
  int mode;
};

deltafs_plfsdir_t* deltafs_plfsdir_create_handle(const char* __conf, int __mode,
                                                 int __io_engine) {
  __mode = __mode & O_ACCMODE;
  if (__mode == O_RDONLY || __mode == O_WRONLY) {
    deltafs_plfsdir_t* dir =
        static_cast<deltafs_plfsdir_t*>(malloc(sizeof(deltafs_plfsdir_t)));
    memset(dir, 0, sizeof(deltafs_plfsdir_t));
    dir->io_engine = __io_engine;
    dir->db_drain_compactions = true;
    dir->io_options = new DirOptions(ParseOptions(__conf));
    dir->side_io_buf_size = 2 << 20;
    dir->mode = __mode;
    dir->is_env_pfs = true;
    dir->enable_io_measurement = true;
    dir->io_options->mode = DefaultDirMode();
    dir->env = DefaultDirEnv();
    return dir;
  } else {
    SetErrno(BadArgs());
    return NULL;
  }
}

int deltafs_plfsdir_set_fixed_kv(deltafs_plfsdir_t* __dir, int __flag) {
  if (__dir != NULL && !__dir->opened) {
    __dir->io_options->fixed_kv_length = static_cast<bool>(__flag);
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_set_unordered(deltafs_plfsdir_t* __dir, int __flag) {
  if (__dir != NULL && !__dir->opened) {
    __dir->unordered = static_cast<bool>(__flag);
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_set_multimap(deltafs_plfsdir_t* __dir, int __flag) {
  if (__dir != NULL && !__dir->opened) {
    __dir->multi = static_cast<bool>(__flag);
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_force_leveldb_fmt(deltafs_plfsdir_t* __dir, int __flag) {
  if (__dir != NULL && !__dir->opened) {
    __dir->io_options->leveldb_compatible = static_cast<bool>(__flag);
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_set_key_size(deltafs_plfsdir_t* __dir, size_t __key_size) {
  if (__key_size < 8) {
    __key_size = 8;
  } else if (__key_size > 16) {
    __key_size = 16;
  }
  if (__dir != NULL && !__dir->opened) {
    __dir->io_options->key_size = __key_size;
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_set_val_size(deltafs_plfsdir_t* __dir, size_t __val_size) {
  if (__dir != NULL && !__dir->opened) {
    __dir->io_options->value_size = __val_size;
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_set_env(deltafs_plfsdir_t* __dir, deltafs_env_t* __env) {
  if (__dir != NULL && !__dir->opened && __env != NULL) {
    __dir->is_env_pfs = __env->is_pfs;
    __dir->env = __env->env;
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_set_thread_pool(deltafs_plfsdir_t* __dir,
                                    deltafs_tp_t* __tp) {
  if (__dir != NULL && !__dir->opened && __tp != NULL) {
    __dir->pool = __tp->pool;
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_set_rank(deltafs_plfsdir_t* __dir, int __rank) {
  if (__dir != NULL && !__dir->opened) {
    __dir->io_options->rank = __rank;
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_set_err_printer(deltafs_plfsdir_t* __dir,
                                    deltafs_printer_t __printer,
                                    void* __printer_arg) {
  if (__dir != NULL && !__dir->opened) {
    __dir->printer_arg = __printer_arg;
    __dir->printer = __printer;
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_set_non_blocking(deltafs_plfsdir_t* __dir, int __flag) {
  if (__dir != NULL && !__dir->opened) {
    const bool non_blocking = static_cast<bool>(__flag);
    __dir->io_options->non_blocking = non_blocking;
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_enable_io_measurement(deltafs_plfsdir_t* __dir,
                                          int __flag) {
  if (__dir != NULL && !__dir->opened) {
    const bool measure_io = static_cast<bool>(__flag);
    __dir->enable_io_measurement = measure_io;
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_get_memparts(deltafs_plfsdir_t* __dir) {
  if (__dir != NULL) {
    int lg_parts = __dir->io_options->lg_parts;
    if (lg_parts < 0) lg_parts = 0;
    return 1 << lg_parts;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

}  // extern C

namespace {
struct ScanState {
  int (*saver)(void*, const char* key, size_t keylen, const char* d,
               size_t dlen);
  void* arg;
};

int ScanSaver(void* arg, const pdlfs::Slice& k, const pdlfs::Slice& v) {
  ScanState* s = reinterpret_cast<ScanState*>(arg);
  return s->saver(s->arg, k.data(), k.size(), v.data(), v.size());
}

std::string LevelDbName(const std::string& parent, int rank) {
  char tmp[20];
  snprintf(tmp, sizeof(tmp), "db-%08x", rank);
  return parent + "/" + tmp;
}

pdlfs::Status OpenAsLevelDb(deltafs_plfsdir_t* dir, const std::string& parent) {
  pdlfs::Status s;
  int rank = dir->io_options->rank;
  pdlfs::Env* env = dir->env;
  std::string dbname = LevelDbName(parent, rank);
  pdlfs::DBOptions dboptions;

  dboptions.skip_lock_file = true;
  dboptions.compression = pdlfs::kNoCompression;
  dboptions.disable_seek_compaction = true;
  dboptions.disable_write_ahead_log = true;
  dboptions.create_if_missing = true;
  dboptions.compaction_pool = dir->pool;
  dboptions.write_buffer_size =  //
      dir->io_options->total_memtable_budget / 2;
  if (dir->enable_io_measurement) {
    dir->io_env = new DirEnvWrapper(dir->env);
    env = dir->io_env;
  }
  if (dir->io_engine == DELTAFS_PLFSDIR_LEVELDB_L0ONLY_BF &&
      dir->io_options->bf_bits_per_key != 0)
    dir->db_filter =
        pdlfs::NewBloomFilterPolicy(int(dir->io_options->bf_bits_per_key));
  if (dir->io_engine == DELTAFS_PLFSDIR_LEVELDB_L0ONLY_BF ||
      dir->io_engine == DELTAFS_PLFSDIR_LEVELDB_L0ONLY || dir->mode == O_RDONLY)
    dboptions.disable_compaction = true;
  if (dir->mode == O_WRONLY) {
    dboptions.error_if_exists = true;
  }

  dboptions.env = env;
  dboptions.filter_policy = dir->db_filter;
  s = pdlfs::DB::Open(dboptions, dbname, &dir->db);

  return s;
}

pdlfs::Status DbPut(deltafs_plfsdir_t* dir, const pdlfs::Slice& key,
                    const pdlfs::Slice& value) {
  pdlfs::Status s;
  pdlfs::WriteOptions options;
  options.sync = false;

  std::string composite = key.ToString();
  pdlfs::PutFixed32(&composite, dir->db_epoch);
  s = dir->db->Put(options, composite, value);

  return s;
}

pdlfs::Status DbEpochFlush(deltafs_plfsdir_t* dir) {
  pdlfs::Status s;
  pdlfs::FlushOptions options;
  options.wait = false;

  s = dir->db->FlushMemTable(options);

  dir->db_epoch++;

  return s;
}

pdlfs::Status DbFlush(deltafs_plfsdir_t* dir) {
  pdlfs::Status s;
  pdlfs::FlushOptions options;
  options.wait = false;

  s = dir->db->FlushMemTable(options);

  return s;
}

pdlfs::Status DbWait(deltafs_plfsdir_t* dir) {
  pdlfs::Status s;
  s = dir->db->DrainCompactions();
  return s;
}

pdlfs::Status DbSync(deltafs_plfsdir_t* dir) {
  pdlfs::Status s;
  s = dir->db->SyncWAL();
  return s;
}

pdlfs::Status DbFin(deltafs_plfsdir_t* dir) {
  pdlfs::Status s;
  pdlfs::FlushOptions options;
  if (dir->io_engine != DELTAFS_PLFSDIR_LEVELDB_L0ONLY_BF &&
      dir->io_engine != DELTAFS_PLFSDIR_LEVELDB_L0ONLY)
    options.force_flush_l0 = true;
  options.wait = true;

  s = dir->db->FlushMemTable(options);
  if (s.ok() && dir->db_drain_compactions) {
    s = dir->db->DrainCompactions();
  }

  return s;
}

pdlfs::Status DbGet(deltafs_plfsdir_t* dir, const pdlfs::Slice& key,
                    std::string* value) {
  pdlfs::Status s;
  pdlfs::ReadOptions options;
  options.fill_cache = false;

  if (dir->io_engine == DELTAFS_PLFSDIR_LEVELDB_L0ONLY_BF) {
    std::string composite = key.ToString();
    for (uint32_t epoch = 0;; epoch++) {
      composite.resize(key.size());
      pdlfs::PutFixed32(&composite, epoch);
      s = dir->db->Get(options, composite, value);
      if (!s.ok()) {
        if (s.IsNotFound()) {
          s = pdlfs::Status::OK();
        }
        break;
      }
    }
  } else {
    pdlfs::Iterator* iter = dir->db->NewIterator(options);
    iter->Seek(key);

    for (; iter->Valid(); iter->Next()) {
      if (iter->key().starts_with(key)) {
        if (iter->key().size() == key.size() + 4) {
          value->append(iter->value().data(), iter->value().size());
        }
      } else {
        break;
      }
    }

    if (!iter->status().ok()) {
      s = iter->status();
    }

    delete iter;
  }

  return s;
}

void FinalizeDirMode(deltafs_plfsdir_t* dir) {
  if (dir->multi && dir->unordered) {
    DirMode multimap_unordered = pdlfs::plfsio::kDmMultiMapUnordered;
    dir->io_options->mode = multimap_unordered;
  } else if (dir->unordered) {
    DirMode unordered = pdlfs::plfsio::kDmUniqueUnordered;
    dir->io_options->mode = unordered;
  } else if (dir->multi) {
    DirMode multimap = pdlfs::plfsio::kDmMultiMap;
    // Allow multiple insertions per key within a single epoch
    dir->io_options->mode = multimap;
  } else {  // By default, each key is inserted at most once within each epoch
#ifndef NDEBUG
    DirMode drop = pdlfs::plfsio::kDmUniqueDrop;
    dir->io_options->mode = drop;  // Debug duplicates
#endif
  }
}

pdlfs::Status OpenDir(deltafs_plfsdir_t* dir, const std::string& name) {
  // To obtain detailed error status, an error printer
  // must be set by the caller
  pdlfs::Status s;
  pdlfs::Env* env = dir->env;
  FinalizeDirMode(dir);

  dir->io_options->allow_env_threads = false;  // No implicit background threads
  dir->io_options->is_env_pfs = dir->is_env_pfs;

  if (dir->enable_io_measurement) {
    dir->io_env = new DirEnvWrapper(dir->env);
    env = dir->io_env;
  }
  dir->io_options->env = env;

  if (dir->mode == O_WRONLY) {
    DirWriter* writer;
    dir->io_options->compaction_pool = dir->pool;
    dir->io_options->measure_writes = false;
    s = DirWriter::Open(*dir->io_options, name, &writer);
    if (s.ok()) {
      dir->writer = writer;
    }
  } else if (dir->mode == O_RDONLY) {
    DirReader* reader;
    dir->io_options->reader_pool = dir->pool;
    dir->io_options->measure_reads = false;
    s = DirReader::Open(*dir->io_options, name, &reader);
    if (s.ok()) {
      dir->reader = reader;
    }
  } else {
    s = BadArgs();
  }

  return s;
}

std::string SideName(const std::string& parent, int rank) {
  char tmp[20];
  snprintf(tmp, sizeof(tmp), "D-%08x.bin", rank);
  return parent + "/" + tmp;
}

pdlfs::Status OpenSideIo(deltafs_plfsdir_t* dir, const std::string& name) {
  pdlfs::Status s;
  int rank = dir->io_options->rank;
  std::string fname = SideName(name, rank);
  pdlfs::Env* const env = dir->io_options->env;
  size_t buf_size = dir->side_io_buf_size;

  if (dir->mode == O_WRONLY) {
    pdlfs::WritableFile* io_file;
    s = env->NewWritableFile(fname.c_str(), &io_file);
    if (s.ok()) {
      if (buf_size != 0) {
        dir->io_buf =
            new pdlfs::MinMaxBufferedWritableFile(io_file, buf_size, buf_size);
        dir->io_writer = dir->io_buf;
      } else {
        dir->io_writer = io_file;
      }
    }
  } else if (dir->mode == O_RDONLY) {
    pdlfs::RandomAccessFile* io_file;
    s = env->NewRandomAccessFile(fname.c_str(), &io_file);
    if (s.ok()) {
      dir->io_reader = io_file;
    }
  } else {
    s = BadArgs();
  }

  return s;
}

pdlfs::Status FinishSideIo(deltafs_plfsdir_t* dir) {
  pdlfs::Status s;
  // Sync() may be disabled by an underlying env,
  // so we do Flush() before Sync().
  // Flush() may be ignored by a buffered writable file.
  // In this case, Sync() will do Flush() implicitly.
  s = dir->io_writer->Flush();
  if (s.ok()) s = dir->io_writer->Sync();
  if (s.ok()) dir->io_writer->Close();
  return s;
}

int DirError(deltafs_plfsdir_t* dir, const pdlfs::Status& s) {
  if (dir != NULL && dir->printer != NULL) {
    dir->printer(s.ToString().c_str(), dir->printer_arg);
  }
  SetErrno(s);
  return -1;
}

bool IsDirOpened(deltafs_plfsdir_t* dir) {
  if (dir != NULL) {
    return dir->opened;
  } else {
    return false;
  }
}

}  // namespace

extern "C" {
int deltafs_plfsdir_open(deltafs_plfsdir_t* __dir, const char* __name) {
  pdlfs::Status s;

  if (__dir == NULL || __dir->opened) {
    s = BadArgs();
  } else if (__name == NULL || __name[0] == 0) {
    s = BadArgs();
  } else {
    if (__dir->io_engine == DELTAFS_PLFSDIR_DEFAULT) {
      s = OpenDir(__dir, __name);
    } else {
      s = OpenAsLevelDb(__dir, __name);
    }
  }

  if (s.ok()) {
    if (__dir->enable_side_io) {
      s = OpenSideIo(__dir, __name);
    }
  }
  if (s.ok()) {
    __dir->opened = true;
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return 0;
  }
}

ssize_t deltafs_plfsdir_put(deltafs_plfsdir_t* __dir, const char* __key,
                            size_t __keylen, int __epoch, const char* __value,
                            size_t __sz) {
  pdlfs::Status s;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_WRONLY) {
    s = BadArgs();
  } else if (__key == NULL) {
    s = BadArgs();
  } else if (__keylen == 0) {
    s = BadArgs();
  } else {
    pdlfs::Slice k(__key, __keylen), v(__value, __sz);
    if (__dir->io_engine == DELTAFS_PLFSDIR_DEFAULT) {
      s = __dir->writer->Add(k, v, __epoch);
    } else {
      s = DbPut(__dir, k, v);
    }
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return __sz;
  }
}

ssize_t deltafs_plfsdir_append(deltafs_plfsdir_t* __dir, const char* __fname,
                               int __epoch, const void* __buf, size_t __sz) {
  pdlfs::Status s;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_WRONLY) {
    s = BadArgs();
  } else if (__fname == NULL) {
    s = BadArgs();
  } else if (__fname[0] == 0) {
    s = BadArgs();
  } else {
    char tmp[16];
#ifdef PLFSIO_HASH_USE_SPOOKY
    pdlfs::Spooky128(__fname, strlen(__fname), 0, 0, tmp);
#else
    pdlfs::murmur_x64_128(__fname, int(strlen(__fname)), 0, tmp);
#endif
    pdlfs::Slice k(tmp, __dir->io_options->key_size);
    const char* data = static_cast<const char*>(__buf);
    pdlfs::Slice v(data, __sz);
    if (__dir->io_engine == DELTAFS_PLFSDIR_DEFAULT) {
      s = __dir->writer->Add(k, v, __epoch);
    } else {
      s = DbPut(__dir, k, v);
    }
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return __sz;
  }
}

int deltafs_plfsdir_epoch_flush(deltafs_plfsdir_t* __dir, int __epoch) {
  pdlfs::Status s;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_WRONLY) {
    s = BadArgs();
  } else {
    if (__dir->io_engine == DELTAFS_PLFSDIR_DEFAULT) {
      s = __dir->writer->EpochFlush(__epoch);
    } else {
      s = DbEpochFlush(__dir);
    }
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return 0;
  }
}

int deltafs_plfsdir_flush(deltafs_plfsdir_t* __dir, int __epoch) {
  pdlfs::Status s;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_WRONLY) {
    s = BadArgs();
  } else {
    if (__dir->io_engine == DELTAFS_PLFSDIR_DEFAULT) {
      s = __dir->writer->Flush(__epoch);
    } else {
      s = DbFlush(__dir);
    }
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return 0;
  }
}

int deltafs_plfsdir_wait(deltafs_plfsdir_t* __dir) {
  pdlfs::Status s;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_WRONLY) {
    s = BadArgs();
  } else {
    if (__dir->io_engine == DELTAFS_PLFSDIR_DEFAULT) {
      s = __dir->writer->Wait();
    } else {
      s = DbWait(__dir);
    }
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return 0;
  }
}

int deltafs_plfsdir_sync(deltafs_plfsdir_t* __dir) {
  pdlfs::Status s;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_WRONLY) {
    s = BadArgs();
  } else {
    if (__dir->io_engine == DELTAFS_PLFSDIR_DEFAULT) {
      s = __dir->writer->Sync();
    } else {
      s = DbSync(__dir);
    }
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return 0;
  }
}

ssize_t deltafs_plfsdir_io_append(deltafs_plfsdir_t* __dir, const void* __buf,
                                  size_t __sz) {
  pdlfs::Status s;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_WRONLY) {
    s = BadArgs();
  } else if (!__dir->enable_side_io) {
    s = BadArgs();
  } else {
    const char* data = static_cast<const char*>(__buf);
    pdlfs::Slice v(data, __sz);
    s = __dir->io_writer->Append(v);
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return __sz;
  }
}

int deltafs_plfsdir_io_flush(deltafs_plfsdir_t* __dir) {
  pdlfs::Status s;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_WRONLY) {
    s = BadArgs();
  } else if (!__dir->enable_side_io) {
    s = BadArgs();
  } else {
    s = __dir->io_writer->Flush();
    // Flush() may be ignored by a buffered writable file.
    if (s.ok() && __dir->io_buf != NULL) {
      s = __dir->io_buf->EmptyBuffer();
    }
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return 0;
  }
}

int deltafs_plfsdir_io_sync(deltafs_plfsdir_t* __dir) {
  pdlfs::Status s;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_WRONLY) {
    s = BadArgs();
  } else if (!__dir->enable_side_io) {
    s = BadArgs();
  } else {
    pdlfs::WritableFile* f = __dir->io_writer;
    s = f->Sync();
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return 0;
  }
}

int deltafs_plfsdir_finish(deltafs_plfsdir_t* __dir) {
  pdlfs::Status s;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_WRONLY) {
    s = BadArgs();
  } else {
    if (__dir->io_engine == DELTAFS_PLFSDIR_DEFAULT) {
      s = __dir->writer->Finish();
    } else {
      s = DbFin(__dir);
    }
  }

  if (s.ok()) {
    if (__dir->enable_side_io) {
      s = FinishSideIo(__dir);
    }
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return 0;
  }
}

char* deltafs_plfsdir_get_property(deltafs_plfsdir_t* __dir,
                                   const char* __key) {
  if (!IsDirOpened(__dir)) {
    return NULL;
  } else if (__key == NULL || __key[0] == 0) {
    return NULL;
  } else {
    pdlfs::Slice k(__key);
    if (k.starts_with("io.") && __dir->io_env != NULL) {
      k.remove_prefix(3);
      if (k == "total_read_open") {
        size_t tro = __dir->io_env->TotalFilesOpenedForRead();
        return MakeChar(tro);
      } else if (k == "total_bytes_read") {
        uint64_t tbr = __dir->io_env->TotalBytesRead();
        return MakeChar(tbr);
      } else if (k == "total_write_open") {
        size_t two = __dir->io_env->TotalFilesOpenedForWrite();
        return MakeChar(two);
      } else if (k == "total_bytes_written") {
        uint64_t tbw = __dir->io_env->TotalBytesWritten();
        return MakeChar(tbw);
      } else if (k == "total_seeks") {
        uint64_t tsk = __dir->io_env->TotalRandomSeeks();
        return MakeChar(tsk);
      }
    } else if (__dir->writer != NULL) {
      if (k == "total_user_data") {
        uint64_t vsz = __dir->writer->TEST_value_bytes();
        uint64_t ksz = __dir->writer->TEST_key_bytes();
        return MakeChar(ksz + vsz);
      } else if (k == "total_memory_usage") {
        uint64_t mem = __dir->writer->TEST_total_memory_usage();
        return MakeChar(mem);
      } else if (k == "num_keys") {
        uint64_t nks = __dir->writer->TEST_num_keys();
        return MakeChar(nks);
      } else if (k == "num_dropped_keys") {
        uint64_t nds = __dir->writer->TEST_num_dropped_keys();
        return MakeChar(nds);
      } else if (k == "sstable_filter_bytes") {
        uint64_t fsz = __dir->writer->TEST_raw_filter_contents();
        return MakeChar(fsz);
      } else if (k == "sstable_index_bytes") {
        uint64_t isz = __dir->writer->TEST_raw_index_contents();
        return MakeChar(isz);
      } else if (k == "sstable_data_bytes") {
        uint64_t dsz = __dir->writer->TEST_raw_data_contents();
        return MakeChar(dsz);
      } else if (k == "num_data_blocks") {
        uint64_t nbs = __dir->writer->TEST_num_data_blocks();
        return MakeChar(nbs);
      } else if (k == "num_sstables") {
        uint64_t tbs = __dir->writer->TEST_num_sstables();
        return MakeChar(tbs);
      }
    } else if (__dir->reader != NULL) {
      // TODO
    }
    return NULL;
  }
}

long long deltafs_plfsdir_get_integer_property(deltafs_plfsdir_t* __dir,
                                               const char* __key) {
  char* val = deltafs_plfsdir_get_property(__dir, __key);
  if (val != NULL) {
    long long result = atoll(val);
    free(val);
    return result;
  } else {
    return 0;
  }
}

char* deltafs_plfsdir_get(deltafs_plfsdir_t* __dir, const char* __key,
                          size_t __keylen, int __epoch, size_t* __sz,
                          size_t* __table_seeks, size_t* __seeks) {
  pdlfs::Status s;
  std::string dst;
  char* result = NULL;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_RDONLY) {
    s = BadArgs();
  } else if (__key == NULL) {
    s = BadArgs();
  } else if (__keylen == 0) {
    s = BadArgs();
  } else {
    DirReader::ReadOp op;
    op.SetEpoch(__epoch);
    op.table_seeks = __table_seeks;
    op.seeks = __seeks;
    if (__dir->io_engine == DELTAFS_PLFSDIR_DEFAULT) {
      s = __dir->reader->Read(op, pdlfs::Slice(__key, __keylen), &dst);
    } else {
      s = DbGet(__dir, pdlfs::Slice(__key, __keylen), &dst);
    }
    if (s.ok()) {
      result = static_cast<char*>(malloc(dst.size()));
      memcpy(result, dst.data(), dst.size());
      if (__sz != NULL) {
        *__sz = dst.size();
      }
    }
  }

  if (!s.ok()) {
    DirError(__dir, s);
    return NULL;
  } else {
    return result;
  }
}

void* deltafs_plfsdir_read(deltafs_plfsdir_t* __dir, const char* __fname,
                           int __epoch, size_t* __sz, size_t* __table_seeks,
                           size_t* __seeks) {
  pdlfs::Status s;
  std::string dst;
  char* result = NULL;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_RDONLY) {
    s = BadArgs();
  } else if (__fname == NULL) {
    s = BadArgs();
  } else if (__fname[0] == 0) {
    s = BadArgs();
  } else {
    char tmp[16];
    DirReader::ReadOp op;
    op.SetEpoch(__epoch);
    op.table_seeks = __table_seeks;
    op.seeks = __seeks;
#ifdef PLFSIO_HASH_USE_SPOOKY
    pdlfs::Spooky128(__fname, strlen(__fname), 0, 0, tmp);
#else
    pdlfs::murmur_x64_128(__fname, int(strlen(__fname)), 0, tmp);
#endif
    pdlfs::Slice k(tmp, __dir->io_options->key_size);
    if (__dir->io_engine == DELTAFS_PLFSDIR_DEFAULT) {
      s = __dir->reader->Read(op, k, &dst);
    } else {
      s = DbGet(__dir, k, &dst);
    }
    if (s.ok()) {
      result = static_cast<char*>(malloc(dst.size()));
      memcpy(result, dst.data(), dst.size());
      if (__sz != NULL) {
        *__sz = dst.size();
      }
    }
  }

  if (!s.ok()) {
    DirError(__dir, s);
    return NULL;
  } else {
    return result;
  }
}

ssize_t deltafs_plfsdir_scan(deltafs_plfsdir_t* __dir, int __epoch,
                             int (*saver)(void* arg, const char* __key,
                                          size_t __keylen, const char* __value,
                                          size_t sz),
                             void* arg) {
  pdlfs::Status s;
  ScanState state;
  state.saver = saver;
  state.arg = arg;
  size_t n = 0;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_RDONLY) {
    s = BadArgs();
  } else {
    DirReader::ScanOp op;
    op.SetEpoch(__epoch);
    op.n = &n;
    if (__dir->io_engine == DELTAFS_PLFSDIR_DEFAULT) {
      s = __dir->reader->Scan(op, ScanSaver, &state);
    } else {
      // Not implemented
    }
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return n;
  }
}

ssize_t deltafs_plfsdir_count(deltafs_plfsdir_t* __dir, int __epoch) {
  pdlfs::Status s;
  size_t n = 0;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_RDONLY) {
    s = BadArgs();
  } else {
    DirReader::CountOp op;
    op.SetEpoch(__epoch);
    if (__dir->io_engine == DELTAFS_PLFSDIR_DEFAULT) {
      s = __dir->reader->Count(op, &n);
    } else {
      // Not implemented
    }
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return n;
  }
}

ssize_t deltafs_plfsdir_io_pread(deltafs_plfsdir_t* __dir, void* __buf,
                                 size_t __sz, off_t __off) {
  pdlfs::Status s;
  pdlfs::Slice result;
  ssize_t n = 0;

  if (!IsDirOpened(__dir)) {
    s = BadArgs();
  } else if (__dir->mode != O_RDONLY) {
    s = BadArgs();
  } else if (!__dir->enable_side_io) {
    s = BadArgs();
  } else {
    char* scratch = static_cast<char*>(__buf);
    s = __dir->io_reader->Read(__off, __sz, &result, scratch);
    if (s.ok()) {
      n = result.size();
      if (n != 0 && result.data() != scratch) {
        memcpy(__buf, result.data(), n);
      }
    }
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return n;
  }
}

int deltafs_plfsdir_destroy(deltafs_plfsdir_t* __dir, const char* __name) {
  pdlfs::Status s;

  if (__dir == NULL) {
    s = BadArgs();
  } else if (__dir->opened) {
    s = BadArgs();
  } else {
    s = pdlfs::plfsio::DestroyDir(__name, *__dir->io_options);
  }

  if (!s.ok()) {
    return DirError(__dir, s);
  } else {
    return 0;
  }
}

int deltafs_plfsdir_free_handle(deltafs_plfsdir_t* __dir) {
  if (__dir == NULL) return 0;

  delete __dir->writer;
  delete __dir->reader;
  delete __dir->db;
  delete __dir->db_filter;
  delete __dir->io_options;
  delete __dir->io_writer;
  delete __dir->io_reader;
  delete __dir->io_env;
  free(__dir);

  return 0;
}

}  // extern C
