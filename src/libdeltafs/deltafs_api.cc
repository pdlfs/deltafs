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
#include "pdlfs-common/logging.h"
#include "pdlfs-common/murmur.h"
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/spooky.h"
#include "pdlfs-common/status.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>

#include <string>

extern "C" {
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
  } else if (s.IsReadOnly()) {
    errno = EROFS;
  } else if (s.IsNotSupported()) {
    errno = ENOSYS;
  } else if (s.IsInvalidArgument()) {
    errno = EINVAL;
  } else if (s.IsDisconnected()) {
    errno = EHOSTUNREACH;
  } else if (s.IsBufferFull()) {
    errno = ENOBUFS;
  } else if (s.IsRange()) {
    errno = ERANGE;
  } else {
    errno = EIO;
  }
}

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

struct deltafs_plfsdir {
  pdlfs::Env* env;          // Not owned by us
  pdlfs::ThreadPool* pool;  // Not owned by us
  bool db_wait_for_compactions;
  uint32_t db_epoch;
  pdlfs::DB* db;
  DirWriter* writer;
  DirReader* reader;
  // If the multi-map mode should be used
  bool multi;
  bool is_env_pfs;
  bool opened;  // If deltafs_plfsdir_open() has been called
  DirOptions* options;
  deltafs_printer_t printer;  // Error printer
  void* printer_arg;
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
    dir->db_wait_for_compactions = true;
    dir->options = new DirOptions(ParseOptions(__conf));
    dir->mode = __mode;
    dir->is_env_pfs = true;
    dir->options->mode = DefaultDirMode();
    dir->env = DefaultDirEnv();
    return dir;
  } else {
    SetErrno(BadArgs());
    return NULL;
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

int deltafs_plfsdir_set_key_size(deltafs_plfsdir_t* __dir, size_t __key_size) {
  if (__key_size < 8) {
    __key_size = 8;
  } else if (__key_size > 16) {
    __key_size = 16;
  }
  if (__dir != NULL && !__dir->opened) {
    __dir->options->key_size = __key_size;
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_set_val_size(deltafs_plfsdir_t* __dir, size_t __val_size) {
  if (__dir != NULL && !__dir->opened) {
    __dir->options->value_size = __val_size;
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
    __dir->options->rank = __rank;
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
    __dir->options->non_blocking = non_blocking;
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
    __dir->options->measure_writes = measure_io;
    __dir->options->measure_reads = measure_io;
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_get_memparts(deltafs_plfsdir_t* __dir) {
  if (__dir != NULL) {
    int lg_parts = __dir->options->lg_parts;
    if (lg_parts < 0) lg_parts = 0;
    return 1 << lg_parts;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

namespace {
struct ScanState {
  void (*saver)(void*, const char* key, size_t keylen, const char* d,
                size_t dlen);
  void* arg;
};

void ScanSaver(void* arg, const pdlfs::Slice& k, const pdlfs::Slice& v) {
  ScanState* state = reinterpret_cast<ScanState*>(arg);
  state->saver(state->arg, k.data(), k.size(), v.data(), v.size());
}

std::string LevelDbName(const std::string& parent, int rank) {
  char tmp[20];
  snprintf(tmp, sizeof(tmp), "db-%08x", rank);
  return parent + "/" + tmp;
}

pdlfs::Status OpenAsLevelDb(deltafs_plfsdir_t* dir, const std::string& parent) {
  pdlfs::Status s;
  int rank = dir->options->rank;
  std::string dbname = LevelDbName(parent, rank);
  pdlfs::DBOptions dboptions;

  dboptions.skip_lock_file = true;
  dboptions.disable_seek_compaction = true;
  dboptions.disable_write_ahead_log = true;
  dboptions.create_if_missing = true;
  dboptions.compaction_pool = dir->pool;
  dboptions.env = dir->env;

  if (dir->mode == O_RDONLY || dir->io_engine == DELTAFS_PLFSDIR_LEVELDB_L0ONLY)
    dboptions.disable_compaction = true;
  if (dir->mode == O_WRONLY) dboptions.error_if_exists = true;
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

pdlfs::Status DbFin(deltafs_plfsdir_t* dir) {
  pdlfs::Status s;
  pdlfs::FlushOptions options;
  options.wait = true;

  s = dir->db->FlushMemTable(options);
  if (s.ok() && dir->db_wait_for_compactions) {
    dir->db->WaitForCompactions();
  }

  return s;
}

pdlfs::Status DbGet(deltafs_plfsdir_t* dir, const pdlfs::Slice& key,
                    std::string* value) {
  pdlfs::Status s;
  pdlfs::ReadOptions options;
  options.fill_cache = false;
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

  s = iter->status();

  return s;
}

inline void FinalizeDirMode(deltafs_plfsdir_t* dir) {
  if (dir->multi) {
    DirMode multimap = pdlfs::plfsio::kDmMultiMap;
    // Allow multiple insertions per key within a single epoch
    dir->options->mode = multimap;
  } else {  // By default, each key is inserted at most once within each epoch
#ifndef NDEBUG
    DirMode drop = pdlfs::plfsio::kDmUniqueDrop;
    dir->options->mode = drop;  // Debug duplicates
#endif
  }
}

pdlfs::Status OpenDir(deltafs_plfsdir_t* dir, const std::string& name) {
  // To obtain detailed error status, an error printer
  // must be set by the caller
  pdlfs::Status s;
  FinalizeDirMode(dir);

  dir->options->allow_env_threads = false;  // No implicit background threads
  dir->options->is_env_pfs = dir->is_env_pfs;
  dir->options->env = dir->env;

  if (dir->mode == O_WRONLY) {
    DirWriter* writer;
    dir->options->compaction_pool = dir->pool;
    s = DirWriter::Open(*dir->options, name, &writer);
    if (s.ok()) {
      dir->writer = writer;
    }
  } else if (dir->mode == O_RDONLY) {
    DirReader* reader;
    dir->options->reader_pool = dir->pool;
    s = DirReader::Open(*dir->options, name, &reader);
    if (s.ok()) {
      dir->reader = reader;
    }
  } else {
    s = BadArgs();
  }

  return s;
}

int DirError(deltafs_plfsdir_t* dir, const pdlfs::Status& s) {
  if (dir->printer != NULL) {
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
      s = __dir->writer->Append(k, v, __epoch);
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
    pdlfs::Slice k(tmp, __dir->options->key_size);
    const char* data = static_cast<const char*>(__buf);
    pdlfs::Slice v(data, __sz);
    if (__dir->io_engine == DELTAFS_PLFSDIR_DEFAULT) {
      s = __dir->writer->Append(k, v, __epoch);
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
    if (__dir->writer != NULL) {
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
    pdlfs::Slice k(tmp, __dir->options->key_size);
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
                             void (*saver)(void* arg, const char* __key,
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

int deltafs_plfsdir_free_handle(deltafs_plfsdir_t* __dir) {
  if (__dir == NULL) return 0;

  delete __dir->options;
  delete __dir->writer;
  delete __dir->reader;
  delete __dir->db;

  free(__dir);

  return 0;
}

}  // extern C
