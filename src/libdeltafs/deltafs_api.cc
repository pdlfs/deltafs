/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs/deltafs_api.h"

#include "deltafs_client.h"
#include "deltafs_plfsio.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/dbfiles.h"
#include "pdlfs-common/logging.h"
#include "pdlfs-common/murmur.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/status.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>

#include <string>

namespace pdlfs {
extern void PrintSysInfo();
}

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

// -------------------------
// Light-weight plfsdir api
// -------------------------
namespace {

// Dir stats
typedef pdlfs::plfsio::DirStats DirStats;
// Dir options
typedef pdlfs::plfsio::DirOptions DirOptions;

// Dir writer
typedef pdlfs::plfsio::Writer Writer;
// Dir Reader
typedef pdlfs::plfsio::Reader Reader;

// Dir Env
typedef pdlfs::Env Env;

static inline Env* DefaultDirEnv() {
  // Avoid double-buffering
  return pdlfs::port::posix::GetUnBufferedIOEnv();
}

static inline DirOptions ParseOptions(const char* conf) {
  if (conf != NULL) {
    return pdlfs::plfsio::ParseDirOptions(conf);
  } else {
    return DirOptions();
  }
}

}  // namespace

struct deltafs_env {
  Env* env;
  int sys;
};

deltafs_env_t* deltafs_env_open(const char* __name, const char* __conf) {
  if (__name == NULL) {
    __name = "posix.unbufferedio";
  }
  if (__conf == NULL) {
    __conf = "";
  }

  bool is_system;
  Env* env = Env::Open(__name, __conf, &is_system);

  if (env != NULL) {
    deltafs_env_t* result = (deltafs_env_t*)malloc(sizeof(deltafs_env_t));
    result->sys = int(is_system);
    result->env = env;
    return result;
  } else {
    SetErrno(BadArgs());
    return NULL;
  }
}

int deltafs_env_is_system(deltafs_env_t* __env) {
  if (__env != NULL) {
    return __env->sys;
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

struct deltafs_plfsdir {
  Env* env;
  int filter_bits_per_key;
  int key_size;
  int mode;
  union {
    Writer* writer;
    Reader* reader;
  } io;
};

deltafs_plfsdir_t* deltafs_plfsdir_create_handle(int __mode) {
  __mode = __mode & O_ACCMODE;
  if (__mode == O_RDONLY || __mode == O_WRONLY) {
    deltafs_plfsdir_t* dir =
        (deltafs_plfsdir_t*)malloc(sizeof(deltafs_plfsdir_t));
    dir->env = DefaultDirEnv();
    dir->filter_bits_per_key = 10;
    dir->key_size = 10;
    dir->mode = __mode;

    if (__mode == O_RDONLY) {
      dir->io.reader = NULL;
    } else {
      dir->io.writer = NULL;
    }

    return dir;
  } else {
    SetErrno(BadArgs());
    return NULL;
  }
}

int deltafs_plfsdir_set_key_size(deltafs_plfsdir_t* __dir, int __key_size) {
  if (__key_size < 8) {
    __key_size = 8;
  } else if (__key_size > 16) {
    __key_size = 16;
  }
  if (__dir != NULL) {
    __dir->key_size = __key_size;
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_set_filter_bits_per_key(deltafs_plfsdir_t* __dir,
                                            int __bits_per_key) {
  if (__bits_per_key < 0) {
    __bits_per_key = 0;
  }
  if (__dir != NULL) {
    __dir->filter_bits_per_key = __bits_per_key;
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_set_env(deltafs_plfsdir_t* __dir, deltafs_env_t* __env) {
  if (__dir != NULL && __env != NULL) {
    __dir->env = __env->env;
    return 0;
  } else {
    SetErrno(BadArgs());
    return -1;
  }
}

int deltafs_plfsdir_open(deltafs_plfsdir_t* __dir, const char* __name,
                         const char* __conf) {
  pdlfs::Status s;

  if (__dir != NULL && __name != NULL) {
    DirOptions options = ParseOptions(__conf);
    options.bf_bits_per_key = __dir->filter_bits_per_key;
    options.key_size = __dir->key_size;
    options.env = __dir->env;
    if (__dir->mode == O_WRONLY) {
      s = Writer::Open(options, __name, &__dir->io.writer);
    } else {
      s = Reader::Open(options, __name, &__dir->io.reader);
    }
  } else {
    s = BadArgs();
  }

  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_plfsdir_append(deltafs_plfsdir_t* __dir, const char* __fname,
                           int __epoch, const void* __buf, size_t __sz) {
  pdlfs::Status s;

  if (__dir != NULL && __dir->mode == O_WRONLY && __fname != NULL) {
    char tmp[16];
    pdlfs::murmur_x64_128(__fname, strlen(__fname), 0, tmp);
    pdlfs::Slice k(tmp, __dir->key_size);
    s = __dir->io.writer->Append(
        k, pdlfs::Slice(reinterpret_cast<const char*>(__buf), __sz));
  } else {
    s = BadArgs();
  }

  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_plfsdir_epoch_flush(deltafs_plfsdir_t* __dir, int __epoch) {
  pdlfs::Status s;

  if (__dir != NULL && __dir->mode == O_WRONLY) {
    s = __dir->io.writer->MakeEpoch();
  } else {
    s = BadArgs();
  }

  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

int deltafs_plfsdir_finish(deltafs_plfsdir_t* __dir) {
  pdlfs::Status s;

  if (__dir != NULL && __dir->mode == O_WRONLY) {
    s = __dir->io.writer->Finish();
  } else {
    s = BadArgs();
  }

  if (s.ok()) {
    return 0;
  } else {
    SetErrno(s);
    return -1;
  }
}

char* deltafs_plfsdir_get_property(deltafs_plfsdir_t* __dir,
                                   const char* __key) {
  char tmp[100];

  if (__dir != NULL && __key != NULL) {
    if (__dir->mode == O_WRONLY) {
      const DirStats* const stats = __dir->io.writer->stats();
      if (stats != NULL) {
        if (strcmp(__key, "compaction_time") == 0) {
          snprintf(tmp, sizeof(tmp), "%llu",
                   static_cast<unsigned long long>(stats->write_micros));
          return strdup(tmp);
        } else if (strcmp(__key, "index_size") == 0) {
          snprintf(tmp, sizeof(tmp), "%llu",
                   static_cast<unsigned long long>(stats->index_size));
          return strdup(tmp);
        } else if (strcmp(__key, "data_size")) {
          snprintf(tmp, sizeof(tmp), "%llu",
                   static_cast<unsigned long long>(stats->data_size));
          return strdup(tmp);
        }
      }
    } else {
      // TODO
    }
  }

  return NULL;
}

char* deltafs_plfsdir_readall(deltafs_plfsdir_t* __dir, const char* __fname) {
  pdlfs::Status s;
  char* buf;

  if (__dir != NULL && __dir->mode == O_RDONLY) {
    std::string dst;
    char tmp[16];
    pdlfs::murmur_x64_128(__fname, strlen(__fname), 0, tmp);
    pdlfs::Slice k(tmp, __dir->key_size);
    s = __dir->io.reader->ReadAll(k, &dst);
    if (s.ok()) {
      buf = (char*)malloc(dst.size());
      memcpy(buf, dst.data(), dst.size());
    }
  } else {
    s = BadArgs();
  }

  if (s.ok()) {
    return buf;
  } else {
    SetErrno(s);
    return NULL;
  }
}

int deltafs_plfsdir_free_handle(deltafs_plfsdir_t* __dir) {
  if (__dir != NULL) {
    if (__dir->mode == O_WRONLY) {
      delete __dir->io.writer;
    } else {
      delete __dir->io.reader;
    }
    free(__dir);
  }

  return 0;
}

}  // extern C
