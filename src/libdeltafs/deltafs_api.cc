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
#include "pdlfs-common/port.h"
#include "pdlfs-common/status.h"
#include "pdlfs-common/xxhash.h"

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

typedef DELTAFS_PLFSDIR Dir;

// Dir options
typedef pdlfs::plfsio::DirOptions DirOptions;
// Dir writer
typedef pdlfs::plfsio::Writer Writer;

static inline pdlfs::Env* DirEnv() {
  // Avoid double-buffering
  return pdlfs::port::posix::GetUnBufferedIOEnv();
}

static DirOptions ParseOptions(const char* conf) {
  DirOptions options = pdlfs::plfsio::ParseDirOptions(conf);
  options.compaction_pool = NULL;
  options.env = DirEnv();
  return options;
}

Dir* deltafs_plfsdir_create(const char* __name, const char* __conf) {
  pdlfs::Status s;
  Writer* writer = NULL;
  s = Writer::Open(ParseOptions(__conf), __name, &writer);

  if (s.ok()) {
    assert(writer != NULL);
    return reinterpret_cast<Dir*>(writer);
  } else {
    SetErrno(s);
    return NULL;
  }
}

int deltafs_plfsdir_append(Dir* __dir, const char* __fname, const void* __buf,
                           size_t __sz) {
  pdlfs::Status s;

  if (__dir != NULL && __fname != NULL) {
    char tmp[8];
    uint64_t h = pdlfs::xxhash64(__fname, strlen(__fname), 0);
    pdlfs::EncodeFixed64(tmp, h);
    pdlfs::Slice k(tmp, sizeof(tmp));
    s = reinterpret_cast<Writer*>(__dir)->Append(
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

int deltafs_plfsdir_epoch_flush(Dir* __dir, void* __arg) {
  pdlfs::Status s;

  if (__dir != NULL) {
    s = reinterpret_cast<Writer*>(__dir)->MakeEpoch();
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

int deltafs_plfsdir_close(Dir* __dir) {
  pdlfs::Status s;

  if (__dir != NULL) {
    Writer* writer = reinterpret_cast<Writer*>(__dir);
    s = writer->Finish();
    delete writer;
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

}  // anonymous namespace

}  // extern C
