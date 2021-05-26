/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */
#include "posix_env.h"

#include "posix_bgrun.h"
#include "posix_fastcopy.h"
#include "posix_filecopy.h"
#include "posix_logger.h"
#include "posix_mmap.h"

#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <sys/stat.h>

#if __cplusplus >= 201103L
#define OVERRIDE override
#else
#define OVERRIDE
#endif

namespace pdlfs {

PosixRandomAccessFile::~PosixRandomAccessFile() { close(fd_); }

PosixBufferedSequentialFile::~PosixBufferedSequentialFile() { fclose(file_); }

PosixSequentialFile::~PosixSequentialFile() { close(fd_); }

class PosixEnv : public Env {
 public:
  explicit PosixEnv(int bg_threads = 1) : tpool_(bg_threads) {}
  virtual ~PosixEnv() {}

  virtual Status NewWritableFile(const char* fname, WritableFile** r) OVERRIDE {
    int fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd != -1) {
      *r = new PosixWritableFile(fname, fd);
      return Status::OK();
    } else {
      *r = NULL;
      return PosixError(fname, errno);
    }
  }

  virtual Status NewSequentialFile(  ///
      const char* fname, SequentialFile** r) OVERRIDE {
    int fd = open(fname, O_RDONLY);
    if (fd != -1) {
      *r = new PosixSequentialFile(fname, fd);
      return Status::OK();
    } else {
      *r = NULL;
      return PosixError(fname, errno);
    }
  }

  virtual Status NewRandomAccessFile(  ///
      const char* fname, RandomAccessFile** r) OVERRIDE {
    int fd = open(fname, O_RDONLY);
    if (fd != -1) {
      *r = new PosixRandomAccessFile(fname, fd);
      return Status::OK();
    } else {
      *r = NULL;
      return PosixError(fname, errno);
    }
  }

  virtual bool FileExists(const char* fname) OVERRIDE {
    return access(fname, F_OK) == 0;
  }

  virtual Status GetChildren(  ///
      const char* dirname, std::vector<std::string>* result) OVERRIDE {
    result->clear();
    DIR* dir = opendir(dirname);
    if (dir != NULL) {
      struct dirent* entry;
      while ((entry = readdir(dir)) != NULL) {
        result->push_back(static_cast<const char*>(entry->d_name));
      }
      closedir(dir);
      return Status::OK();
    } else {
      return PosixError(dirname, errno);
    }
  }

  virtual Status DeleteFile(const char* fname) OVERRIDE {
    Status result;
    if (unlink(fname) != 0) {
      result = PosixError(fname, errno);
    }
    return result;
  }

  virtual Status CreateDir(const char* dirname) OVERRIDE {
    Status result;
    if (mkdir(dirname, 0755) != 0) {
      result = PosixError(dirname, errno);
    }
    return result;
  }

  virtual Status AttachDir(const char* dirname) OVERRIDE {
    Status result;
    DIR* dir = opendir(dirname);
    if (dir == NULL) {
      result = PosixError(dirname, errno);
    } else {
      closedir(dir);
    }
    return result;
  }

  virtual Status DeleteDir(const char* dirname) OVERRIDE {
    Status result;
    if (rmdir(dirname) != 0) {
      result = PosixError(dirname, errno);
    }
    return result;
  }

  virtual Status DetachDir(const char* dirname) OVERRIDE {
    return Status::NotSupported(Slice());
  }

  virtual Status GetFileSize(const char* fname, uint64_t* size) OVERRIDE {
    Status s;
    struct stat sbuf;
    if (stat(fname, &sbuf) == 0) {
      *size = static_cast<uint64_t>(sbuf.st_size);
    } else {
      s = PosixError(fname, errno);
      *size = 0;
    }
    return s;
  }

  virtual Status CopyFile(const char* src, const char* dst) OVERRIDE {
#if defined(PDLFS_OS_LINUX)
    return FastCopy(src, dst);
#else
    return Copy(src, dst);
#endif
  }

  virtual Status RenameFile(const char* src, const char* dst) OVERRIDE {
    Status result;
    if (rename(src, dst) != 0) {
      result = PosixError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const char* fname, FileLock** lock) OVERRIDE {
    *lock = NULL;
    Status s;
    int fd = open(fname, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      s = PosixError(fname, errno);
    } else if (!locks_.Insert(fname)) {
      close(fd);
      s = Status::IOError(fname, "Lock already held by process");
    } else if (LockOrUnlock(fd, true) == -1) {
      s = PosixError(fname, errno);
      close(fd);
      locks_.Remove(fname);
    } else {
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->name_ = fname;
      my_lock->fd_ = fd;
      *lock = my_lock;
    }
    return s;
  }

  virtual Status UnlockFile(FileLock* lock) OVERRIDE {
    Status s;
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      s = PosixError(my_lock->name_, errno);
    }
    locks_.Remove(my_lock->name_);
    close(my_lock->fd_);
    delete my_lock;
    return s;
  }

  virtual void Schedule(void (*function)(void*), void* arg) OVERRIDE {
    tpool_.Schedule(function, arg);
  }

  virtual void StartThread(void (*function)(void*), void* arg) OVERRIDE {
    tpool_.StartThread(function, arg);
  }

  virtual Status GetTestDirectory(std::string* result) OVERRIDE {
    const char* env = getenv("TEST_TMPDIR");
    if (env == NULL || env[0] == '\0') {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/pdlfs-test-%d",
               static_cast<int>(geteuid()));
      *result = buf;
    } else {
      *result = env;
    }
    // Ignore error since directory may exist
    CreateDir(result->c_str());
    return Status::OK();
  }

  virtual Status NewLogger(const char* fname, Logger** result) OVERRIDE {
    FILE* f = fopen(fname, "w");
    if (f != NULL) {
      *result = new PosixLogger(f, port::PthreadId);
      return Status::OK();
    } else {
      *result = NULL;
      return PosixError(fname, errno);
    }
  }

 private:
  PosixThreadPool tpool_;
  LockTable locks_;
};

class PosixLibcBufferedIoEnvWrapper : public EnvWrapper {
 public:
  explicit PosixLibcBufferedIoEnvWrapper(Env* base) : EnvWrapper(base) {}
  virtual ~PosixLibcBufferedIoEnvWrapper() {}

  virtual Status NewWritableFile(const char* fname, WritableFile** r) OVERRIDE {
    FILE* f = fopen(fname, "w");
    if (f != NULL) {
      *r = new PosixBufferedWritableFile(fname, f);
      return Status::OK();
    } else {
      *r = NULL;
      return PosixError(fname, errno);
    }
  }

  virtual Status NewSequentialFile(  ///
      const char* fname, SequentialFile** r) OVERRIDE {
    FILE* f = fopen(fname, "r");
    if (f != NULL) {
      *r = new PosixBufferedSequentialFile(fname, f);
      return Status::OK();
    } else {
      *r = NULL;
      return PosixError(fname, errno);
    }
  }
};

class PosixMmapIoEnvWrapper : public EnvWrapper {
 public:
  explicit PosixMmapIoEnvWrapper(Env* base) : EnvWrapper(base) {}
  virtual ~PosixMmapIoEnvWrapper() {}

  virtual Status NewRandomAccessFile(  ///
      const char* fname, RandomAccessFile** r) OVERRIDE {
    *r = NULL;
    Status s;
    int fd = open(fname, O_RDONLY);
    if (fd < 0) {
      s = PosixError(fname, errno);
    } else if (!mmap_limit_.Acquire()) {
      *r = new PosixRandomAccessFile(fname, fd);
    } else {
      uint64_t size;
      s = target()->GetFileSize(fname, &size);
      if (s.ok()) {
        if (size != 0) {
          void* base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
          if (base != MAP_FAILED) {
            *r = new PosixMmapReadableFile(fname, base, size, &mmap_limit_);
          } else {
            s = PosixError(fname, errno);
          }
        } else {
          *r = new PosixEmptyFile();
        }
      }
      close(fd);
      if (!s.ok()) {
        mmap_limit_.Release();
      }
    }
    return s;
  }

 private:
  MmapLimiter mmap_limit_;
};

Env* Env::NewBufferedIoEnvWrapper(Env* const base) {
  return new PosixLibcBufferedIoEnvWrapper(base);
}

Env* Env::NewMmapIoEnvWrapper(Env* const base) {
  return new PosixMmapIoEnvWrapper(base);
}

static pthread_once_t once = PTHREAD_ONCE_INIT;

static Env* posix_env_wrapped;
static Env* posix_env;

static void InitEnvs() {
  Env* const base = new PosixEnv;
  posix_env_wrapped =
      new PosixMmapIoEnvWrapper(new PosixLibcBufferedIoEnvWrapper(base));
  posix_env = base;
}

static Env* PosixGetUnBufferedIoEnv() {
  port::PthreadCall("pthread_once", pthread_once(&once, InitEnvs));
  return posix_env;
}

static Env* PosixGetDefaultEnv() {
  port::PthreadCall("pthread_once", pthread_once(&once, InitEnvs));
  return posix_env_wrapped;
}

// Return the current time in microseconds.
uint64_t CurrentMicros() {
  uint64_t result;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  result = static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  return result;
}

// Sleep for a certain amount of microseconds.
// We may sleep a bit longer than the specified amount.
void SleepForMicroseconds(int micros) { usleep(static_cast<unsigned>(micros)); }

// Map integer errors to status objects.
Status PosixError(const Slice& err_context, int err_number) {
  switch (err_number) {
    case EEXIST:
      return Status::AlreadyExists(err_context);
    case ENOENT:
      return Status::NotFound(err_context);
    case EACCES:
      return Status::AccessDenied(err_context);
    default:
      return Status::IOError(err_context, strerror(err_number));
  }
}

// Return the base Env implemented using the standard os io calls.
// The returned result belongs to the system.
Env* Env::GetUnBufferedIoEnv() {
  Env* result = PosixGetUnBufferedIoEnv();
  return result;
}

// Return the default Env for common use.
// The returned result belongs to the system.
Env* Env::Default() {
  Env* result = PosixGetDefaultEnv();
  return result;
}

}  // namespace pdlfs
