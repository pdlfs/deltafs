/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "posix_env.h"
#include "posix_logger.h"
#include "posix_sock.h"

#include <dirent.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <deque>

namespace pdlfs {

#if defined(PDLFS_OS_LINUX) && defined(_GNU_SOURCE)
static Status OSCopyFile(const char* src, const char* dst) {
  Status status;
  int r = -1;
  int w = -1;
  if ((r = open(src, O_RDONLY)) == -1) {
    status = IOError(src, errno);
  }
  if (status.ok()) {
    if ((w = open(dst, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1) {
      status = IOError(dst, errno);
    }
  }
  if (status.ok()) {
    int p[2];
    if (pipe(p) == -1) {
      status = IOError("pipe", errno);
    } else {
      const size_t batch_size = 4096;
      while (splice(p[0], 0, w, 0, splice(r, 0, p[1], 0, batch_size, 0), 0) > 0)
        ;
      close(p[0]);
      close(p[1]);
    }
  }
  if (r != -1) {
    close(r);
  }
  if (w != -1) {
    close(w);
  }
  return status;
}
#endif

// Helper class to limit mmap file usage so that we do not end up
// running out virtual memory or running into kernel performance
// problems for very large databases.
class MmapLimiter {
 public:
  // Up to 1000 mmaps for 64-bit binaries; none for smaller pointer sizes.
  MmapLimiter() {
    MutexLock l(&mu_);
    SetAllowed(sizeof(void*) >= 8 ? 1000 : 0);
  }

  // If another mmap slot is available, acquire it and return true.
  // Else return false.
  bool Acquire() {
    if (GetAllowed() <= 0) {
      return false;
    }
    MutexLock l(&mu_);
    intptr_t x = GetAllowed();
    if (x <= 0) {
      return false;
    } else {
      SetAllowed(x - 1);
      return true;
    }
  }

  // Release a slot acquired by a previous call to Acquire() that returned true.
  void Release() {
    MutexLock l(&mu_);
    SetAllowed(GetAllowed() + 1);
  }

 private:
  port::Mutex mu_;
  port::AtomicPointer allowed_;

  intptr_t GetAllowed() const {
    return reinterpret_cast<intptr_t>(allowed_.Acquire_Load());
  }

  void SetAllowed(intptr_t v) {
    mu_.AssertHeld();
    allowed_.Release_Store(reinterpret_cast<void*>(v));
  }

  MmapLimiter(const MmapLimiter&);
  void operator=(const MmapLimiter&);
};

class PosixMmapReadableFile : public RandomAccessFile {
 private:
  std::string filename_;
  void* mmapped_region_;
  size_t length_;
  MmapLimiter* limiter_;

 public:
  PosixMmapReadableFile(const char* fname, void* base, size_t length,
                        MmapLimiter* limiter)
      : filename_(fname),
        mmapped_region_(base),
        length_(length),
        limiter_(limiter) {}

  virtual ~PosixMmapReadableFile() {
    munmap(mmapped_region_, length_);
    limiter_->Release();
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    if (offset + n > length_) {
      *result = Slice();
      s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
    }
    return s;
  }
};

class PosixFixedThreadPool : public ThreadPool {
 public:
  explicit PosixFixedThreadPool(int max_threads, bool eager_init = false,
                                void* attr = NULL)
      : bg_cv_(&mu_),
        num_pool_threads_(0),
        max_threads_(max_threads),
        shutting_down_(false),
        paused_(false) {
    if (eager_init) {
      // Create pool threads immediately
      MutexLock ml(&mu_);
      InitPool(attr);
    }
  }

  virtual ~PosixFixedThreadPool();
  virtual void Schedule(void (*function)(void*), void* arg);
  virtual std::string ToDebugString();
  virtual void Resume();
  virtual void Pause();

  void StartThread(void (*function)(void*), void* arg);
  void InitPool(void* attr);

 private:
  // BGThread() is the body of the background thread
  void BGThread();

  static void* BGWrapper(void* arg) {
    reinterpret_cast<PosixFixedThreadPool*>(arg)->BGThread();
    return NULL;
  }

  port::Mutex mu_;
  port::CondVar bg_cv_;
  int num_pool_threads_;
  int max_threads_;

  bool shutting_down_;
  bool paused_;

  // Entry per Schedule() call
  struct BGItem {
    void* arg;
    void (*function)(void*);
  };
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_;

  struct StartThreadState {
    void (*user_function)(void*);
    void* arg;
  };

  static void* StartThreadWrapper(void* arg) {
    StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
    state->user_function(state->arg);
    delete state;
    return NULL;
  }
};

class PosixEnv : public Env {
 public:
  explicit PosixEnv(int bg_threads = 1) : pool_(bg_threads) {}
  virtual ~PosixEnv() { abort(); }

  virtual Status NewSequentialFile(const char* fname, SequentialFile** r) {
    FILE* f = fopen(fname, "r");
    if (f != NULL) {
      *r = new PosixBufferedSequentialFile(fname, f);
      return Status::OK();
    } else {
      *r = NULL;
      return IOError(fname, errno);
    }
  }

  virtual Status NewRandomAccessFile(const char* fname, RandomAccessFile** r) {
    *r = NULL;
    Status s;
    int fd = open(fname, O_RDONLY);
    if (fd < 0) {
      s = IOError(fname, errno);
    } else if (!mmap_limit_.Acquire()) {
      *r = new PosixRandomAccessFile(fname, fd);
    } else {
      uint64_t size;
      s = GetFileSize(fname, &size);
      if (s.ok()) {
        if (size != 0) {
          void* base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
          if (base != MAP_FAILED) {
            *r = new PosixMmapReadableFile(fname, base, size, &mmap_limit_);
          } else {
            s = IOError(fname, errno);
          }
        } else {
          s = Status::NotSupported(Slice());
        }
      }
      close(fd);
      if (!s.ok()) {
        mmap_limit_.Release();
      }
    }
    return s;
  }

  virtual Status NewWritableFile(const char* fname, WritableFile** r) {
    FILE* f = fopen(fname, "w");
    if (f != NULL) {
      *r = new PosixBufferedWritableFile(fname, f);
      return Status::OK();
    } else {
      *r = NULL;
      return IOError(fname, errno);
    }
  }

  virtual bool FileExists(const char* fname) {
    return access(fname, F_OK) == 0;
  }

  virtual Status GetChildren(const char* dirname,
                             std::vector<std::string>* result) {
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
      return IOError(dirname, errno);
    }
  }

  virtual Status DeleteFile(const char* fname) {
    Status result;
    if (unlink(fname) != 0) {
      result = IOError(fname, errno);
    }
    return result;
  }

  virtual Status CreateDir(const char* dirname) {
    Status result;
    if (mkdir(dirname, 0755) != 0) {
      result = IOError(dirname, errno);
    }
    return result;
  }

  virtual Status AttachDir(const char* dirname) {
    Status result;
    DIR* dir = opendir(dirname);
    if (dir == NULL) {
      result = IOError(dirname, errno);
    } else {
      closedir(dir);
    }
    return result;
  }

  virtual Status DeleteDir(const char* dirname) {
    Status result;
    if (rmdir(dirname) != 0) {
      result = IOError(dirname, errno);
    }
    return result;
  }

  virtual Status DetachDir(const char* dirname) {
    return Status::NotSupported(Slice());
  }

  virtual Status GetFileSize(const char* fname, uint64_t* size) {
    Status s;
    struct stat sbuf;
    if (stat(fname, &sbuf) == 0) {
      *size = static_cast<uint64_t>(sbuf.st_size);
    } else {
      s = IOError(fname, errno);
      *size = 0;
    }
    return s;
  }

  virtual Status CopyFile(const char* src, const char* dst) {
#if defined(PDLFS_OS_LINUX) && defined(_GNU_SOURCE)
    return OSCopyFile(src, dst);
#else
    Status status;
    int r = -1;
    int w = -1;
    if ((r = open(src, O_RDONLY)) == -1) {
      status = IOError(src, errno);
    }
    if (status.ok()) {
      if ((w = open(dst, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1) {
        status = IOError(dst, errno);
      }
    }
    if (status.ok()) {
      ssize_t n;
      char buf[4096];
      while ((n = read(r, buf, 4096)) > 0) {
        ssize_t m = write(w, buf, n);
        if (m != n) {
          status = IOError(dst, errno);
          break;
        }
      }
      if (n == -1) {
        if (status.ok()) {
          status = IOError(src, errno);
        }
      }
    }
    if (r != -1) {
      close(r);
    }
    if (w != -1) {
      close(w);
    }
    return status;
#endif
  }

  virtual Status RenameFile(const char* src, const char* dst) {
    Status result;
    if (rename(src, dst) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const char* fname, FileLock** lock) {
    *lock = NULL;
    Status s;
    int fd = open(fname, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      s = IOError(fname, errno);
    } else if (!locks_.Insert(fname)) {
      close(fd);
      s = Status::IOError(fname, "Lock already held by process");
    } else if (LockOrUnlock(fd, true) == -1) {
      s = IOError(fname, errno);
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

  virtual Status UnlockFile(FileLock* lock) {
    Status s;
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      s = IOError("Unlock", errno);
    }
    locks_.Remove(my_lock->name_);
    close(my_lock->fd_);
    delete my_lock;
    return s;
  }

  virtual void Schedule(void (*function)(void*), void* arg) {
    pool_.Schedule(function, arg);
  }

  virtual void StartThread(void (*function)(void*), void* arg) {
    pool_.StartThread(function, arg);
  }

  virtual Status GetTestDirectory(std::string* result) {
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

  virtual Status NewLogger(const char* fname, Logger** result) {
    FILE* f = fopen(fname, "w");
    if (f != NULL) {
      *result = new PosixLogger(f, port::PthreadId);
      return Status::OK();
    } else {
      *result = NULL;
      return IOError(fname, errno);
    }
  }

  virtual uint64_t NowMicros() {
    uint64_t result;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    result = static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
    return result;
  }

  virtual void SleepForMicroseconds(int micros) {
    usleep(static_cast<unsigned>(micros));
  }

  virtual Status FetchHostname(std::string* hostname) {
    char buf[PDLFS_HOST_NAME_MAX];
    if (gethostname(buf, sizeof(buf)) == -1) {
      return IOError("Cannot get hostname", errno);
    } else {
      *hostname = buf;
      return Status::OK();
    }
  }

  virtual Status FetchHostIPAddrs(std::vector<std::string>* ips) {
    PosixSock sock;
    Status s = sock.OpenSocket();
    if (s.ok()) s = sock.LoadSocketConfig();
    if (s.ok()) s = sock.GetHostIPAddresses(ips);
    return s;
  }

 private:
  PosixFixedThreadPool pool_;
  PosixLockTable locks_;
  MmapLimiter mmap_limit_;
};

#if defined(PDLFS_OS_LINUX)
class PosixDirectIOWrapper : public EnvWrapper {
 public:
  explicit PosixDirectIOWrapper(Env* base) : EnvWrapper(base) {}
  virtual ~PosixDirectIOWrapper() { abort(); }

  virtual Status NewWritableFile(const char* fname, WritableFile** r) {
    int fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0644);
    if (fd != -1) {
      *r = new PosixWritableFile(fname, fd);
      return Status::OK();
    } else {
      *r = NULL;
      return IOError(fname, errno);
    }
  }

  virtual Status NewRandomAccessFile(const char* fname, RandomAccessFile** r) {
    int fd = open(fname, O_RDONLY);
    if (fd != -1) {
      *r = new PosixRandomAccessFile(fname, fd);
      return Status::OK();
    } else {
      *r = NULL;
      return IOError(fname, errno);
    }
  }

  virtual Status NewSequentialFile(const char* fname, SequentialFile** r) {
    int fd = open(fname, O_RDONLY);
    if (fd != -1) {
      *r = new PosixSequentialFile(fname, fd);
      return Status::OK();
    } else {
      *r = NULL;
      return IOError(fname, errno);
    }
  }
};
#endif

class PosixUnBufferedIOWrapper : public EnvWrapper {
 public:
  explicit PosixUnBufferedIOWrapper(Env* base) : EnvWrapper(base) {}
  virtual ~PosixUnBufferedIOWrapper() { abort(); }

  virtual Status NewWritableFile(const char* fname, WritableFile** r) {
    int fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd != -1) {
      *r = new PosixWritableFile(fname, fd);
      return Status::OK();
    } else {
      *r = NULL;
      return IOError(fname, errno);
    }
  }

  virtual Status NewRandomAccessFile(const char* fname, RandomAccessFile** r) {
    int fd = open(fname, O_RDONLY);
    if (fd != -1) {
      *r = new PosixRandomAccessFile(fname, fd);
      return Status::OK();
    } else {
      *r = NULL;
      return IOError(fname, errno);
    }
  }

  virtual Status NewSequentialFile(const char* fname, SequentialFile** r) {
    int fd = open(fname, O_RDONLY);
    if (fd != -1) {
      *r = new PosixSequentialFile(fname, fd);
      return Status::OK();
    } else {
      *r = NULL;
      return IOError(fname, errno);
    }
  }
};

static pthread_t Pthread(void* (*func)(void*), void* arg, void* attr) {
  pthread_t th;
  pthread_attr_t* ta = reinterpret_cast<pthread_attr_t*>(attr);
  port::PthreadCall("pthread_create", pthread_create(&th, ta, func, arg));
  port::PthreadCall("pthread_detach", pthread_detach(th));
  return th;
}

std::string PosixFixedThreadPool::ToDebugString() {
  char tmp[100];
  snprintf(tmp, sizeof(tmp), "POSIX fixed thread pool: num_threads=%d",
           max_threads_);
  return tmp;
}

PosixFixedThreadPool::~PosixFixedThreadPool() {
  mu_.Lock();
  shutting_down_ = true;
  bg_cv_.SignalAll();
  while (num_pool_threads_ != 0) {
    bg_cv_.Wait();
  }
  mu_.Unlock();
}

void PosixFixedThreadPool::InitPool(void* attr) {
  mu_.AssertHeld();
  while (num_pool_threads_ < max_threads_) {
    num_pool_threads_++;
    Pthread(BGWrapper, this, attr);
  }
}

void PosixFixedThreadPool::Schedule(void (*function)(void*), void* arg) {
  MutexLock ml(&mu_);
  if (shutting_down_) return;
  InitPool(NULL);  // Start background threads if necessary

  // If the queue is currently empty, the background threads
  // may be waiting.
  if (queue_.empty()) bg_cv_.SignalAll();

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;
}

void PosixFixedThreadPool::BGThread() {
  void (*function)(void*) = NULL;
  void* arg;

  while (true) {
    {
      MutexLock l(&mu_);
      // Wait until there is an item that is ready to run
      while (!shutting_down_ && (paused_ || queue_.empty())) {
        bg_cv_.Wait();
      }
      if (shutting_down_) {
        assert(num_pool_threads_ > 0);
        num_pool_threads_--;
        bg_cv_.SignalAll();
        return;
      }

      assert(!queue_.empty());
      function = queue_.front().function;
      arg = queue_.front().arg;
      queue_.pop_front();
    }

    assert(function != NULL);
    function(arg);
  }
}

void PosixFixedThreadPool::Resume() {
  MutexLock ml(&mu_);
  paused_ = false;
  bg_cv_.SignalAll();
}

void PosixFixedThreadPool::Pause() {
  MutexLock ml(&mu_);
  paused_ = true;
}

void PosixFixedThreadPool::StartThread(void (*function)(void*), void* arg) {
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  Pthread(StartThreadWrapper, state, NULL);
}

ThreadPool* ThreadPool::NewFixed(int num_threads, bool eager_init, void* attr) {
  return new PosixFixedThreadPool(num_threads, eager_init, attr);
}

static pthread_once_t once = PTHREAD_ONCE_INIT;

static Env* posix_dio;
static Env* posix_unbufio;
static Env* posix_env;

static void InitGlobalPosixEnvs() {
  Env* base = new PosixEnv;
  posix_unbufio = new PosixUnBufferedIOWrapper(base);
#if defined(PDLFS_OS_LINUX)
  posix_dio = new PosixDirectIOWrapper(base);
#else
  posix_dio = NULL;
#endif
  posix_env = base;
}

namespace port {
namespace posix {
Env* GetDefaultEnv() {
  pthread_once(&once, &InitGlobalPosixEnvs);
  return posix_env;
}

Env* GetUnBufferedIOEnv() {
  pthread_once(&once, &InitGlobalPosixEnvs);
  return posix_unbufio;
}

Env* GetDirectIOEnv() {
  pthread_once(&once, &InitGlobalPosixEnvs);
  return posix_dio;
}
}  // namespace posix
}  // namespace port

Env* Env::Default() {
#if !defined(PDLFS_PLATFORM_POSIX)
#error "!!! This code should not compile !!!"
#else
  Env* result = port::posix::GetDefaultEnv();
  assert(result != NULL);
  return result;
#endif
}

}  // namespace pdlfs
