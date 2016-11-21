/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <dirent.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <deque>

#include "posix_env.h"
#include "posix_logger.h"
#include "posix_sock.h"

namespace pdlfs {

#if defined(PDLFS_OS_LINUX) && defined(_GNU_SOURCE)
static Status OSCopyFile(const Slice& s, const Slice& t) {
  Status status;
  int r = -1;
  int w = -1;
  if ((r = open(s.c_str(), O_RDONLY)) == -1) {
    status = IOError(s, errno);
  }
  if (status.ok()) {
    if ((w = open(t.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1) {
      status = IOError(t, errno);
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
  MmapLimiter() { SetAllowed(sizeof(void*) >= 8 ? 1000 : 0); }

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
  PosixMmapReadableFile(const Slice& fname, void* base, size_t length,
                        MmapLimiter* limiter)
      : filename_(fname.ToString()),
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
  PosixFixedThreadPool(int size)
      : bg_cv_(&mu_),
        started_threads_(0),
        max_threads_(size),
        shutting_down_(NULL) {}

  virtual ~PosixFixedThreadPool();
  virtual void Schedule(void (*function)(void* arg), void* arg);
  virtual std::string ToDebugString();
  void StartThread(void (*function)(void* arg), void* arg);

 private:
  // BGThread() is the body of the background thread
  void BGThread();

  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PosixFixedThreadPool*>(arg)->BGThread();
    return NULL;
  }

  port::Mutex mu_;
  port::CondVar bg_cv_;
  int started_threads_;
  int max_threads_;

  port::AtomicPointer shutting_down_;
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
  explicit PosixEnv() : pool_(1) {}
  virtual ~PosixEnv() { assert(false); }

  virtual Status NewSequentialFile(const Slice& fname,
                                   SequentialFile** result) {
    FILE* f = fopen(fname.c_str(), "r");
    if (f != NULL) {
      *result = new PosixBufferedSequentialFile(fname, f);
      return Status::OK();
    } else {
      *result = NULL;
      return IOError(fname, errno);
    }
  }

  virtual Status NewRandomAccessFile(const Slice& fname,
                                     RandomAccessFile** result) {
    *result = NULL;
    Status s;
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd < 0) {
      s = IOError(fname, errno);
    } else if (mmap_limit_.Acquire()) {
      uint64_t size;
      s = GetFileSize(fname, &size);
      if (s.ok()) {
        void* base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
        if (base != MAP_FAILED) {
          *result = new PosixMmapReadableFile(fname, base, size, &mmap_limit_);
        } else {
          s = IOError(fname, errno);
        }
      }
      close(fd);
      if (!s.ok()) {
        mmap_limit_.Release();
      }
    } else {
      *result = new PosixRandomAccessFile(fname, fd);
    }
    return s;
  }

  virtual Status NewWritableFile(const Slice& fname, WritableFile** result) {
    FILE* f = fopen(fname.c_str(), "w");
    if (f != NULL) {
      *result = new PosixBufferedWritableFile(fname, f);
      return Status::OK();
    } else {
      *result = NULL;
      return IOError(fname, errno);
    }
  }

  virtual bool FileExists(const Slice& fname) {
    return access(fname.c_str(), F_OK) == 0;
  }

  virtual Status GetChildren(const Slice& dir,
                             std::vector<std::string>* result) {
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == NULL) {
      return IOError(dir, errno);
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL) {
      result->push_back(entry->d_name);
    }
    closedir(d);
    return Status::OK();
  }

  virtual Status DeleteFile(const Slice& fname) {
    Status result;
    if (unlink(fname.c_str()) != 0) {
      result = IOError(fname, errno);
    }
    return result;
  }

  virtual Status CreateDir(const Slice& dirname) {
    Status result;
    if (mkdir(dirname.c_str(), 0755) != 0) {
      result = IOError(dirname, errno);
    }
    return result;
  }

  virtual Status AttachDir(const Slice& dirname) {
    Status result;
    DIR* dir = opendir(dirname.c_str());
    if (dir == NULL) {
      result = IOError(dirname, errno);
    } else {
      closedir(dir);
    }
    return result;
  }

  virtual Status DeleteDir(const Slice& dirname) {
    Status result;
    if (rmdir(dirname.c_str()) != 0) {
      result = IOError(dirname, errno);
    }
    return result;
  }

  virtual Status DetachDir(const Slice& dirname) {
    return Status::NotSupported(Slice());
  }

  virtual Status GetFileSize(const Slice& fname, uint64_t* size) {
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      s = IOError(fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
  }

  virtual Status CopyFile(const Slice& src, const Slice& target) {
#if defined(PDLFS_OS_LINUX) && defined(_GNU_SOURCE)
    return OSCopyFile(src, target);
#else
    Status status;
    int r = -1;
    int w = -1;
    if ((r = open(src.c_str(), O_RDONLY)) == -1) {
      status = IOError(src, errno);
    }
    if (status.ok()) {
      if ((w = open(target.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644)) ==
          -1) {
        status = IOError(target, errno);
      }
    }
    if (status.ok()) {
      ssize_t n;
      char buf[4096];
      while (status.ok() && (n = read(r, buf, 4096)) > 0) {
        if (write(w, buf, n) != n) {
          status = IOError(target, errno);
        }
      }
      if (status.ok() && n == -1) {
        status = IOError(src, errno);
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

  virtual Status RenameFile(const Slice& src, const Slice& target) {
    Status result;
    if (rename(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const Slice& fname, FileLock** lock) {
    *lock = NULL;
    Status s;
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      s = IOError(fname, errno);
    } else if (!locks_.Insert(fname)) {
      close(fd);
      s = Status::IOError(fname, "lock already held by process");
    } else if (LockOrUnlock(fd, true) == -1) {
      s = IOError(fname, errno);
      close(fd);
      locks_.Remove(fname);
    } else {
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      my_lock->name_ = fname.ToString();
      *lock = my_lock;
    }
    return s;
  }

  virtual Status UnlockFile(FileLock* lock) {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status s;
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      s = IOError("unlock", errno);
    }
    locks_.Remove(my_lock->name_);
    close(my_lock->fd_);
    delete my_lock;
    return s;
  }

  virtual void Schedule(void (*function)(void*), void* arg) {
    pool_.Schedule(function, arg);
  }

  virtual void StartThread(void (*function)(void* arg), void* arg) {
    pool_.StartThread(function, arg);
  }

  virtual Status GetTestDirectory(std::string* result) {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/pdlfs-test-%d",
               static_cast<int>(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixLogger(f, port::PthreadId);
      return Status::OK();
    }
  }

  virtual uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual void SleepForMicroseconds(int micros) { usleep(micros); }

  virtual Status FetchHostname(std::string* hostname) {
    char buf[HOST_NAME_MAX];
    Status s;
    if (gethostname(buf, HOST_NAME_MAX) == -1) {
      s = IOError("gethostname", errno);
    } else {
      *hostname = buf;
    }
    return s;
  }

  virtual Status FetchHostIPAddrs(std::vector<std::string>* ips) {
    PosixSock sock;
    Status s = sock.OpenSocket();
    if (s.ok()) {
      s = sock.LoadSocketConfig();
    }
    if (s.ok()) {
      s = sock.GetHostIPAddresses(ips);
    }
    return s;
  }

 private:
  PosixFixedThreadPool pool_;
  PosixLockTable locks_;
  MmapLimiter mmap_limit_;
};

class PosixUnBufferedIOWrapper : public EnvWrapper {
 public:
  PosixUnBufferedIOWrapper(Env* base) : EnvWrapper(base) {}

  virtual ~PosixUnBufferedIOWrapper() {}

  virtual Status NewWritableFile(const Slice& fname, WritableFile** result) {
    int fd = open(fname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, DEFFILEMODE);
    if (fd != -1) {
      *result = new PosixWritableFile(fname, fd);
      return Status::OK();
    } else {
      *result = NULL;
      return IOError(fname, errno);
    }
  }

  virtual Status NewRandomAccessFile(const Slice& fname,
                                     RandomAccessFile** result) {
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd != -1) {
      *result = new PosixRandomAccessFile(fname, fd);
      return Status::OK();
    } else {
      *result = NULL;
      return IOError(fname, errno);
    }
  }

  virtual Status NewSequentialFile(const Slice& fname,
                                   SequentialFile** result) {
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd != -1) {
      *result = new PosixSequentialFile(fname, fd);
      return Status::OK();
    } else {
      *result = NULL;
      return IOError(fname, errno);
    }
  }
};

static pthread_t PthreadCreate(void* (*start_routine)(void*), void* arg) {
  pthread_t new_th;
  port::PthreadCall("pthread_create",
                    pthread_create(&new_th, NULL, start_routine, arg));
  port::PthreadCall("pthread_detach", pthread_detach(new_th));
  return new_th;
}

std::string PosixFixedThreadPool::ToDebugString() {
  char tmp[100];
  snprintf(tmp, sizeof(tmp), "POSIX fixed thread pool: max_thread=%d",
           max_threads_);
  return tmp;
}

PosixFixedThreadPool::~PosixFixedThreadPool() {
  mu_.Lock();
  shutting_down_.Release_Store(this);
  bg_cv_.SignalAll();
  while (started_threads_ != 0) {
    bg_cv_.Wait();
  }
  mu_.Unlock();
}

void PosixFixedThreadPool::Schedule(void (*function)(void*), void* arg) {
  if (!shutting_down_.Acquire_Load()) {
    mu_.Lock();

    // Start background threads if necessary
    while (started_threads_ < max_threads_) {
      assert(started_threads_ >= 0);
      started_threads_++;
      PthreadCreate(BGThreadWrapper, this);
    }

    // If the queue is currently empty, the background thread may currently be
    // waiting.
    if (queue_.empty()) {
      bg_cv_.SignalAll();
    }

    // Add to priority queue
    queue_.push_back(BGItem());
    queue_.back().function = function;
    queue_.back().arg = arg;

    mu_.Unlock();
  }
}

void PosixFixedThreadPool::BGThread() {
  void (*function)(void*) = NULL;
  void* arg;

  while (true) {
    {
      MutexLock l(&mu_);
      // Wait until there is an item that is ready to run
      while (queue_.empty() && !shutting_down_.Acquire_Load()) {
        bg_cv_.Wait();
      }
      if (shutting_down_.Acquire_Load()) {
        started_threads_--;
        assert(started_threads_ >= 0);
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
    function = NULL;
  }
}

void PosixFixedThreadPool::StartThread(void (*function)(void* arg), void* arg) {
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCreate(StartThreadWrapper, state);
}

ThreadPool* ThreadPool::NewFixed(int num_threads) {
  return new PosixFixedThreadPool(num_threads);
}

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* posix_unbufio;
static Env* posix_env;

static void InitGlobalPosixEnvs() {
  Env* base = new PosixEnv;
  posix_unbufio = new PosixUnBufferedIOWrapper(base);
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
}  // namespace posix
}  // namespace port

Env* Env::Default() {
#if !defined(PDLFS_PLATFORM_POSIX)
#error "This code should not compile"
#else
  Env* result = port::posix::GetDefaultEnv();
  assert(result != NULL);
  return result;
#endif
}

}  // namespace pdlfs
