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
  if ((r = open(s.data(), O_RDONLY)) == -1) {
    status = IOError(s, errno);
  }
  if (status.ok()) {
    if ((w = open(t.data(), O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1) {
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

class PosixEnv : public Env {
 public:
  PosixEnv();

  virtual ~PosixEnv() { assert(false); }

  virtual Status NewSequentialFile(const Slice& fname,
                                   SequentialFile** result) {
    FILE* f = fopen(fname.data(), "r");
    if (f == NULL) {
      *result = NULL;
      if (errno == ENOENT) {
        return Status::NotFound(fname);
      } else {
        return IOError(fname, errno);
      }
    } else {
      *result = new PosixSequentialFile(fname, f);
      return Status::OK();
    }
  }

  virtual Status NewRandomAccessFile(const Slice& fname,
                                     RandomAccessFile** result) {
    *result = NULL;
    Status s;
    int fd = open(fname.data(), O_RDONLY);
    if (fd < 0) {
      if (errno == ENOENT) {
        s = Status::NotFound(fname);
      } else {
        s = IOError(fname, errno);
      }
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
    Status s;
    FILE* f = fopen(fname.data(), "w");
    if (f == NULL) {
      *result = NULL;
      s = IOError(fname, errno);
    } else {
      *result = new PosixWritableFile(fname, f);
    }
    return s;
  }

  virtual bool FileExists(const Slice& fname) {
    return access(fname.data(), F_OK) == 0;
  }

  virtual Status GetChildren(const Slice& dir,
                             std::vector<std::string>* result) {
    result->clear();
    DIR* d = opendir(dir.data());
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
    if (unlink(fname.data()) != 0) {
      if (errno == ENOENT) {
        result = Status::NotFound(fname);
      } else {
        result = IOError(fname, errno);
      }
    }
    return result;
  }

  virtual Status CreateDir(const Slice& dirname) {
    Status result;
    if (mkdir(dirname.data(), 0755) != 0) {
      result = IOError(dirname, errno);
    }
    return result;
  }

  virtual Status DeleteDir(const Slice& dirname) {
    Status result;
    if (rmdir(dirname.data()) != 0) {
      result = IOError(dirname, errno);
    }
    return result;
  }

  virtual Status GetFileSize(const Slice& fname, uint64_t* size) {
    Status s;
    struct stat sbuf;
    if (stat(fname.data(), &sbuf) != 0) {
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
    if ((r = open(src.data(), O_RDONLY)) == -1) {
      status = IOError(src, errno);
    }
    if (status.ok()) {
      if ((w = open(target.data(), O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1) {
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
    if (rename(src.data(), target.data()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const Slice& fname, FileLock** lock) {
    *lock = NULL;
    Status s;
    int fd = open(fname.data(), O_RDWR | O_CREAT, 0644);
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

  virtual void Schedule(void (*function)(void*), void* arg);

  virtual void StartThread(void (*function)(void* arg), void* arg);

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

  static uint64_t FetchTid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixLogger(f, &PosixEnv::FetchTid);
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
  // BGThread() is the body of the background thread
  void BGThread();

  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PosixEnv*>(arg)->BGThread();
    return NULL;
  }

  pthread_mutex_t mu_;
  pthread_cond_t bgsignal_;
  pthread_t bgthread_;
  bool started_bgthread_;

  // Entry per Schedule() call
  struct BGItem {
    void* arg;
    void (*function)(void*);
  };
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_;

  PosixLockTable locks_;
  MmapLimiter mmap_limit_;
};

PosixEnv::PosixEnv() : started_bgthread_(false) {
  port::PthreadCall("pthread_mutex_init", pthread_mutex_init(&mu_, NULL));
  port::PthreadCall("pthread_cond_init", pthread_cond_init(&bgsignal_, NULL));
}

void PosixEnv::Schedule(void (*function)(void*), void* arg) {
  port::PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mu_));

  // Start background thread if necessary
  if (!started_bgthread_) {
    started_bgthread_ = true;
    port::PthreadCall(
        "pthread_create",
        pthread_create(&bgthread_, NULL, &PosixEnv::BGThreadWrapper, this));
  }

  // If the queue is currently empty, the background thread may currently be
  // waiting.
  if (queue_.empty()) {
    port::PthreadCall("pthread_cond_signal", pthread_cond_signal(&bgsignal_));
  }

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;

  port::PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mu_));
}

void PosixEnv::BGThread() {
  while (true) {
    // Wait until there is an item that is ready to run
    port::PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mu_));
    while (queue_.empty()) {
      port::PthreadCall("pthread_cond_wait",
                        pthread_cond_wait(&bgsignal_, &mu_));
    }

    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop_front();

    port::PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mu_));
    (*function)(arg);
  }
}

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

void PosixEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  port::PthreadCall("pthread_create",
                    pthread_create(&t, NULL, &StartThreadWrapper, state));
}

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* posix_env;

static void InitGlobalPosixEnv() { posix_env = new PosixEnv; }

Env* Env::Default() {
  pthread_once(&once, &InitGlobalPosixEnv);
  return posix_env;
}

}  // namespace pdlfs
