#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/env.h"
#include "pdlfs-common/mutexlock.h"

namespace pdlfs {

// Delay initialization until the first time an Env function
// is called.  Implementation is thread safe.
class LazyInitEnv : public Env {
 public:
  LazyInitEnv(const std::string& env_name, const std::string& env_conf)
      : env_name_(env_name), env_conf_(env_conf), env_ok_(true), env_(NULL) {}

  virtual ~LazyInitEnv() {
    if (env_ != Env::Default()) {
      delete env_;
    }
  }

  virtual Status NewSequentialFile(const Slice& f, SequentialFile** r) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->NewSequentialFile(f, r);
    } else {
      return s;
    }
  }

  virtual Status NewRandomAccessFile(const Slice& f, RandomAccessFile** r) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->NewRandomAccessFile(f, r);
    } else {
      return s;
    }
  }

  virtual Status NewWritableFile(const Slice& f, WritableFile** r) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->NewWritableFile(f, r);
    } else {
      return s;
    }
  }

  virtual bool FileExists(const Slice& f) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->FileExists(f);
    } else {
      return false;
    }
  }

  virtual Status GetChildren(const Slice& d, std::vector<std::string>* r) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->GetChildren(d, r);
    } else {
      return s;
    }
  }

  virtual Status DeleteFile(const Slice& f) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->DeleteFile(f);
    } else {
      return s;
    }
  }

  virtual Status CreateDir(const Slice& d) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->CreateDir(d);
    } else {
      return s;
    }
  }

  virtual Status AttachDir(const Slice& d) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->AttachDir(d);
    } else {
      return s;
    }
  }

  virtual Status DeleteDir(const Slice& d) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->DeleteDir(d);
    } else {
      return s;
    }
  }

  virtual Status DetachDir(const Slice& d) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->DetachDir(d);
    } else {
      return s;
    }
  }

  virtual Status GetFileSize(const Slice& f, uint64_t* size) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->GetFileSize(f, size);
    } else {
      return s;
    }
  }

  virtual Status CopyFile(const Slice& src, const Slice& dst) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->CopyFile(src, dst);
    } else {
      return s;
    }
  }

  virtual Status RenameFile(const Slice& src, const Slice& dst) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->RenameFile(src, dst);
    } else {
      return s;
    }
  }

  virtual Status LockFile(const Slice& f, FileLock** l) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->LockFile(f, l);
    } else {
      return s;
    }
  }

  virtual Status UnlockFile(FileLock* l) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->UnlockFile(l);
    } else {
      return s;
    }
  }

  virtual void Schedule(void (*f)(void*), void* a) {
    return Env::Default()->Schedule(f, a);
  }

  virtual void StartThread(void (*f)(void*), void* a) {
    return Env::Default()->StartThread(f, a);
  }

  virtual Status GetTestDirectory(std::string* path) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->GetTestDirectory(path);
    } else {
      return s;
    }
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->NewLogger(fname, result);
    } else {
      return s;
    }
  }

  virtual uint64_t NowMicros() { return Env::Default()->NowMicros(); }

  virtual void SleepForMicroseconds(int micros) {
    Env::Default()->SleepForMicroseconds(micros);
  }

  virtual Status FetchHostname(std::string* hostname) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->FetchHostname(hostname);
    } else {
      return s;
    }
  }

  virtual Status FetchHostIPAddrs(std::vector<std::string>* ips) {
    Status s = OpenEnv();
    if (s.ok()) {
      return env_->FetchHostIPAddrs(ips);
    } else {
      return s;
    }
  }

 private:
  // No copying allowed
  void operator=(const LazyInitEnv&);
  LazyInitEnv(const LazyInitEnv&);

  Status OpenEnv() {
    Status s;
    if (env_ok_ && env_ == NULL) {
      MutexLock ml(&mu_);
      if (env_ok_ && env_ == NULL) {
        env_ = Env::Open(env_name_, env_conf_);
        if (env_ == NULL) {
          env_ok_ = false;
        }
      }
    }
    if (!env_ok_) {
      char tmp[30];
      snprintf(tmp, sizeof(tmp), "Fail to load env %s", env_name_.c_str());
      return Status::IOError(tmp);
    } else {
      return s;
    }
  }

  // Constant after construction
  std::string env_name_;
  std::string env_conf_;
  // State below is protected by mu_
  port::Mutex mu_;
  // False if lazy initialization failed
  bool env_ok_;
  // Lazy initialized
  Env* env_;
};

}  // namespace pdlfs
