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
#pragma once

#include "pdlfs-common/env.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"

#include <deque>

namespace pdlfs {

// A simple thread pool implementation with a fixed max pool size.
// Once created, threads keep running until pool destruction.
class PosixThreadPool : public ThreadPool {
 public:
  PosixThreadPool(int max_threads, bool eager_init = false, void* attr = NULL)
      : bg_cv_(&mu_),
        num_pool_threads_(0),
        max_threads_(max_threads),
        shutting_down_(false),
        paused_(false) {
    if (eager_init) {
      // Start pool threads immediately
      MutexLock ml(&mu_);
      InitPool(attr);
    }
  }

  virtual ~PosixThreadPool();
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
    reinterpret_cast<PosixThreadPool*>(arg)->BGThread();
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

}  // namespace pdlfs
