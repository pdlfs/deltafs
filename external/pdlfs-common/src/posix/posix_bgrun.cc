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
#include "posix_bgrun.h"

#include <stdio.h>

namespace pdlfs {

std::string PosixThreadPool::ToDebugString() {
  char tmp[100];
  snprintf(tmp, sizeof(tmp), "Tpool: max_threads=%d", max_threads_);
  return tmp;
}

namespace {
pthread_t Pthread(void* (*func)(void*), void* arg, void* attr) {
  pthread_t th;
  pthread_attr_t* ta = reinterpret_cast<pthread_attr_t*>(attr);
  port::PthreadCall("pthread_create", pthread_create(&th, ta, func, arg));
  port::PthreadCall("pthread_detach", pthread_detach(th));
  return th;
}
}  // namespace

PosixThreadPool::~PosixThreadPool() {
  mu_.Lock();
  shutting_down_ = true;
  bg_cv_.SignalAll();
  while (num_pool_threads_ != 0) {
    bg_cv_.Wait();
  }
  mu_.Unlock();
}

void PosixThreadPool::InitPool(void* attr) {
  mu_.AssertHeld();
  while (num_pool_threads_ < max_threads_) {
    Pthread(BGWrapper, this, attr);
    num_pool_threads_++;
  }
}

void PosixThreadPool::Schedule(void (*function)(void*), void* arg) {
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

void PosixThreadPool::BGThread() {
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

void PosixThreadPool::Resume() {
  MutexLock ml(&mu_);
  paused_ = false;
  bg_cv_.SignalAll();
}

void PosixThreadPool::Pause() {
  MutexLock ml(&mu_);
  paused_ = true;
}

void PosixThreadPool::StartThread(void (*function)(void*), void* arg) {
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  Pthread(StartThreadWrapper, state, NULL);
}

ThreadPool* ThreadPool::NewFixed(int num_threads, bool eager_init, void* attr) {
  return new PosixThreadPool(num_threads, eager_init, attr);
}

}  // namespace pdlfs
