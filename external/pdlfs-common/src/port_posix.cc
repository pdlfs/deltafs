/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/pdlfs_platform.h"

#include "pdlfs-common/port_posix.h"

namespace pdlfs {
namespace port {

Mutex::Mutex() {
  PthreadCall("pthread_mutex_init", pthread_mutex_init(&mu_, NULL));
}

Mutex::~Mutex() {
  PthreadCall("pthread_mutex_destroy", pthread_mutex_destroy(&mu_));
}

void Mutex::Lock() {
  PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mu_));
}

void Mutex::Unlock() {
  PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mu_));
}

CondVar::CondVar(Mutex* mu) : mu_(mu) {
  PthreadCall("pthread_cond_init", pthread_cond_init(&cv_, NULL));
}

CondVar::~CondVar() {
  PthreadCall("pthread_cond_destroy", pthread_cond_destroy(&cv_));
}

void CondVar::Wait() {
  PthreadCall("pthread_cond_wait", pthread_cond_wait(&cv_, &mu_->mu_));
}

// Return true iff timeout, false otherwise
bool CondVar::TimedWait(uint64_t micro) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  struct timespec ts;
  ts.tv_nsec = 1000 * ((micro + tv.tv_usec) % 1000000);
  ts.tv_sec = tv.tv_sec + (micro + tv.tv_usec) / 1000000;
  int r = pthread_cond_timedwait(&cv_, &mu_->mu_, &ts);
  if (r != 0) {
    if (r != ETIMEDOUT) {
      perror("pthread_cond_timedwait failed");
      abort();
    } else {
      return true;  // Timeout!
    }
  }

  return false;
}

void CondVar::Signal() {
  PthreadCall("pthread_cond_signal", pthread_cond_signal(&cv_));
}

void CondVar::SignalAll() {
  PthreadCall("pthread_cond_broadcast", pthread_cond_broadcast(&cv_));
}

void InitOnce(OnceType* once, void (*initializer)()) {
  PthreadCall("pthread_once", pthread_once(once, initializer));
}

uint64_t PthreadId() {
  pthread_t tid = pthread_self();
  uint64_t thread_id = 0;
  memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
  return thread_id;
}

}  // namespace port
}  // namespace pdlfs
