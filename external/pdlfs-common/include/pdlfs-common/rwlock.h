#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>

#include "pdlfs-common/port.h"

namespace pdlfs {

class RWLock {
 public:
  explicit RWLock(port::Mutex* mu)
      : mu_(mu), cv_(mu), waiting_writers_(0), writers_(0), readers_(0) {}

  void RdLock() {
    mu_->AssertHeld();
    while (writers_ > 0 || waiting_writers_ > 0) {
      cv_.Wait();
    }
    readers_++;
  }

  void RdUnlock() {
    mu_->AssertHeld();
    readers_--;
    if (readers_ == 0) {
      cv_.SignalAll();
    }
  }

  void WrLock() {
    mu_->AssertHeld();
    waiting_writers_++;
    while (readers_ > 0 || writers_ > 0) {
      cv_.Wait();
    }
    waiting_writers_--;
    writers_++;
  }

  void WrUnlock() {
    mu_->AssertHeld();
    writers_--;
    assert(writers_ == 0);
    cv_.SignalAll();
  }

 private:
  port::Mutex* mu_;
  port::CondVar cv_;
  int waiting_writers_;
  int writers_;
  int readers_;
};

class SharedLock {
 public:
  explicit SharedLock(RWLock* lock) : lock_(lock) { lock_->RdLock(); }

  ~SharedLock() { lock_->RdUnlock(); }

 private:
  // No copying allowed
  void operator=(const SharedLock&);
  SharedLock(const SharedLock&);

  RWLock* lock_;
};

class ExclusiveLock {
 public:
  explicit ExclusiveLock(RWLock* lock) : lock_(lock) { lock_->WrLock(); }

  ~ExclusiveLock() { lock_->WrUnlock(); }

 private:
  // No copying allowed
  void operator=(const ExclusiveLock&);
  ExclusiveLock(const ExclusiveLock&);

  RWLock* lock_;
};

}  // namespace pdlfs
