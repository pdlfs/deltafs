/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/port.h"

namespace pdlfs {

// Represents everything that can be locked for synchronization.
class Lockable {
 public:
  Lockable() {}
  virtual void Lock() = 0;
  virtual void Unlock() = 0;
  virtual ~Lockable() {}

 private:
  // No copying allowed
  Lockable(const Lockable&);
  void operator=(const Lockable&);
};

// A lock that does nothing.
class DummyLock : public Lockable {
 public:
  DummyLock() {}
  virtual void Lock() {}  // Does nothing.
  virtual void Unlock() {}
  virtual ~DummyLock() {}
};

// Helper class that locks a mutex on construction and
// unlocks the mutex when the destructor of the MutexLock
// object is invoked.
class MutexLock {
 public:
  explicit MutexLock(port::Mutex* mu) : mu_(mu) { mu_->Lock(); }

  ~MutexLock() { mu_->Unlock(); }

 private:
  // No copying allowed
  MutexLock(const MutexLock&);
  MutexLock& operator=(const MutexLock&);

  port::Mutex* const mu_;
};

}  // namespace pdlfs
