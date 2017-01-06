#pragma once

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/port.h"

namespace pdlfs {

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
