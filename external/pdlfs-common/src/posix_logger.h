/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "posix_env.h"

#include <stdarg.h>
#include <stdio.h>

namespace pdlfs {

// Logger implementation that can be shared by all environments
// where enough posix functionality is available.
class PosixLogger : public Logger {
 private:
  FILE* file_;
  // Return the thread id for the current thread
  uint64_t (*gettid_)();

 public:
  PosixLogger(FILE* f, uint64_t (*gettid)()) : file_(f), gettid_(gettid) {}

  virtual void Logv(const char* file, int line, int severity, int verbose,
                    const char* format, va_list ap);

  virtual ~PosixLogger() {
    if (file_ != NULL) {
      fclose(file_);
    }
  }
};

}  // namespace pdlfs
