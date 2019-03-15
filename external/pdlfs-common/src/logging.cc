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

#include "pdlfs-common/logging.h"

namespace pdlfs {

void Verbose(Logger* info_log, const char* file, int line, int level,
             const char* fmt, ...) {
  if (info_log != NULL) {
    va_list ap;
    va_start(ap, fmt);
    info_log->Logv(file, line, 0, level, fmt, ap);
    va_end(ap);
  }
}

void Info(Logger* info_log, const char* file, int line, const char* fmt, ...) {
  if (info_log != NULL) {
    va_list ap;
    va_start(ap, fmt);
    info_log->Logv(file, line, 0, 0, fmt, ap);
    va_end(ap);
  }
}

void Warn(Logger* info_log, const char* file, int line, const char* fmt, ...) {
  if (info_log != NULL) {
    va_list ap;
    va_start(ap, fmt);
    info_log->Logv(file, line, 1, 0, fmt, ap);
    va_end(ap);
  }
}

void Error(Logger* info_log, const char* file, int line, const char* fmt, ...) {
  if (info_log != NULL) {
    va_list ap;
    va_start(ap, fmt);
    info_log->Logv(file, line, 2, 0, fmt, ap);
    va_end(ap);
  }
}

}  // namespace pdlfs
