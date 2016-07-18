#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/env.h"

// Common logging interface that could be used like:
//
//   Verbose(__LOG_ARGS__, verbose_level, "msg_fmt", va_args)
//   Info(__LOG_ARGS__, "msg_fmt", va_args)
//   Error(__LOG_ARGS__, "msg_fmt", va_args)
//
// If glog is present, all the logging is completed by it, otherwise
// these log entries will go to stderr.
namespace pdlfs {

#define __LOG_ARGS__ ::pdlfs::Logger::Default(), __FILE__, __LINE__

// Emit a verbose log entry to *info_log if info_log is non-NULL.
extern void Verbose(Logger* info_log, const char* file, int line, int level,
                    const char* format, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((__format__(__printf__, 5, 6)))
#endif
    ;

// Emit an info log entry to *info_log if info_log is non-NULL.
extern void Info(Logger* info_log, const char* file, int line,
                 const char* format, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((__format__(__printf__, 4, 5)))
#endif
    ;

// Emit an error log entry to *info_log if info_log is non-NULL.
extern void Error(Logger* info_log, const char* file, int line,
                  const char* format, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((__format__(__printf__, 4, 5)))
#endif
    ;

}  // namespace pdlfs
