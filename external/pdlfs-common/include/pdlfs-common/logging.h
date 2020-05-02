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
#pragma once

#include "pdlfs-common/env.h"

// Common logging interface designed to be used like:
//  - Verbose(__LOG_ARGS__, verbose_level, "format string", va_args)
//  - Info(__LOG_ARGS__, "format string", va_args)
//  - Warn(__LOG_ARGS__, "format string", va_args)
//  - Error(__LOG_ARGS__, "format string", va_args)
//
// If google-glog is present, all logging activities will go to it.
// We log to stderr otherwise.
namespace pdlfs {
#define DEF_LOGGER ::pdlfs::Logger::Default()
#define __LOG_ARGS__ DEF_LOGGER, __FILE__, __LINE__
#define Log0v(lvl, ...) Error(DEF_LOGGER, __FILE__, __LINE__, lvl, __VA_ARGS__)
// Emit a verbose log entry to *info_log if info_log is non-NULL.
extern void Verbose(Logger* info_log, const char* file, int line, int level,
                    const char* format, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((__format__(__printf__, 5, 6)))
#endif
    ;
#define Log0(...) Info(DEF_LOGGER, __FILE__, __LINE__, __VA_ARGS__)
// Emit an info log entry to *info_log if info_log is non-NULL.
extern void Info(Logger* info_log, const char* file, int line,
                 const char* format, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((__format__(__printf__, 4, 5)))
#endif
    ;
#define Log1(...) Warn(DEF_LOGGER, __FILE__, __LINE__, __VA_ARGS__)
// Emit a warning log entry to *info_log if info_log is non-NULL.
extern void Warn(Logger* info_log, const char* file, int line,
                 const char* format, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((__format__(__printf__, 4, 5)))
#endif
    ;
#define Log2(...) Error(DEF_LOGGER, __FILE__, __LINE__, __VA_ARGS__)
// Emit an error log entry to *info_log if info_log is non-NULL.
extern void Error(Logger* info_log, const char* file, int line,
                  const char* format, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((__format__(__printf__, 4, 5)))
#endif
    ;

}  // namespace pdlfs
