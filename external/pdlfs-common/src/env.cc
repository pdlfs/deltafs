/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdio.h>

#include "pdlfs-common/env.h"
#include "pdlfs-common/logging.h"
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/rados/rados_ld.h"
#if defined(PDLFS_PLATFORM_POSIX)
#include "posix_logger.h"
#endif
#if defined(GLOG)
#include <glog/logging.h>
#endif

namespace pdlfs {

static const size_t kBufferSize = 8192;

Env::~Env() {}

SequentialFile::~SequentialFile() {}

RandomAccessFile::~RandomAccessFile() {}

WritableFile::~WritableFile() {}

Logger::~Logger() {}

FileLock::~FileLock() {}

ThreadPool::~ThreadPool() {}

EnvWrapper::~EnvWrapper() {}

Env* Env::Open(const Slice& env_name, const Slice& env_conf) {
  assert(env_name.size() != 0);
#if VERBOSE >= 1
  Verbose(__LOG_ARGS__, 1, "env.name -> %s", env_name.c_str());
  Verbose(__LOG_ARGS__, 1, "env.conf -> %s", env_conf.c_str());
#endif
#ifdef RADOS
  if (env_name == "rados") {
    return reinterpret_cast<Env*>(PDLFS_Load_rados_env(env_conf.c_str()));
  }
#endif
  if (env_name == "posix") {
#if defined(PDLFS_PLATFORM_POSIX)
    return Env::Default();
#else
    return NULL;
#endif
  } else {
    return NULL;
  }
}

static void EmitLog(Logger* info_log, const char* fmt, va_list ap) {
  info_log->Logv("pdlfs-xxx.cc", 0, 0, 3, fmt, ap);
}

void Log(Logger* info_log, const char* fmt, ...) {
  if (info_log != NULL) {
    va_list ap;
    va_start(ap, fmt);
    EmitLog(info_log, fmt, ap);
    va_end(ap);
  }
}

static Status DoWriteStringToFile(Env* env, const Slice& data,
                                  const Slice& fname, bool should_sync) {
  WritableFile* file;
  Status s = env->NewWritableFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  s = file->Append(data);
  if (s.ok() && should_sync) {
    s = file->Sync();
  }
  if (s.ok()) {
    s = file->Close();
  }
  delete file;  // Will auto-close if we did not close above
  if (!s.ok()) {
    env->DeleteFile(fname);
  }
  return s;
}

Status WriteStringToFile(Env* env, const Slice& data, const Slice& fname) {
  return DoWriteStringToFile(env, data, fname, false);
}

Status WriteStringToFileSync(Env* env, const Slice& data, const Slice& fname) {
  return DoWriteStringToFile(env, data, fname, true);
}

Status ReadFileToString(Env* env, const Slice& fname, std::string* data) {
  data->clear();
  SequentialFile* file;
  Status s = env->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  char* space = new char[kBufferSize];
  while (true) {
    Slice fragment;
    s = file->Read(kBufferSize, &fragment, space);
    if (!s.ok()) {
      break;
    }
    AppendSliceTo(data, fragment);
    if (fragment.empty()) {
      break;
    }
  }
  delete[] space;
  delete file;
  return s;
}

namespace {
/* clang-format off */
#if defined(PDLFS_PLATFORM_POSIX) && defined(GLOG)
class PosixGoogleLogger : public Logger {
 public:
  PosixGoogleLogger() {}
  // We try twice: the first time with a fixed-size stack allocated buffer
  // and the second time with a much larger heap allocated buffer.
  static char* VsnprintfWrapper(char (&buffer)[500], const char* fmt,
                                va_list ap) {
    char* base;
    int bufsize;
    for (int iter = 0; iter < 2; iter++) {
      if (iter == 0) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 30000;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize - 1;

      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, fmt, backup_ap);
        va_end(backup_ap);
      }

      // Truncate to available space
      if (p >= limit) {
        if (iter == 0) {
          continue;  // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }

      // Add newline if necessary
      if (p == base || p[-1] != '\n') {
        *p++ = '\n';
      }

      p[0] = 0;
      break;
    }

    return base;
  }

  virtual void Logv(const char* file, int line, int severity, int verbose,
                    const char* format, va_list ap) {
    if (severity > 0 || VLOG_IS_ON(verbose)) {
      char buffer[500];
      char* msg = VsnprintfWrapper(buffer, format, ap);
      ::google::LogMessage(file, line, severity).stream() << msg;
      if (msg != buffer) {
        delete[] msg;
      }
    }
  }
};
#endif
/* clang-format on */
class NoOpLogger : public Logger {
 public:
  NoOpLogger() {}
  virtual void Logv(const char* file, int line, int severity, int verbose,
                    const char* format, va_list ap) {
    // empty
  }
};
}  // namespace

Logger* Logger::Default() {
#if defined(PDLFS_PLATFORM_POSIX) && defined(GLOG)
  static PosixGoogleLogger logger;
  return &logger;
#elif defined(PDLFS_PLATFORM_POSIX)
  static PosixLogger logger(stderr, port::PthreadId);
  return &logger;
#else
  static NoOpLogger logger;
  return &logger;
#endif
}

}  // namespace pdlfs
