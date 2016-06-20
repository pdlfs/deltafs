/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/env.h"

#include <stdio.h>
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

void Log(Logger* info_log, const char* fmt, ...) {
  if (info_log != NULL) {
    va_list ap;
    va_start(ap, fmt);
    info_log->Logv(fmt, ap);
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

/* clang-format off */
namespace {
#if defined(GLOG)
class GoogleLogger : public Logger {
 public:
  GoogleLogger() {}
  static const char* VsnprintfWrapper(char* dst, size_t n, const char* fmt,
                               va_list ap) {
    vsnprintf(dst, n, fmt, ap);
    return dst;
  }
  virtual void Logv(const char* fmt, va_list ap) {
    char buffer[5000];
    !(VLOG_IS_ON(1)) ? (void)0 : google::LogMessageVoidify() &
            ::google::LogMessage("???", 0).stream()
            << VsnprintfWrapper(buffer, 5000, fmt, ap);
  }
};
#endif

class SyserrLogger : public Logger {
 public:
  SyserrLogger() {}
  virtual void Logv(const char* fmt, va_list ap) {
    char buffer[5000];
    char* p = buffer;
    p += vsnprintf(p, sizeof(buffer), fmt, ap);
    // Add newline if necessary
    if (p == buffer || p[-1] != '\n') {
      *p++ = '\n';
      *p = 0;
    }
    fprintf(stderr, "%s", buffer);
  }
};
}  // namespace
/* clang-format on */

Logger* Logger::Default() {
#if defined(GLOG)
  static GoogleLogger logger;
  return &logger;
#else
  static SyserrLogger logger;
  return &logger;
#endif
}

}  // namespace pdlfs
