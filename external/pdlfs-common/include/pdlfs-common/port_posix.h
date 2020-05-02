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

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */
#pragma once

#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/pdlfs_platform.h"

#include <string>
#undef PLATFORM_IS_LITTLE_ENDIAN
#if defined(PDLFS_OS_MACOSX)
#include <machine/endian.h>
#if defined(__DARWIN_LITTLE_ENDIAN) && defined(__DARWIN_BYTE_ORDER)
#define PLATFORM_IS_LITTLE_ENDIAN \
  (__DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN)
#endif
#include <libkern/OSByteOrder.h>
#define htobe16(x) OSSwapHostToBigInt16(x)
#define htole16(x) OSSwapHostToLittleInt16(x)
#define be16toh(x) OSSwapBigToHostInt16(x)
#define le16toh(x) OSSwapLittleToHostInt16(x)
#define htobe32(x) OSSwapHostToBigInt32(x)
#define htole32(x) OSSwapHostToLittleInt32(x)
#define be32toh(x) OSSwapBigToHostInt32(x)
#define le32toh(x) OSSwapLittleToHostInt32(x)
#define htobe64(x) OSSwapHostToBigInt64(x)
#define htole64(x) OSSwapHostToLittleInt64(x)
#define be64toh(x) OSSwapBigToHostInt64(x)
#define le64toh(x) OSSwapLittleToHostInt64(x)
#elif defined(PDLFS_OS_SOLARIS)
#include <sys/isa_defs.h>
#ifdef _LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN true
#else
#define PLATFORM_IS_LITTLE_ENDIAN false
#endif
#elif defined(PDLFS_OS_FREEBSD) || defined(PDLFS_OS_OPENBSD) || \
    defined(PDLFS_OS_NETBSD)
#include <sys/endian.h>
#include <sys/types.h>
#define PLATFORM_IS_LITTLE_ENDIAN (_BYTE_ORDER == _LITTLE_ENDIAN)
#elif defined(PDLFS_OS_HPUX)
#define PLATFORM_IS_LITTLE_ENDIAN false
#else
#include <endian.h>
#endif
#include <pthread.h>
#ifdef PDLFS_SNAPPY
#include <snappy.h>
#endif
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#include "pdlfs-common/atomic_pointer.h"  // Platform-specific atomic pointer

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

#if defined(PDLFS_OS_MACOSX) || defined(PDLFS_OS_SOLARIS) || \
    defined(PDLFS_OS_FREEBSD) || defined(PDLFS_OS_NETBSD) || \
    defined(PDLFS_OS_OPENBSD) || defined(PDLFS_OS_HPUX) ||   \
    defined(PDLFS_OS_CYGWIN)
// Use fread/fwrite/fflush on platforms without _unlocked variants
#define fread_unlocked fread
#define fwrite_unlocked fwrite
#define fflush_unlocked fflush
#endif

#if defined(PDLFS_OS_MACOSX) || defined(PLDFS_OS_FREEBSD) || \
    defined(PDLFS_OS_OPENBSD)
// Use fsync() on platforms without fdatasync()
#define fdatasync fsync
#endif

namespace pdlfs {
class Env;

namespace port {

#define PDLFS_HOST_NAME_MAX _POSIX_HOST_NAME_MAX
static const bool kLittleEndian = PLATFORM_IS_LITTLE_ENDIAN;
#undef PLATFORM_IS_LITTLE_ENDIAN

#if (_XOPEN_SOURCE - 0) >= 500 && !defined(NDEBUG)
#define PDLFS_MUTEX_DEBUG
#if (_XOPEN_SOURCE - 0) >= 700
#define PDLFS_MUTEX_DEBUG_ROBUST
#endif
#endif

class CondVar;

class Mutex {
 public:
  Mutex();
#if defined(PDLFS_MUTEX_DEBUG)
  void AssertHeld();
#else
  void AssertHeld() {}
#endif
  void Lock();
  void Unlock();
  ~Mutex();

 private:
  friend class CondVar;
  pthread_mutex_t mu_;

  // No copying
  void operator=(const Mutex&);
  Mutex(const Mutex&);
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu);
  // Return true iff we run out of time, false otherwise.
  bool TimedWait(uint64_t micro);
  void Wait();
  void Signal();
  void SignalAll();
  ~CondVar();

 private:
  // No copying
  void operator=(const CondVar&);
  CondVar(const CondVar&);

  pthread_cond_t cv_;
  Mutex* mu_;
};

typedef pthread_once_t OnceType;
#define PDLFS_ONCE_INIT PTHREAD_ONCE_INIT
extern void InitOnce(OnceType* once, void (*initializer)());
extern uint64_t PthreadId();

inline bool Snappy_Compress(const char* input, size_t length,
                            ::std::string* output) {
#ifdef PDLFS_SNAPPY
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
#endif

  return false;
}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
#ifdef PDLFS_SNAPPY
  return snappy::GetUncompressedLength(input, length, result);
#else
  return false;
#endif
}

inline bool Snappy_Uncompress(const char* input, size_t length, char* output) {
#ifdef PDLFS_SNAPPY
  return snappy::RawUncompress(input, length, output);
#else
  return false;
#endif
}

inline bool GetHeapProfile(void (*)(void*, const char*, int), void*) {
  return false;
}

extern void PthreadCall(const char* label, int result);

}  // namespace port
}  // namespace pdlfs
