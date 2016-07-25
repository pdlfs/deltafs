/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdint.h>
#include <string.h>

#include "pdlfs-common/status.h"

namespace pdlfs {

const char* Status::CopyState(const char* state) {
  uint32_t size;
  memcpy(&size, state, sizeof(size));
  char* result = new char[size + 5];
  memcpy(result, state, size + 5);
  return result;
}

Status::Status(Code code, const Slice& msg, const Slice& msg2) {
  assert(code != kOk);
  const uint32_t len1 = msg.size();
  const uint32_t len2 = msg2.size();
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
  char* result = new char[size + 5];
  memcpy(result, &size, sizeof(size));
  result[4] = static_cast<char>(code);
  memcpy(result + 5, msg.data(), len1);
  if (len2) {
    result[5 + len1] = ':';
    result[6 + len1] = ' ';
    memcpy(result + 7 + len1, msg2.data(), len2);
  }
  state_ = result;
}

namespace {
/* clang-format off */
static const char* kCodeString[] = {
  /* kOk */ "OK",                                         /* 0 */
  /* kNotFound */ "Not found",                            /* 1 */
  /* kAlreadyExists */ "Already exists",                  /* 2 */
  /* kCorruption */ "Corruption",                         /* 3 */
  /* kNotSupported */ "Not implemented",                  /* 4 */
  /* kInvalidArgument */ "Invalid argument",              /* 5 */
  /* kIOError */ "IO error",                              /* 6 */
  /* kBufferFull */ "Buffer full",                        /* 7 */
  /* kReadOnly */ "Read only",                            /* 8 */
  /* kWriteOnly */ "Write only",                          /* 9 */
  /* kDeadLocked */ "Dead locked",                        /* 10 */
  /* kOptimisticLockFailed */ "Optimistic lock failed",   /* 11 */
  /* kTryAgain */ "Try again",                            /* 12 */
  /* kDisconnected */ "Disconnected",                     /* 13 */
  /* kAssertionFailed */ "Assertion failed",              /* 14 */
  /* kAccessDenied */ "Permission denied",                /* 15 */
  /* kDirExpected */ "Dir expected",                      /* 16 */
  /* kFileExpected */ "File expected",                    /* 17 */
  /* kDirNotEmpty */ "Dir not empty",                     /* 18 */
  /* kDirNotAllocated */ "Dir not allocated",             /* 19 */
  /* kDirDisabled */ "Dir disabled",                      /* 20 */
  /* kDirMarkedDeleted */ "Dir marked deleted",           /* 21 */
  /* kInvalidFileDescriptor */ "Invalid file descriptor", /* 22 */
  /* kTooManyOpens */ "Too many open files"               /* 23 */
};
/* clang-format on */
}

std::string Status::ToString() const {
  if (state_ == NULL) {
    return "OK";
  } else {
    const char* type = kCodeString[err_code()];
    uint32_t length;
    memcpy(&length, state_, sizeof(length));
    if (length == 0) {
      return type;
    } else {
      std::string result(type);
      result.append(" :");
      result.append(state_ + 5, length);
      return result;
    }
  }
}

}  // namespace pdlfs
