#pragma once

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <string>

#include "pdlfs-common/slice.h"

namespace pdlfs {

// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.
class Status {
 public:
  // Create a status that indicates a success.
  Status() : state_(NULL) {}
  ~Status() { delete[] state_; }

  Status(const Status& s);  // Copy from the specified status.
  void operator=(const Status& s);

  // Return a success status.
  static Status OK() { return Status(); }

  // Returns true iff the status indicates success.
  bool ok() const { return (state_ == NULL); }

#define DEF_ERR(Err)                                                 \
  static Status Err(const Slice& msg, const Slice& msg2 = Slice()) { \
    return Status(k##Err, msg, msg2);                                \
  }                                                                  \
  bool Is##Err() const { return code() == k##Err; }

  DEF_ERR(NotFound);
  DEF_ERR(AlreadyExists);
  DEF_ERR(Corruption);
  DEF_ERR(NotSupported);
  DEF_ERR(InvalidArgument);
  DEF_ERR(IOError);
  DEF_ERR(BufferFull);
  DEF_ERR(ReadOnly);
  DEF_ERR(WriteOnly);
  DEF_ERR(DeadLocked);
  DEF_ERR(OptimisticLockFailed);
  DEF_ERR(TryAgain);
  DEF_ERR(Disconnected);
  DEF_ERR(AssertionFailed);
  DEF_ERR(AccessDenied);
  DEF_ERR(DirExpected);
  DEF_ERR(FileExpected);
  DEF_ERR(DirNotEmpty);
  DEF_ERR(DirNotAllocated);
  DEF_ERR(DirDisabled);
  DEF_ERR(DirMarkedDeleted);

#undef DEF_ERR

  int err_code() const { return static_cast<int>(code()); }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

  enum Code {
    kOk = 0,
    kNotFound = 1,
    kAlreadyExists = 2,
    kCorruption = 3,
    kNotSupported = 4,
    kInvalidArgument = 5,
    kIOError = 6,
    kBufferFull = 7,
    kReadOnly = 8,
    kWriteOnly = 9,
    kDeadLocked = 10,
    kOptimisticLockFailed = 11,
    kTryAgain = 12,
    kDisconnected = 13,
    kAssertionFailed = 14,
    kAccessDenied = 15,
    kDirExpected = 16,
    kFileExpected = 17,
    kDirNotEmpty = 18,
    kDirNotAllocated = 19,
    kDirDisabled = 20,
    kDirMarkedDeleted = 21
  };

  static const int kMaxCode = kDirMarkedDeleted;

  static Status FromCode(int err_code) {
    assert(err_code > 0 && err_code <= kMaxCode);
    return Status(static_cast<Code>(err_code), Slice(), Slice());
  }

 private:
  // OK status has a NULL state_.  Otherwise, state_ is a new[] array
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4]    == code
  //    state_[5..]  == message
  const char* state_;

  Code code() const {
    return (state_ == NULL) ? kOk : static_cast<Code>(state_[4]);
  }

  Status(Code code, const Slice& msg, const Slice& msg2);
  static const char* CopyState(const char* s);
};

inline Status::Status(const Status& s) {
  state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
}

inline void Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (state_ != s.state_) {
    delete[] state_;
    if (s.state_ != NULL) {
      state_ = CopyState(s.state_);
    } else {
      state_ = NULL;
    }
  }
}

}  // namespace pdlfs
