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

#include "pdlfs-common/slice.h"

#include <stdint.h>

namespace pdlfs {
typedef uint64_t SequenceNumber;

class Buffer {
 public:
  virtual void Fill(const char* data, size_t size) = 0;

 protected:
  virtual ~Buffer() {}
};

namespace db {
class StringBuf : public Buffer {
 public:
  explicit StringBuf(std::string* s) : s_(s) {}
  virtual ~StringBuf() {}

  virtual void Fill(const char* data, size_t size) {
    if (size != 0) {
      s_->assign(data, size);
    }
  }

 private:
  std::string* s_;
};

class DirectBuf : public Buffer {
 public:
  DirectBuf(char* p, size_t s) : p_(p), size_(s) {}
  virtual ~DirectBuf() {}

  virtual void Fill(const char* data, size_t size) {
    if (size > size_) {
      // We run out of space, so we record the size
      // so the application can retry with a large enough buffer next time.
      data_ = Slice(NULL, size);
    } else {
      data_ = Slice(p_, size);
      if (size != 0) {
        memcpy(p_, data, size);
      }
    }
  }

  Slice Read() { return data_; }

 private:
  Slice data_;
  char* p_;
  size_t size_;
};

}  // namespace db
}  // namespace pdlfs
