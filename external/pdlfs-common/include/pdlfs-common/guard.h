#pragma once

/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>

namespace pdlfs {

template <typename T, typename Ref>
class RefGuard {
  Ref* ref_;
  T* src_;

  // No copying allowed
  void operator=(const RefGuard&);
  RefGuard(const RefGuard&);

 public:
  explicit RefGuard(T* t, Ref* r) : ref_(r), src_(t) {}
  ~RefGuard() {
    if (ref_ != NULL) {
      src_->Release(ref_);
    }
  }
};

}  // namespace pdlfs
