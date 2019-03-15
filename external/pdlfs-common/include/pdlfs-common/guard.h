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
