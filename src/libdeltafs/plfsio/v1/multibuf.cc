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

#include "multibuf.h"

namespace pdlfs {
namespace plfsio {

MultiBuffering::MultiBuffering(port::Mutex* mu, port::CondVar* cv,
                               size_t n_total, size_t n_active)
    : mu_(mu),
      bg_cv_(cv),
      num_compac_scheduled_(0),
      num_compac_completed_(0),
      finished_(false),
      num_bg_compactions_(0),
      bufs_active_(nullptr),
      n_total_(n_total),
      n_active_(n_active) {}

// Wait until there is no outstanding compactions.
// REQUIRES: __Finish() has NOT been called.
// REQUIRES: mu_ has been LOCKed.
Status MultiBuffering::__Wait() {
  WaitForAny();  // Wait until !num_bg_compactions_
  return bg_status_;
}

// Wait for a certain compaction to clear.
// REQUIRES: mu_ has been LOCKed.
void MultiBuffering::WaitFor(uint32_t compac_seq) {
  mu_->AssertHeld();
  while (bg_status_.ok() && num_compac_completed_ < compac_seq) {
    bg_cv_->Wait();
  }
}

// Wait until there is no outstanding compactions.
// REQUIRES: mu_ has been LOCKed.
void MultiBuffering::WaitForAny() {
  mu_->AssertHeld();
  while (bg_status_.ok() && num_bg_compactions_) {
    bg_cv_->Wait();
  }
}

}  // namespace plfsio
}  // namespace pdlfs
