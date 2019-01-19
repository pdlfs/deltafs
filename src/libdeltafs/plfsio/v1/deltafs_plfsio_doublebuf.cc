/*
 * Copyright (c) 2015-2019 Carnegie Mellon University and
 *         Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_doublebuf.h"

namespace pdlfs {
namespace plfsio {

DoubleBuffering::DoubleBuffering(port::Mutex* mu, port::CondVar* cv, void* buf0,
                                 void* buf1)
    : mu_(mu),
      bg_cv_(cv),
      num_compac_scheduled_(0),
      num_compac_completed_(0),
      finished_(false),
      is_compaction_forced_(false),
      has_bg_compaction_(false),
      mem_buf_(NULL),
      imm_buf_(NULL),
      buf0_(buf0),
      buf1_(buf1) {}

// Wait for a certain compaction to clear.
// REQUIRES: mu_ has been LOCKed.
void DoubleBuffering::WaitFor(uint32_t compac_seq) {
  mu_->AssertHeld();
  while (bg_status_.ok() && num_compac_completed_ < compac_seq) {
    bg_cv_->Wait();
  }
}

// Wait until there is no outstanding compactions.
// REQUIRES: mu_ has been LOCKed.
void DoubleBuffering::WaitForCompactions() {
  mu_->AssertHeld();
  while (bg_status_.ok() && has_bg_compaction_) {
    bg_cv_->Wait();
  }
}

}  // namespace plfsio
}  // namespace pdlfs
