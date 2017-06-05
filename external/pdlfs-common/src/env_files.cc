/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/env_files.h"

namespace pdlfs {

Status WholeFileBufferedRandomAccessFile::Load() {
  Status status;
  assert(base_ != NULL);
  buf_size_ = 0;
  while (buf_size_ < max_buf_size_) {  // Reload until we reach max_buf_size_
    size_t n = io_size_;
    if (n > max_buf_size_ - buf_size_) {
      n = max_buf_size_ - buf_size_;
    }
    Slice sli;
    char* p = buf_ + buf_size_;
    status = base_->Read(n, &sli, p);
    if (!status.ok()) break;  // Error
    if (sli.empty()) break;   // EOF
    if (sli.data() != p) {
      // File implementation gave us pointer to some other data.
      // Explicitly copy it into our buffer.
      memcpy(p, sli.data(), sli.size());
    }
    buf_size_ += sli.size();
  }

  delete base_;
  base_ = NULL;
  return status;
}

}  // namespace pdlfs
