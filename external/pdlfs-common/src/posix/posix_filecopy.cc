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
#include "posix_filecopy.h"

#include "posix_env.h"

namespace pdlfs {

// Copy *src to *dst by streaming data through a small user-space buffer space.
// Return OK on success, or a non-OK status on errors.
Status Copy(const char* src, const char* dst) {
  Status status;
  int r = -1;
  int w = -1;
  if ((r = open(src, O_RDONLY)) == -1) {
    status = PosixError(src, errno);
  }
  if (status.ok()) {
    if ((w = open(dst, O_CREAT | O_TRUNC | O_WRONLY, 0644)) == -1) {
      status = PosixError(dst, errno);
    }
  }
  if (status.ok()) {
    ssize_t n;
    char buf[4096];
    while ((n = read(r, buf, 4096)) > 0) {
      ssize_t m = write(w, buf, n);
      if (m != n) {
        status = PosixError(dst, errno);
        break;
      }
    }
    if (n == -1) {
      if (status.ok()) {
        status = PosixError(src, errno);
      }
    }
  }
  if (r != -1) {
    close(r);
  }
  if (w != -1) {
    close(w);
  }
  return status;
}

}  // namespace pdlfs
