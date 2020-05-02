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

#include "posix_fastcopy.h"
#include "posix_env.h"

namespace pdlfs {

// Faster file copy without using user-space buffers. Return OK on success,
// or a non-OK status on errors.
#if defined(PDLFS_OS_LINUX)
Status FastCopy(const char* src, const char* dst) {
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
    int p[2];
    if (pipe(p) == -1) {
      status = PosixError("pipe", errno);
    } else {
      const size_t batch_size = 4096;
      while (splice(p[0], 0, w, 0, splice(r, 0, p[1], 0, batch_size, 0), 0) > 0)
        ;
      close(p[0]);
      close(p[1]);
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
#endif

}  // namespace pdlfs
