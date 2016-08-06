/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#if defined(RADOS)
#include "rados_common.h"

namespace pdlfs {
namespace rados {

void RadosOpCtx::IO_safe(rados_completion_t comp, void* arg) {
  if (arg != NULL) {
    RadosOpCtx* ctx = reinterpret_cast<RadosOpCtx*>(arg);
    MutexLock ml(ctx->mu_);
    int err = rados_aio_get_return_value(comp);
    if (ctx->err_ == 0 && err != 0) {
      ctx->err_ = err;
    }
    ctx->Unref();
  }
}

}  // namespace rados
}  // namespace pdlfs

#endif  // RADOS
