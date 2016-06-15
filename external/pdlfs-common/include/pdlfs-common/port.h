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

// Include the appropriate platform specific file below.
#if defined(PDLFS_PLATFORM_POSIX)
#include "pdlfs-common/port_posix.h"
#endif
