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
#define Log(info_log, lvl, ...) \
  Verbose(info_log, __FILE__, __LINE__, lvl, __VA_ARGS__)
#include "pdlfs-common/logging.h"
