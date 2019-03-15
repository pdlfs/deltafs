#
# Copyright (c) 2019 Carnegie Mellon University,
# Copyright (c) 2019 Triad National Security, LLC, as operator of
#     Los Alamos National Laboratory
#
# All rights reserved.
#
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#

#
# CMakeLists.txt  top-level cmake file for deltafs/src
# 22-Jun-2016  chuck@ece.cmu.edu
#
include (CheckCXXCompilerFlag)

#
# set some general deltafs compile options (this is a directory property)
#
set (deltafs-try-common-flags -Wpedantic -Wall)
list (APPEND deltafs-try-common-flags -Wno-long-long -Wno-sign-compare)
foreach (lcv ${deltafs-try-common-flags})
    CHECK_CXX_COMPILER_FLAG (${lcv} flag${lcv})
    if (${flag${lcv}})
        add_compile_options (${lcv})
    endif ()
endforeach ()

add_subdirectory (libdeltafs)
add_subdirectory (cmds)
add_subdirectory (server)

