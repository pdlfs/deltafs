#
# Copyright (c) 2019 Carnegie Mellon University,
# Copyright (c) 2019 Triad National Security, LLC, as operator of
#     Los Alamos National Laboratory.
#
# All rights reserved.
#
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#

#
# CMakeLists.txt  cmake file for the pdlfs-common tools
# 10-Nov-2016  chuck@ece.cmu.edu
#

#
# pdlfs_db_bench: the level db benchmarking program
#
add_executable (pdlfs_db_bench pdlfs_db_bench.cc)
target_link_libraries (pdlfs_db_bench pdlfs-common)
install (TARGETS pdlfs_db_bench RUNTIME DESTINATION bin)
