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
# CMakeLists.txt  cmake file for the pdlfs-common examples
# 09-Nov-2016  chuck@ece.cmu.edu
#


# XXX: example example, just add_executable with a target link to pdlfs-common
# add_executable (foo foo.c)
# target_link_libraries (foo pdlfs-common)

add_executable (leveldb_example leveldb_example.cc)
target_link_libraries (leveldb_example pdlfs-common)

add_executable (leveldb_bulk_example leveldb_bulk_example.cc)
target_link_libraries (leveldb_bulk_example pdlfs-common)
