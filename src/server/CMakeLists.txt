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
# CMakeLists.txt  cmake file for server directory
# 16-Jun-2016  chuck@ece.cmu.edu
#

add_executable (deltafs-srvr deltafs_server.cc)
target_link_libraries (deltafs-srvr deltafs)

#
# "make install" rules
#
install (TARGETS deltafs-srvr 
         RUNTIME DESTINATION bin)
