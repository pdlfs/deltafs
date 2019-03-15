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
# CMakeLists.txt  cmake file for cmds directory
# 02-Sep-2016  chuck@ece.cmu.edu
#

add_executable (deltafs-sysinfo deltafs_sysinfo.cc)
target_link_libraries (deltafs-sysinfo deltafs)

add_executable (deltafs-shell deltafs_shell.cc)
target_link_libraries (deltafs-shell deltafs)

add_executable (deltafs-mkdir deltafs_mkdir.cc)
target_link_libraries (deltafs-mkdir deltafs)

add_executable (deltafs-mkdirplus deltafs_mkdirplus.cc)
target_link_libraries (deltafs-mkdirplus deltafs)

add_executable (deltafs-ls deltafs_ls.cc)
target_link_libraries (deltafs-ls deltafs)

add_executable (deltafs-touch deltafs_touch.cc)
target_link_libraries (deltafs-touch deltafs)

add_executable (deltafs-chown deltafs_chown.cc)
target_link_libraries (deltafs-chown deltafs)

add_executable (deltafs-unlink deltafs_unlink.cc)
target_link_libraries (deltafs-unlink deltafs)

add_executable (deltafs-stat deltafs_stat.cc)
target_link_libraries (deltafs-stat deltafs)

add_executable (deltafs-accessdir deltafs_accessdir.cc)
target_link_libraries (deltafs-accessdir deltafs)

add_executable (deltafs-access deltafs_access.cc)
target_link_libraries (deltafs-access deltafs)

#
# "make install" rules
#
install (TARGETS deltafs-sysinfo deltafs-shell deltafs-mkdir deltafs-mkdirplus
                 deltafs-ls deltafs-touch deltafs-unlink deltafs-stat
                 deltafs-accessdir deltafs-access
                 deltafs-chown
         RUNTIME DESTINATION bin)
