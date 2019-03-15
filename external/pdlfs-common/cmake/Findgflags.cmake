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
# find gflags library and setup an imported target for it.
# this is a bit complicated because old installs of gflags
# do not provide cmake config files for gflags, but new of
# gflags versions do (and provide extra targets like "gflags_shared"
# that we normally would not provide).
#
# since cmake looks for Find*.cmake MODULE packages first,
# we may hit the case where Findgflags.cmake gets included
# on a system where a gflags CONFIG-style package is provided.
# if you have a package like glog that uses the CONFIG-style
# cmake package and this MODULE package gets found first, then
# you get undefined link errors when linking the shared lib
# (e.g. lib "-lgflags_shared" not found).
#
# what to do?   we'll have this MODULE config file check
# for a CONFIG-style cmake package first, before doing anything
# else.  if we detect a CONFIG-style cmake package, we always
# yield control to that.   hopefully that solves most problem
# cases...
#

#
# inputs (for the MODULE config):
#   - GFLAGS_INCLUDE_DIR: hint for finding gflags/gflags.h
#   - GFLAGS_LIBRARY_DIR: hint for finding gflags lib
#
# output:
#   - "gflags" library target
#   - gflags_FOUND / GFLAGS_FOUND  (set if found)
#

# probe for CONFIG-style module first
find_package (gflags QUIET CONFIG)

if (gflags_FOUND)

    message (STATUS "note: gflags find module yielding to gflags config package")

else ()

    # no CONFIG-style module present... do MODULE stuff instead...
    include (FindPackageHandleStandardArgs)

    find_path (GFLAGS_INCLUDE gflags/gflags.h HINTS ${GFLAGS_INCLUDE_DIR})
    find_library (GFLAGS_LIBRARY gflags HINTS ${GFLAGS_LIBRARY_DIR})

    find_package_handle_standard_args (gflags DEFAULT_MSG
        GFLAGS_INCLUDE GFLAGS_LIBRARY)

    mark_as_advanced (GFLAGS_INCLUDE GFLAGS_LIBRARY)

    if (GFLAGS_FOUND AND NOT TARGET gflags)
        add_library (gflags UNKNOWN IMPORTED)
        set_target_properties (gflags PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${GFLAGS_INCLUDE}")
        set_property (TARGET gflags APPEND PROPERTY
            IMPORTED_LOCATION "${GFLAGS_LIBRARY}")
        message (STATUS "note: gflags no config-package, using local find module")
    endif ()

endif ()
