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
# find RADOS library and set up an imported target for it since
# RADOS don't provide this for us...
#

# 
# inputs:
#   - RADOS_INCLUDE_DIR: hint for finding rado/librados.h
#   - RADOS_LIBRARY_DIR: hint for finding rados lib
#
# output:
#   - "rados" library target 
#   - RADOS_FOUND  (set if found)
#

include (FindPackageHandleStandardArgs)

find_path (RADOS_INCLUDE rados/librados.h HINTS ${RADOS_INCLUDE_DIR})
find_library (RADOS_LIBRARY rados HINTS ${RADOS_LIBRARY})

find_package_handle_standard_args (RADOS DEFAULT_MSG 
    RADOS_INCLUDE RADOS_LIBRARY)

mark_as_advanced (RADOS_INCLUDE RADOS_LIBRARY)

if (RADOS_FOUND AND NOT TARGET rados)
    add_library (rados UNKNOWN IMPORTED)
    set_target_properties (rados PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${RADOS_INCLUDE}")
    set_property (TARGET rados APPEND PROPERTY
        IMPORTED_LOCATION "${RADOS_LIBRARY}")
endif ()

