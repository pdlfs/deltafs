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
# find snappy library and set up an imported target for it since
# snappy doesn't provide this for us...
#

# 
# inputs:
#   - SNAPPY_INCLUDE_DIR: hint for finding snappy.h
#   - SNAPPY_LIBRARY_DIR: hint for finding snappy lib
#
# output:
#   - "snappy" library target 
#   - SNAPPY_FOUND  (set if found)
#

include (FindPackageHandleStandardArgs)

find_path (SNAPPY_INCLUDE snappy.h HINTS ${SNAPPY_INCLUDE_DIR})
find_library (SNAPPY_LIBRARY snappy HINTS ${SNAPPY_LIBRARY_DIR})

find_package_handle_standard_args (Snappy DEFAULT_MSG 
    SNAPPY_INCLUDE SNAPPY_LIBRARY)

mark_as_advanced (SNAPPY_INCLUDE SNAPPY_LIBRARY)

if (SNAPPY_FOUND AND NOT TARGET snappy)
    add_library (snappy UNKNOWN IMPORTED)
    set_target_properties (snappy PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${SNAPPY_INCLUDE}")
    set_property (TARGET snappy APPEND PROPERTY
        IMPORTED_LOCATION "${SNAPPY_LIBRARY}")
endif ()

