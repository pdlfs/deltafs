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
# cmake-options.cmake  handle generic pdlfs/cmake config
# 27-Oct-2016  chuck@ece.cmu.edu
#

#
# generic cmake setup and config we use for pdlfs.  note we assume 
# that pdlfs-common/cmake is on CMAKE_MODULE_PATH so we can include
# files from that directory (e.g. xpkg-import).
#

#  general cmake flags:
#    -DCMAKE_INSTALL_PREFIX=/usr/local     -- the prefix for installing
#    -DCMAKE_BUILD_TYPE=type               -- type can be DEBUG, RELEASE, ...
#    -DCMAKE_PREFIX_PATH=/dir              -- external packages
#    -DBUILD_SHARED_LIBS=OFF               -- turn ON for shared libs
#    -DBUILD_TESTS=OFF                     -- turn ON to build unit tests
#
#     note that CMAKE_PREFIX_PATH can be a list of directories:
#      -DCMAKE_PREFIX_PATH='/dir1;/dir2;/dir3'
#

#
# link shared lib with full rpath. on some modern platforms, the newer
# runpath will be used instead of rpath.
set (CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# quiet CMP0042 warning  (not needed if cmake >=3.0)
set (CMAKE_MACOSX_RPATH 1)

enable_testing ()

# setup cache variables for ccmake
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE RelWithDebInfo
         CACHE STRING "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS 
                  "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (DEBUG_SANITIZER Off CACHE STRING "Sanitizer for debug builds")
mark_as_advanced (DEBUG_SANITIZER)
set_property (CACHE DEBUG_SANITIZER PROPERTY STRINGS
              "Off" "Address" "Thread" "Undefined")
set (CMAKE_PREFIX_PATH "" CACHE STRING "External dependencies path")
set (BUILD_SHARED_LIBS "OFF" CACHE BOOL "Build a shared library")
set (BUILD_TESTS "OFF" CACHE BOOL "Build test programs")

#
# sanitizer config (only used in debug builds)
#
### set (as_flags "-fsanitize=address,leak -O1 -fno-omit-frame-pointer")
#

set (as_flags "-fsanitize=address -O1 -fno-omit-frame-pointer")
set (ts_flags "-fsanitize=thread  -O1 -fno-omit-frame-pointer")
set (us_flags "-fsanitize=undefined -O1")

if (${CMAKE_C_COMPILER_ID} STREQUAL "GNU" OR
    ${CMAKE_C_COMPILER_ID} STREQUAL "Clang")
    if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
        if (${DEBUG_SANITIZER} STREQUAL "Address")
            set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${as_flags}")
        elseif (${DEBUG_SANITIZER} STREQUAL "Thread")
            set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${ts_flags}")
        elseif (${DEBUG_SANITIZER} STREQUAL "Undefined")
            set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${us_flags}")
        endif ()
    endif ()
endif ()

if (${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU" OR
    ${CMAKE_CXX_COMPILER_ID} STREQUAL "Clang")
    if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
        if (${DEBUG_SANITIZER} STREQUAL "Address")
            set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${as_flags}")
        elseif (${DEBUG_SANITIZER} STREQUAL "Thread")
            set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${ts_flags}")
        elseif (${DEBUG_SANITIZER} STREQUAL "Undefined")
            set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${us_flags}")
        endif ()
    endif ()
endif ()

#
# use O2 (not O3) when it comes to either Intel or Cray compilers. O2 is
# the default optimization level for these compilers and are often
# equivalent as O3 in common compilers such as GNU and Clang.
#
if (${CMAKE_C_COMPILER_ID} STREQUAL "Intel" OR
    ${CMAKE_C_COMPILER_ID} STREQUAL "Cray")
    set (CMAKE_C_FLAGS_DEBUG "-g")
    set (CMAKE_C_FLAGS_RELWITHDEBINFO "-g -O2 -DNDEBUG")
    set (CMAKE_C_FLAGS_RELEASE "-O2 -DNDEBUG")
endif ()

if (${CMAKE_CXX_COMPILER_ID} STREQUAL "Intel" OR
    ${CMAKE_CXX_COMPILER_ID} STREQUAL "Cray")
    set (CMAKE_CXX_FLAGS_DEBUG "-g")
    set (CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g -O2 -DNDEBUG")
    set (CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")
endif ()
