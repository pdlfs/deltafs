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
# pdlfs-options.cmake  handle standard pdlfs-common options
# 27-Oct-2016  chuck@ece.cmu.edu
#

#
# this file handles the standard PDLFS options (i.e. setting up the 
# cache variables, creating library targets, etc.).   note we assume 
# that pdlfs-common/cmake is on CMAKE_MODULE_PATH so we can include
# files from that directory (e.g. xpkg-import).
#
include (xpkg-import)

#
# pdlfs-common config flags:
#   -DPDLFS_PLATFORM=POSIX                 -- platform (currently only posix)
#   -DPDLFS_TARGET_OS=Linux                -- target os name (cross compile)
#   -DPDLFS_TARGET_OS_VERSION=4.4.0        -- target os vers. (cross compile)
#   -DPDLFS_HOST_OS=Linux                  -- "uname -s" on host os
#   -DPDLFS_HOST_OS_VERSION=4.4.0          -- "uname -r" on host os
#   -DPDLFS_COMMON_LIBNAME=pdlfs-common    -- name for binary lib files
#   -DPDLFS_COMMON_DEFINES='D1;D2'         -- add -DD1/-DD2 to compile options
#
# pdlfs-common config compile time options flags:
#   -DPDLFS_GFLAGS=ON                      -- use gflags for arg parsing
#     - GFLAGS_INCLUDE_DIR: optional hint for finding gflags/gflags.h
#     - GFLAGS_LIBRARY_DIR: optional hint for finding gflags lib
#   -DPDLFS_GLOG=ON                        -- use glog for logging
#   -DPDLFS_MARGO_RPC=ON                   -- compile in margo rpc code
#   -DPDLFS_MERCURY_RPC=ON                 -- compile in mercury rpc code
#   -DPDLFS_RADOS=ON                       -- compile in RADOS env
#     - RADOS_INCLUDE_DIR: optional hint for finding rado/librados.h
#     - RADOS_LIBRARY_DIR: optional hint for finding rados lib
#   -DPDLFS_SNAPPY=ON                      -- compile in snappy compression
#     - SNAPPY_INCLUDE_DIR: optional hint for finding snappy.h
#     - SNAPPY_LIBRARY_DIR: optional hint for finding snappy lib
#   -DPDLFS_VERBOSE=1                      -- set max log verbose level
#
# output variables:
#    PDLFS_COMPONENT_CFG                   -- list of requested components
#
# note: package config files for external packages must be preinstalled in 
#       CMAKE_INSTALL_PATH or on CMAKE_PREFIX_PATH, except as noted.
#

#
# tests... we EXCLUDE_FROM_ALL the tests and add a custom target
# "pdl-build-tests" to build tests (we prefix with "pdl-" since
# custom targets must be globally unique).   if the top-level
# CMakeLists.txt calls enable_testing (), then you can "make pdl-build-tests"
# and then "make test" (or run ctest directly).
#
if (NOT TARGET pdl-build-tests)           # can only define it once
    if (BUILD_TESTS)
        add_custom_target (pdl-build-tests ALL)
    else ()
        add_custom_target (pdl-build-tests)
    endif ()
endif ()

#
# PDLFS_COMMON_LIBNAME allows clients to do custom compile-time configuration
# of the library and install the customized version under an alternate name
# (e.g. lib/libdeltafs-common.a instead of lib/pdlfs-common.a).  note that
# the include files are still shared under include/pdlfs-common.  This is
# handled in pdlfs-common/src/CMakeLists.txt
#

#
# setup cached variables with default values and documentation strings
# for ccmake...
#
set (PDLFS_PLATFORM "POSIX" CACHE STRING "Select platform (e.g. POSIX)")
set (PDLFS_TARGET_OS "${CMAKE_SYSTEM_NAME}" CACHE
     STRING "Select target os")
set (PDLFS_TARGET_OS_VERSION "${CMAKE_SYSTEM_VERSION}" CACHE
     STRING "Select target os version")
set (PDLFS_HOST_OS "${CMAKE_HOST_SYSTEM_NAME}" CACHE
     STRING "Select host os (uname -s)")
set (PDLFS_HOST_OS_VERSION "${CMAKE_HOST_SYSTEM_VERSION}" CACHE
     STRING "Select host os version (uname -r)")
mark_as_advanced (PDLFS_PLATFORM PDLFS_TARGET_OS PDLFS_TARGET_OS_VERSION)
mark_as_advanced (PDLFS_HOST_OS PDLFS_HOST_OS_VERSION)
set (PDLFS_VERBOSE "0" CACHE STRING "Max verbose level in log outputs")
set_property (CACHE PDLFS_VERBOSE PROPERTY STRINGS "0" "1" "2" "3"
              "4" "5" "6" "7" "8" "9" "10")

# ensure correct log verbose level
add_compile_options (-DVERBOSE=${PDLFS_VERBOSE})

# library naming variables...
set (PDLFS_COMMON_LIBNAME "pdlfs-common" CACHE
     STRING "Custom name to install pdlfs-common with")
set (PDLFS_COMMON_DEFINES "" CACHE
     STRING "Additional defines for this version of pdlfs-common")

#
# handle third party package configuration
#

set (PDLFS_GFLAGS "OFF" CACHE
     BOOL "Use GFLAGS (libgflags-dev) for arg parsing")
set (PDLFS_GLOG "OFF" CACHE
     BOOL "Use GLOG (libgoogle-glog-dev) for logging")
set (PDLFS_MARGO_RPC "OFF" CACHE
     BOOL "Include Margo/abt-snoozer/argobots RPC interface")
set (PDLFS_MERCURY_RPC "OFF" CACHE
     BOOL "Include Mercury RPC interface")
set (PDLFS_RADOS "OFF" CACHE
     BOOL "Include RADOS object store")
set (PDLFS_SNAPPY "OFF" CACHE
     BOOL "Include (libsnappy-dev) for compression")

#
# now start pulling the parts in.  currently we set find_package to
# REQUIRED (so cmake will fail if the package is missing).  we could
# remove that and print a (more meaningful?) custom error with a
# message FATAL_ERROR ...   we also set a ${PDLFS_COMPONENT_CFG}
# variable list for users to use.
#
unset (PDLFS_COMPONENT_CFG)

if (PDLFS_GFLAGS)
    find_package(gflags REQUIRED)
    list (APPEND PDLFS_COMPONENT_CFG "gflags")
    message (STATUS "Enabled gflags - PDLFS_GFLAGS=ON")
endif ()

if (PDLFS_GLOG)
    xdual_import (glog::glog,glog,libglog REQUIRED)
    list (APPEND PDLFS_COMPONENT_CFG "glog::glog")
    message (STATUS "Enabled glog - PDLFS_GLOG=ON")
endif ()

if (PDLFS_MERCURY_RPC)
    find_package(mercury CONFIG REQUIRED)
    list (APPEND PDLFS_COMPONENT_CFG "mercury")
    message (STATUS "Enabled mercury - PDLFS_MERCURY_RPC=ON")
endif ()

if (PDLFS_MARGO_RPC)
    xpkg_import_module (margo REQUIRED margo)
    list (APPEND PDLFS_COMPONENT_CFG "margo")
    message (STATUS "Enabled margo - PDLFS_MARGO_RPC=ON")
endif ()

if (PDLFS_RADOS)
    find_package(RADOS MODULE REQUIRED)
    list (APPEND PDLFS_COMPONENT_CFG "RADOS")
    message (STATUS "Enabled RADOS - PDLFS_RADOS=ON")
endif ()

if (PDLFS_SNAPPY)
    find_package(Snappy MODULE REQUIRED)
    list (APPEND PDLFS_COMPONENT_CFG "Snappy")
    message (STATUS "Enabled Snappy - PDLFS_SNAPPY=ON")
endif ()
