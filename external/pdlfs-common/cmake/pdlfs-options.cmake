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

#
# pdlfs-common config flags:
#   -DPDLFS_PLATFORM=POSIX                 -- platform (currently only posix)
#   -DPDLFS_TARGET_OS=Linux                -- name for the target os (when cross compile)
#   -DPDLFS_TARGET_OS_VERSION=4.4.0        -- version for the target os (when cross compile)
#   -DPDLFS_OS=Linux                       -- the result of "uname -s" on host os
#   -DPDLFS_OS_VERSION=4.4.0               -- the result of "uname -r" on host os
#   -DPDLFS_COMMON_LIBNAME=pdlfs-common    -- name for binary lib files
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
#
# note: package config files for external packages must be preinstalled in 
#       CMAKE_INSTALL_PATH or on CMAKE_PREFIX_PATH, except as noted.
#

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
set (PDLFS_OS "${CMAKE_HOST_SYSTEM_NAME}" CACHE
     STRING "Select host os (uname -s)")
set (PDLFS_OS_VERSION "${CMAKE_HOST_SYSTEM_VERSION}" CACHE
     STRING "Select host os version (uname -r)")
set (PDLFS_GFLAGS "OFF" CACHE
     BOOL "Use GFLAGS (libgflags-dev) for arg parsing")
set (PDLFS_GLOG "OFF" CACHE
     BOOL "Use GLOG (libgoogle-glog-dev) for logging")
set (PDLFS_MARGO_RPC "OFF" CACHE
     BOOL "Compile in Margo/abt-snoozer/argobots RPC interface")
set (PDLFS_MERCURY_RPC "OFF" CACHE
     BOOL "Compile in Mercury RPC interface")
set (PDLFS_RADOS "OFF" CACHE
     BOOL "Compile in RADOS object store")
set (PDLFS_SNAPPY "OFF" CACHE
     BOOL "Enable SNAPPY (libsnappy-dev) for compression")
set (PDLFS_VERBOSE "0" CACHE STRING "Max verbose level in log outputs")
set_property (CACHE PDLFS_VERBOSE PROPERTY STRINGS "0" "1" "2" "3"
              "4" "5" "6" "7" "8" "9" "10")

#
# ensure max log verbose level
#
add_compile_options (-DVERBOSE=${PDLFS_VERBOSE})

#
# now start pulling the parts in.  currently we set find_package to
# REQUIRED (so cmake will fail if the package is missing).  we could
# remove that and print a (more meaningful?) custom error with a
# message FATAL_ERROR ...
#
include (xpkg-import)

if (PDLFS_GFLAGS)
    find_package(gflags REQUIRED)
    message (STATUS "gflags enabled")
endif ()

if (PDLFS_GLOG)
    xdual_import (glog::glog,glog,libglog REQUIRED)
    message (STATUS "glog enabled")
endif ()

if (PDLFS_MERCURY_RPC)
    find_package(mercury CONFIG REQUIRED)
    message (STATUS "mercury rpc enabled")
endif ()

if (PDLFS_MARGO_RPC)
    xpkg_import_module (margo REQUIRED margo)
    message (STATUS "margo rpc enabled")
endif ()

if (PDLFS_RADOS)
    find_package(RADOS MODULE REQUIRED)
    message (STATUS "rados enabled")
endif ()

if (PDLFS_SNAPPY)
    find_package(Snappy MODULE REQUIRED)
    message (STATUS "snappy enabled")
endif ()
