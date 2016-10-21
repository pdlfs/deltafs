#
# CMakeLists.txt  top-level cmake file for deltafs
# 16-Jun-2016  chuck@ece.cmu.edu
#

#
# deltafs is ... XXX
#

#
# configuration/build:
#   - choose a build directory and "cd" to it
#   - cmake [flags] directory
#   - make
#
#  where directory is the top-level source directory where this file resides.
#
#  general cmake flags:
#    -DCMAKE_INSTALL_PREFIX=/usr/local     -- the prefix for installing
#    -DCMAKE_BUILD_TYPE=type               -- type can be DEBUG, RELEASE, ...
#    -DCMAKE_PREFIX_PATH=/dir              -- external packages
#    -DBUILD_SHARED_LIBS=OFF               -- turn ON for shared libs
#
#     note that CMAKE_PREFIX_PATH can be a list of directories:
#      -DCMAKE_PREFIX_PATH='/dir1;/dir2;/dir3'
#
# general PDLFS config compile time options flags:
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
#
# DELTAFS specific compile time options flags:
#   -DDELTAFS_MPI=ON                       -- enable MPI
#
#    If you want to force a particular MPI compiler other than what we
#    autodetect (e.g. if you want to compile regular stuff with GNU and
#    parallel stuff with Intel), you can set your favorite
#    MPI_<lang>_COMPILER explicitly).
#

# note: package config files for external packages must be preinstalled in
#       CMAKE_INSTALL_PATH or on CMAKE_PREFIX_PATH, except as noted.
#

cmake_minimum_required (VERSION 2.8.12)

project (DELTAFS)

# link shared lib with full rpath
set (CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# quiet CMP0042 warning
set (CMAKE_MACOSX_RPATH 1)

enable_testing ()

#
# we compile everything with -DDELTAFS.  we also set the common library's
# name to deltafs-common (since we may add deltafs-specific code to it).
# we request (but don't require) C++ 11 standard for possible performance
# improvements due it its move semantics.
#
add_definitions (-DDELTAFS)
set (PDLFS_COMMON_LIBNAME "deltafs-common")
set (CMAKE_CXX_STANDARD 11)

if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release
         CACHE STRING "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS 
                  "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (DEBUG_SANITIZER Off CACHE STRING "Sanitizer for debug builds")
set_property (CACHE DEBUG_SANITIZER PROPERTY STRINGS
              "Off" "Address" "Thread")
set (CMAKE_PREFIX_PATH "" CACHE STRING "External dependencies path")
set (BUILD_SHARED_LIBS "OFF" CACHE BOOL "Build a shared library")
set (BUILD_TESTS "OFF" CACHE BOOL "Build test programs")

# user hooks to configure pdlfs-common
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

set (DELTAFS_MPI "OFF" CACHE
     BOOL "Enable DELTAFS MPI-based communication")

#
# external packages
#
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/external/pdlfs-common/cmake")
include (xpkg-import)

if (PDLFS_GFLAGS)
    find_package(gflags REQUIRED)
    if (NOT GFLAGS_FOUND AND NOT gflags_FOUND)
        message (FATAL_ERROR
        "-- gflags not found try setting -D GFLAGS_{INCLUDE,LIBRARY}_DIR")
    endif ()
endif ()

if (PDLFS_GLOG)
    xdual_import (glog::glog,glog,libglog REQUIRED)
endif ()

if (PDLFS_MERCURY_RPC)
    find_package(mercury CONFIG REQUIRED)
endif ()

if (PDLFS_MARGO_RPC)
    xpkg_import_module (margo REQUIRED margo)
endif ()

if (PDLFS_RADOS)
    find_package(RADOS MODULE REQUIRED)
    if (NOT RADOS_FOUND)
        message (FATAL_ERROR
        "-- rados not found try setting -D RADOS_{INCLUDE,LIBRARY}_DIR")
    endif ()
endif ()

if (PDLFS_SNAPPY)
    find_package(Snappy MODULE REQUIRED)
    if (NOT SNAPPY_FOUND)
        message (FATAL_ERROR
        "-- snappy not found try setting -D SNAPPY_{INCLUDE,LIBRARY}_DIR")
    endif ()
endif ()

if (DELTAFS_MPI)
    find_package(MPI REQUIRED MODULE)
endif ()

#
# sanitizer config (XXX: does not probe compiler to see if sanitizer flags
# are supported... )
#
set (as_flags "-fsanitize=address -O1 -fno-omit-frame-pointer")
set (ts_flags "-fsanitize=thread  -O1 -fno-omit-frame-pointer")
if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    if (${DEBUG_SANITIZER} STREQUAL "Address")
        set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${as_flags}")
        set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${as_flags}")
    elseif (${DEBUG_SANITIZER} STREQUAL "Thread")
        set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${ts_flags}")
        set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${ts_flags}")
    endif ()
endif ()

add_subdirectory (external/pdlfs-common/src)
add_subdirectory (src)

