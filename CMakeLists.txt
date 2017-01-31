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
#   -DPDLFS_VERBOSE=1                      -- set max log verbose level
#
# DELTAFS specific compile time options flags:
#   -DDELTAFS_BENCHMARKS=ON                -- build our MPI-based benchmarks
#   -DDELTAFS_MPI=ON                       -- enable MPI in deltafs
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

# add pdlfs-common cmake module directory to the path
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/external/pdlfs-common/cmake")

#
# we compile everything with -DDELTAFS by attaching it as a property of
# the common lib.  we also set the common library's name to deltafs-common
# (since we may add deltafs-specific code to it)  we request (but don't
# require) C++ 11 standard for possible performance improvements due it
# its move semantics.
#
set (PDLFS_COMMON_LIBNAME "deltafs-common" CACHE
     STRING "Custom name to install pdlfs-common with")
set (PDLFS_COMMON_DEFINES "DELTAFS" CACHE
     STRING "Additional defines for this version of pdlfs-common")
mark_as_advanced (PDLFS_COMMON_LIBNAME PDLFS_COMMON_DEFINES)
set (CMAKE_CXX_STANDARD 11)

# pull in pdlfs handling of generic cmake config
include (cmake-options)

# handle all the common PDLFS options using cmake/pdlfs-options.cmake
include (pdlfs-options)

# user hooks to configure deltafs
set (DELTAFS_BENCHMARKS "OFF" CACHE BOOL "Build benchmarks (requires MPI)")
set (DELTAFS_MPI "OFF" CACHE
     BOOL "Enable DELTAFS MPI-based communication")

#
# external packages
#
if (DELTAFS_MPI OR DELTAFS_BENCHMARKS)
    find_package(MPI MODULE)
    if (NOT MPI_FOUND)
        message (FATAL_ERROR "MPI not found (required for DELTAFS_MPI or benchmarks)")
    endif ()
endif ()

add_subdirectory (external/pdlfs-common/src)
add_subdirectory (src)
if (DELTAFS_BENCHMARKS)
    add_subdirectory (benchmarks)
endif ()
