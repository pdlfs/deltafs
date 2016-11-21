#
# CMakeLists.txt  top-level cmake file for the pdlfs-common library
# 26-May-2016  chuck@ece.cmu.edu
#

#
# pdlfs-common is a lower-level utility library that is shared among
# pdlfs projects.  it can be compiled stand-alone or as a git subrepo
# within another git repository by adding "src" as a subdirectory.
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
# pdlfs-common config flags:
#   -DPDLFS_PLATFORM=POSIX                 -- platform (currently only posix)
#   -DPDLFS_OS=Linux                       -- "uname -s" for target
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
#
#
# note: package config files for external packages must be preinstalled in 
#       CMAKE_INSTALL_PATH or on CMAKE_PREFIX_PATH, except as noted.
#
# pdlfs-common extras:
#   -DPDLFS_EXAMPLES=ON                    -- build example programs
#

#
# PDLFS_COMMON_LIBNAME allows clients to do custom compile-time configuration
# of the library and install the customized version under an alternate name
# (e.g. lib/libdeltafs-common.a instead of lib/pdlfs-common.a).  note that
# the include files are still shared under include/pdlfs-common.
#

cmake_minimum_required (VERSION 2.8) # uses target_include_directories from 2.8

project (PDLFS-COMMON)

# add our cmake module directory to the path
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# pull in pdlfs handling of generic cmake config
include (cmake-options)

# handle all the common PDLFS options using cmake/pdlfs-options.cmake
include (pdlfs-options)

# local options
set (PDLFS_EXAMPLES "OFF" CACHE BOOL "Build examples programs")
set (PDLFS_TOOLS "OFF" CACHE BOOL "Enable pdlfs tool programs")

add_subdirectory (src)

if (PDLFS_EXAMPLES)
add_subdirectory (examples)
endif ()

if (PDLFS_TOOLS)
add_subdirectory (tools)
endif ()
