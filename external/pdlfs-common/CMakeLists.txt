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
#   -DPDLFS_MERCURY_RPC=ON                 -- compile in mercury rpc code
#   -DPDLFS_RADOS=ON                       -- compile in RADOS env
#     - RADOS_INCLUDE_DIR: optional hint for finding rado/librados.h
#     - RADOS_LIBRARY_DIR: optional hint for finding rados lib
#
#
# note: package config files for external packages must be preinstalled in 
#       CMAKE_INSTALL_PATH or on CMAKE_PREFIX_PATH, except as noted.
#

#
# PDLFS_COMMON_LIBNAME allows clients to do custom compile-time configuration
# of the library and install the customized version under an alternate name
# (e.g. lib/libdeltafs-common.a instead of lib/pdlfs-common.a).  note that
# the include files are still shared under include/pdlfs-common.
#

cmake_minimum_required (VERSION 2.8) # uses target_include_directories from 2.8

project (PDLFS-COMMON)

# link shared lib with rpath
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# quiet CMP0042 warning
set (CMAKE_MACOSX_RPATH 1)

enable_testing ()

set (PDLFS_PLATFORM "POSIX" CACHE STRING "Select platform (e.g. POSIX)")
set (PDLFS_OS "${CMAKE_SYSTEM_NAME}" CACHE 
              STRING "Select operating system (uname -s)")

#
# external packages
#
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

if (PDLFS_MERCURY_RPC)
    find_package(mercury CONFIG REQUIRED)
    message ("-- mercury enabled")
endif ()

if (PDLFS_RADOS)
    find_package(Rados MODULE REQUIRED)
    if (NOT RADOS_FOUND)
        message (FATAL_ERROR
        "-- rados not found try setting -D RADOS_{INCLUDE,LIBRARY}_DIR")
    endif ()
    message ("-- rados enabled")
endif ()

add_subdirectory (src)

