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
# config compile time options flags:
#   -DPDLFS_MARGO_RPC=ON                   -- compile in margo rpc code
#   -DPDLFS_MERCURY_RPC=ON                 -- compile in mercury rpc code
#   -DPDLFS_RADOS=ON                       -- compile in RADOS env
#     - RADOS_INCLUDE_DIR: optional hint for finding rado/librados.h
#     - RADOS_LIBRARY_DIR: optional hint for finding rados lib
#
#
# note: package config files for external packages must be preinstalled in
#       CMAKE_INSTALL_PATH or on CMAKE_PREFIX_PATH, except as noted.
#

cmake_minimum_required (VERSION 2.8) # uses target_include_directories from 2.8

project (DELTAFS)

# link shared lib with rpath
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# quiet CMP0042 warning
set (CMAKE_MACOSX_RPATH 1)

enable_testing ()

#
# we compile everything with -DDELTAFS.  we also set the common library's
# name to deltafs-common (since we may add deltafs-specific code to it).
#
add_definitions (-DDELTAFS)
set (PDLFS_COMMON_LIBNAME "deltafs-common")

if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release
         CACHE STRING "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS 
                  "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (CMAKE_PREFIX_PATH "" CACHE STRING "External dependencies path")
set (BUILD_SHARED_LIBS "OFF" CACHE BOOL "Build a shared library")
set (BUILD_TESTS "OFF" CACHE BOOL "Build test programs")

# user hooks to configure pdlfs-common
set (PDLFS_MARGO_RPC "OFF" CACHE
     BOOL "Compile in Margo/abt-snoozer/argobots RPC interface")
set (PDLFS_MERCURY_RPC "OFF" CACHE
     BOOL "Compile in Mercury RPC interface")
set (PDLFS_RADOS "OFF" CACHE
     BOOL "Compile in RADOS object store")

#
# external packages
#
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/external/pdlfs-common/cmake")
include (xpkg-import)

if (PDLFS_MERCURY_RPC)
    find_package(mercury CONFIG REQUIRED)
    message ("-- mercury rpc enabled")
endif ()

if (PDLFS_MARGO_RPC)
    xpkg_import_module (margo REQUIRED margo)
    message ("-- margo rpc enabled")
endif ()

if (PDLFS_RADOS)
    find_package(RADOS MODULE REQUIRED)
    if (NOT RADOS_FOUND)
        message (FATAL_ERROR
        "-- rados not found try setting -D RADOS_{INCLUDE,LIBRARY}_DIR")
    endif ()
    message ("-- rados enabled")
endif ()

add_subdirectory (external/pdlfs-common/src)
add_subdirectory (src)

