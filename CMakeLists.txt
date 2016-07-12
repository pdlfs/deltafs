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
#  where directory is the top-level source directory where this file
#  resides.   useful flags:
#
#    -DCMAKE_INSTALL_PREFIX=/usr/local     -- the prefix for installing
#    -DCMAKE_BUILD_TYPE=type               -- type can be DEBUG, RELEASE, ...
#

cmake_minimum_required (VERSION 2.8) # uses target_include_directories from 2.8

project (DELTAFS)

#
# we compile everything with -DDELTAFS.  we also set the common library's
# name to deltafs-common (since we may add deltafs-specific code to it).
#
add_definitions (-DDELTAFS)
set (PDLFS_COMMON_LIBNAME "deltafs-common")

#
# add the automatically determined parts of the RPATH
# which point to directories outside the build tree to the install RPATH
#
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

#
# determine if ${CMAKE_INSTALL_PREFIX}/lib is a system directory by
# searching for it in CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES.
# if it isn't a system directory, then add it to the rpath so apps 
# will run without having to set something like LD_LIBRARY_PATH ...
#
list (FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES 
            "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)
if ("${isSystemDir}" STREQUAL "-1")       # -1 means it wasn't find
   set (CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
endif()

enable_testing ()

add_subdirectory (external/pdlfs-common/src)

add_subdirectory (src)

