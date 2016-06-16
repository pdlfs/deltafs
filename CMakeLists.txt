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

enable_testing ()
if (NOT TARGET check)
    add_custom_target (check COMMAND ${CMAKE_CTEST_COMMAND})
endif ()

add_subdirectory (external/pdlfs-common/src)

add_subdirectory (src)

#XXXCDC: check install
