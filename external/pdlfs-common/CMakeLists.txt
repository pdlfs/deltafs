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
#  where directory is the top-level source directory where this file
#  resides.   useful flags:
#
#    -DCMAKE_INSTALL_PREFIX=/usr/local     -- the prefix for installing
#    -DCMAKE_BUILD_TYPE=type               -- type can be DEBUG, RELEASE, ...
#    -DPDLFS_PLATFORM=POSIX                -- currently only posix
#    -DPDLFS_OS=Linux                      -- uname -s for target
#

cmake_minimum_required (VERSION 2.8) # uses target_include_directories from 2.8

project (PDLFS-COMMON)

set (PDLFS_PLATFORM "POSIX" CACHE STRING "Select platform (e.g. POSIX)")
set (PDLFS_OS "${CMAKE_SYSTEM_NAME}" CACHE 
              STRING "Select operating system (uname -s)")

add_subdirectory (src)


#XXXCDC: check install
