#
# CMakeLists.txt  cmake file for src directory
# 16-Jun-2016  chuck@ece.cmu.edu
#

#
# this file is either included from ../CMakeLists.txt or some other
# file if we are being embedded within another project.
#

# configure/load in standard modules we plan to use
set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package (Threads REQUIRED)
include (CheckCXXCompilerFlag)

#
# library version set here (e.g. for generating shared libs if we want
#
set (DELTAFS_VERSION_MAJOR 1)
set (DELTAFS_VERSION_MINOR 0)
set (DELTAFS_VERSION_PATCH 0)

#
# list of source files and tests
#

# main directory sources and tests
set (deltafs-srcs deltafs_api.cc gigaplus.cc xxhash.cc)
set (deltafs-tests gigaplus_test.cc)

#
# here's the compiler/machine/os dependent stuff...
#

set (deltafs-try-common-flags -Wpedantic -Wno-long-long -Wall
                              -Wno-sign-compare -pthread)
foreach (lcv ${deltafs-try-common-flags})
    CHECK_CXX_COMPILER_FLAG (${lcv} flag${lcv})
    if (${flag${lcv}})
        add_compile_options (${lcv})
    endif ()
endforeach ()

# "-pthread" should take care of all thread related options.
# but if we don't have it, fall back to -D_REENTRANT
if (NOT flag-pthread)
    add_definitions (-D_REENTRANT)
endif ()

# XXX: untested solaris, does it need "-mt"?
# XXX: leveldb also added "-lrt" .. but is that really needed?
if (${CMAKE_SYSTEM_NAME} STREQUAL "SunOS")
    CHECK_CXX_COMPILER_FLAG (-mt flag-mt)
    if (${flag-mt})
        add_compile_options (-mt)
    endif ()
endif ()

#
# end of the compiler/machine/os dependent stuff!
#

#
# generate complete list of files and tests from the parts
#
set (deltafs-all-srcs ${deltafs-srcs})
set (deltafs-all-tests ${deltafs-tests})

#
# build static lib and arrange for targets to -I our include dir 
#
add_library (deltafs-static STATIC ${deltafs-all-srcs})
target_include_directories (deltafs-static PUBLIC "../include")
target_link_libraries (deltafs-static pdlfs-common-static)
if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries (deltafs-static "${CMAKE_THREAD_LIBS_INIT}")
endif ()

#
# build shared lib and arrange for targets to -I our include dir 
#
set(CMAKE_MACOSX_RPATH 1)
add_library (deltafs SHARED ${deltafs-all-srcs})
 target_include_directories (deltafs PUBLIC "../include")
target_link_libraries (deltafs pdlfs-common)
if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries (deltafs "${CMAKE_THREAD_LIBS_INIT}")
endif ()
set (deltafs-vers
    "${DELTAFS_VERSION_MAJOR}.${DELTAFS_VERSION_MINOR}")
set (deltafs-vers
    "${deltafs-vers}.${DELTAFS_VERSION_PATCH}")
set_target_properties(deltafs
                      PROPERTIES VERSION ${deltafs-vers}
                      SOVERSION ${DELTAFS_VERSION_MAJOR})

#
# "make install" rules
#
install (TARGETS deltafs-static deltafs
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
install (DIRECTORY ../include/deltafs
         DESTINATION include
         FILES_MATCHING PATTERN "*.h")

#
# tests... we EXCLUDE_FROM_ALL the tests and add a custom target
# "pdl-build-tests" to build tests (we prefix with "pdl-" since
# custom targets must be globally unique).   if the top-level
# CMakeLists.txt calls enable_testing (), then you can "make pdl-build-tests"
# and then "make test" (or run ctest directly).
#
if (NOT TARGET pdl-build-tests)           # can only define it once
    add_custom_target (pdl-build-tests)
endif ()
foreach (lcv ${deltafs-all-tests})

    # need a test name... use regex to extract the file basename into ${id}
    string (REGEX REPLACE ".*/" "" id ${lcv})
    string (REGEX REPLACE "([A-Za-z0-9_]+)\\.c+$" "\\1" id ${id})

    add_executable (${id} EXCLUDE_FROM_ALL ${lcv})
    target_link_libraries (${id} deltafs)
    add_test (${id} ${id})
    add_dependencies (pdl-build-tests ${id})

endforeach ()

