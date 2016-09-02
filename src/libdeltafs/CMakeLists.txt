#
# CMakeLists.txt  cmake file for libdeltafs directory
# 16-Jun-2016  chuck@ece.cmu.edu
#

#
# this file is either included from ../CMakeLists.txt or some other
# file if we are being embedded within another project.
#

# configure/load in standard modules we plan to use
include (CMakePackageConfigHelpers)
include (CheckCXXCompilerFlag)
set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package (Threads REQUIRED)

# where the cmake package files get installed (for find_package)
set (dfs-pkg-loc "share/cmake/deltafs")

#
# library version set here (e.g. for generating shared libs if we want
#
set (DELTAFS_VERSION_MAJOR 1)
set (DELTAFS_VERSION_MINOR 0)
set (DELTAFS_VERSION_PATCH 0)
set (dfs-vers "${DELTAFS_VERSION_MAJOR}.${DELTAFS_VERSION_MINOR}")
set (dfs-vers "${dfs-vers}.${DELTAFS_VERSION_PATCH}")

#
# list of source files and tests
#

# main directory sources and tests
set (deltafs-srcs deltafs_api.cc deltafs_client.cc deltafs_conf.cc
     deltafs_mds.cc mds.cc mds_api.cc mds_cli.cc mds_factory.cc mds_srv.cc)
set (deltafs-tests mds_api_test.cc mds_srv_test.cc)

#
# generate complete list of files and tests from the parts
#
set (deltafs-all-srcs ${deltafs-srcs})
set (deltafs-all-tests ${deltafs-tests})

#
# create the library target (user can specify shared vs. static
# using BUILD_SHARED_LIBS).   arrange for users of our lib to
# get the correct -Is for our include directories.  (for building
# we need a -I pointing to the build dir for pdlfs_config_expand.h)
#
add_library (deltafs ${deltafs-all-srcs})
target_include_directories (deltafs PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/../../include>
    $<INSTALL_INTERFACE:include>)
target_link_libraries (deltafs pdlfs-common)
if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries (deltafs "${CMAKE_THREAD_LIBS_INIT}")
endif ()
set_target_properties(deltafs
                      PROPERTIES VERSION ${dfs-vers}
                      SOVERSION ${DELTAFS_VERSION_MAJOR})

#
# here's the compiler/machine/os dependent stuff...
#

set (deltafs-try-common-flags -Wpedantic -Wno-long-long -Wall
                              -Wno-sign-compare -pthread)
foreach (lcv ${deltafs-try-common-flags})
    CHECK_CXX_COMPILER_FLAG (${lcv} flag${lcv})
    if (${flag${lcv}})
        target_compile_options (deltafs PRIVATE ${lcv})
    endif ()
endforeach ()

# "-pthread" should take care of all thread related options.
# but if we don't have it, fall back to -D_REENTRANT
if (NOT flag-pthread)
    target_compile_definitions (deltafs PUBLIC -D_REENTRANT)
endif ()

# XXX: untested solaris, does it need "-mt"?
# XXX: leveldb also added "-lrt" .. but is that really needed?
if (${CMAKE_SYSTEM_NAME} STREQUAL "SunOS")
    CHECK_CXX_COMPILER_FLAG (-mt flag-mt)
    if (${flag-mt})
        target_compile_options (deltafs PUBLIC -mt)
    endif ()
endif ()

#
# end of the compiler/machine/os dependent stuff!
#

#
# installation stuff (packaging and install commands)
#
write_basic_package_version_file(
    "deltafs-config-version.cmake"
    VERSION ${dfs-vers}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
configure_file (deltafs-config.cmake.in deltafs-config.cmake @ONLY)

# "make install" rules...
install (TARGETS deltafs EXPORT deltafs-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
install (EXPORT deltafs-targets
         DESTINATION ${dfs-pkg-loc})
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/deltafs-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/deltafs-config-version.cmake"
         DESTINATION ${dfs-pkg-loc})
install (DIRECTORY ../../include/deltafs
         DESTINATION include
         FILES_MATCHING PATTERN "*.h")

#
# tests... we EXCLUDE_FROM_ALL the tests and use pdlfs-common's
# pdl-build-tests target for building.
#
foreach (lcv ${deltafs-all-tests})

    # need a test name... use regex to extract the file basename into ${id}
    string (REGEX REPLACE ".*/" "" id ${lcv})
    string (REGEX REPLACE "([A-Za-z0-9_]+)\\.c+$" "\\1" id ${id})

    add_executable (${id} EXCLUDE_FROM_ALL ${lcv})
    target_link_libraries (${id} deltafs)
    add_test (${id} ${id})
    add_dependencies (pdl-build-tests ${id})

endforeach ()

