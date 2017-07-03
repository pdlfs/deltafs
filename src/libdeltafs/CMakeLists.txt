#
# CMakeLists.txt  cmake file for libdeltafs directory
# 16-Jun-2016  chuck@ece.cmu.edu
#

#
# this file is either included from ../CMakeLists.txt or some other
# file if we are being embedded within another project.
#

#
# list of source files and tests
#

# main directory sources and tests
set (deltafs-srcs deltafs_api.cc deltafs_client.cc deltafs_conf.cc
     deltafs_mds.cc deltafs_plfsio.cc deltafs_plfsio_internal.cc
     deltafs_plfsio_format.cc deltafs_plfsio_batch.cc
     deltafs_plfsio_log.cc deltafs_plfsio_events.cc
     deltafs_envs.cc mds.cc mds_api.cc mds_cli.cc mds_factory.cc
     mds_srv.cc snap_stor.cc)

set (deltafs-tests deltafs_plfsio_test mds_api_test.cc mds_srv_test.cc)

# configure/load in standard modules we plan to use
include (CMakePackageConfigHelpers)
set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package (Threads REQUIRED)

# common lib is always required
set (DELTAFS_REQUIRED_PACKAGES "deltafs-common")

# optional deltafs-bb
if (DELTAFS_BBOS)
    list (APPEND deltafs-srcs bbos_env.cc)
    list (APPEND DELTAFS_REQUIRED_PACKAGES bbos)
endif ()


if (DELTAFS_MPI)
    list (APPEND DELTAFS_REQUIRED_PACKAGES MPI)
    # MPI isn't a normal imported target, so sadly it requires special
    # handling...  see below.
endif ()

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

# setup and configure deltafs_config.h.in
configure_file ("../../include/deltafs/deltafs_config.h.in"
                "../../include/deltafs/deltafs_config.h" @ONLY)

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
target_include_directories (deltafs PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (deltafs BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/../../include>)
target_link_libraries (deltafs deltafs-common)
if (DELTAFS_BBOS)
    target_compile_definitions(deltafs PUBLIC "-DDELTAFS_BBOS")
    target_link_libraries (deltafs bbos)
endif ()

# special handling for MPI, where the config comes in via MPI_<lang>_ vars.
# we only add to the build interface so that we don't put hardcoded paths
# in the config files.
#
if (DELTAFS_MPI)
    # mpich on ub14 gives a leading space that we need to trim off
    string (REPLACE " " ";" mpicxx_flags "${MPI_CXX_COMPILE_FLAGS}")
    foreach (lcv ${mpicxx_flags})
        if (NOT ${lcv} STREQUAL "")
            target_compile_options (deltafs PUBLIC $<BUILD_INTERFACE:${lcv}>)
        endif ()
    endforeach ()

    # XXX: have to do this one dir at a time otherwise, otherwise I get
    # error: target 'deltafs' INTERFACE_INCLUDE_DIRECTORIES contains path
    #         prefixed in the source directory
    foreach (lcv ${MPI_CXX_INCLUDE_PATH})
        target_include_directories (deltafs PUBLIC $<BUILD_INTERFACE:${lcv}>)
    endforeach ()
    foreach (lcv ${MPI_CXX_LIBRARIES})
        target_link_libraries(deltafs $<BUILD_INTERFACE:${lcv}>)
    endforeach ()
    # XXX: this doesn't propagate to lib users, is that a problem?
    # XXX: prob ok.
    set_property (TARGET deltafs APPEND PROPERTY LINK_FLAGS
                  ${MPI_CXX_LINK_FLAGS})
    get_property (link_flags TARGET deltafs PROPERTY LINK_FLAGS)
endif ()

if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries (deltafs "${CMAKE_THREAD_LIBS_INIT}")
endif ()
set_target_properties(deltafs
                      PROPERTIES VERSION ${dfs-vers}
                      SOVERSION ${DELTAFS_VERSION_MAJOR})

#
#
# here's the compiler/machine/os dependent stuff...
# this likely isn't that useful (carryover from leveldb)
#

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
install (FILES
   "${CMAKE_CURRENT_BINARY_DIR}/../../include/deltafs/deltafs_config.h"
   DESTINATION include/deltafs)

#
# tests... we EXCLUDE_FROM_ALL the tests and use pdlfs-options.cmake's
# pdl-build-tests target for building.
#
foreach (lcv ${deltafs-all-tests})

    # use basename as test name
    get_filename_component (id ${lcv} NAME_WE)

    add_executable (${id} EXCLUDE_FROM_ALL ${lcv})
    target_link_libraries (${id} deltafs)
    add_test (${id} ${id})
    add_dependencies (pdl-build-tests ${id})

endforeach ()

