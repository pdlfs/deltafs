#
# CMakeLists.txt  cmake file for src directory
# 26-May-2016  chuck@ece.cmu.edu
#

#
# this file is either included from ../CMakeLists.txt or some other
# file if we are being embedded within another project.
#

#
# list of source files and tests
#

# main directory sources and tests
set (pdlfs-common-srcs arena.cc cache.cc coding.cc crc32c.cc dbfiles.cc
     dbfiles.cc dcntl.cc env.cc fio.cc fstypes.cc gigaplus.cc hash.cc
     histogram.cc index_cache.cc lease.cc log_reader.cc log_writer.cc
     logging.cc lookup_cache.cc mdb.cc mercury_rpc.cc
     osd.cc osd_internal.cc port_posix.cc posix_env.cc rpc.cc
     slice.cc status.cc strutil.cc testharness.cc testutil.cc xxhash.cc)
set (pdlfs-common-tests arena_test.cc cache_test.cc coding_test.cc
     crc32c_test.cc dbfiles_test.cc env_test.cc fstypes_test.cc
     gigaplus_test.cc hash_test.cc log_test.cc mercury_test.cc osd_test.cc)

# leveldb directory sources and tests
set (pdlfs-leveldb-srcs block.cc block_builder.cc bloom.cc comparator.cc
     db/builder.cc db/db.cc db/db_impl.cc db/db_iter.cc db/dbformat.cc
     db/memtable.cc db/options.cc db/repair.cc db/table_cache.cc
     db/version_edit.cc db/version_set.cc db/write_batch.cc filter_block.cc
     filter_policy.cc format.cc iterator.cc merger.cc table.cc
     table_builder.cc table_stats.cc two_level_iterator.cc )
set (pdlfs-leveldb-tests bloom_test.cc db/autocompact_test.cc
     db/bulk_test.cc db/corruption_test.cc db/db_table_test.cc db/db_test.cc
     db/dbformat_test.cc db/version_edit_test.cc db/version_set_test.cc
     db/write_batch_test.cc filter_block_test.cc skiplist_test.cc
     table_test.cc)

#
# configure/load in standard modules we plan to use and probe the enviroment
#
include (CMakePackageConfigHelpers)
include (CheckCXXCompilerFlag)

set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package (Threads REQUIRED)

if (TARGET mercury AND PDLFS_MERCURY_RPC)
    list (APPEND PDLFS_REQUIRED_PACKAGES mercury)   # for *-config.cmake
    list (APPEND pdlfs-xtra-libs mercury)           # for build
    # XXX: mercury doesn't set its INTERFACE_INCLUDE_DIRECTORIES property
    # XXX: so we have to do it here...
    list (APPEND pdlfs-xtra-incs ${MERCURY_INCLUDE_DIR})
endif ()

if (TARGET rados AND PDLFS_RADOS)
    list (APPEND pdlfs-xtra-libs rados)
    list (APPEND PDLFS_EXTRA_COMPONENTS rados)
endif ()

#
# we allow the library binary to be installed and "find_package"'d 
# under an alternate name (though header files are still under pdlfs-common).
# we set "PDLFS_NAME" to the name we are going to use.
#
if (PDLFS_COMMON_LIBNAME)
    set (PDLFS_NAME ${PDLFS_COMMON_LIBNAME})        # user specified name
else ()
    set (PDLFS_NAME "pdlfs-common")                 # default name
endif ()

# where to install files for "find_package"
set (pdlfs-pkg-loc "share/cmake/${PDLFS_NAME}")

#
# library version set here (e.g. for shared libs).  we allow our container
# to choose the version, or it can just take the default.
#
if (NOT DEFINED PDLFS_COMMON_VERSION_MAJOR)
    set (PDLFS_COMMON_VERSION_MAJOR 1)
    set (PDLFS_COMMON_VERSION_MINOR 0)
    set (PDLFS_COMMON_VERSION_PATCH 0)
endif ()
set (pdlfs-vers "${PDLFS_COMMON_VERSION_MAJOR}.${PDLFS_COMMON_VERSION_MINOR}")
set (PDLFS_COMMON_VERSION "${pdlfs-vers}.${PDLFS_COMMON_VERSION_PATCH}")

#
# provide reasonable defaults for platform/os
#
if (NOT PDLFS_PLATFORM)
  set (PDLFS_PLATFORM POSIX)
endif ()
if (NOT PDLFS_OS)
  set (PDLFS_OS "${CMAKE_SYSTEM_NAME}")
endif ()

#
# upper case and edit PDLFS_OS to match the old build system uses
# (since some of the code still expects it... XXX: cleanup later)
#
string (TOUPPER ${PDLFS_OS} PDLFS_OS_EDITED)
if (${PDLFS_OS_EDITED} STREQUAL "DARWIN")
  # XXX should switch MACOSX to DARWIN
  set (PDLFS_OS_EDITED "MACOSX")
elseif (${PDLFS_OS_EDITED} STREQUAL "SUNOS")
  # XXX should switch SOLARIS to SUNOS
  set (PDLFS_OS_EDITED "SOLARIS")
elseif (${PDLFS_OS_EDITED} STREQUAL "HP-UX")
  set (PDLFS_OS_EDITED "HPUX")
elseif (${PDLFS_OS_EDITED} MATCHES "^CYGWIN_")
  set (PDLFS_OS_EDITED "CYGWIN")
  set (PDLFS_OS_LINUX 1)          # yuck, inherited it from leveldb
endif ()

#
# setup and configure pdlfs_config_expand.h.in and pdlfs_platform_expand.h.in
#
set (PDLFS_PLATFORM_${PDLFS_PLATFORM} 1)
set (PDLFS_OS_${PDLFS_OS_EDITED} 1)
configure_file ("../include/pdlfs-common/pdlfs_config_expand.h.in"
                "../include/pdlfs-common/${PDLFS_NAME}_config_expand.h" @ONLY)
configure_file ("../include/pdlfs-common/pdlfs_platform_expand.h.in"
                "../include/pdlfs-common/pdlfs_platform_expand.h" @ONLY)

#
# prepend the subdir name to files that live in subdirs (e.g. leveldb/)
#
string(REGEX REPLACE "([^;]+)" "leveldb/\\1" pdlfs-leveldb-srcs
                                          "${pdlfs-leveldb-srcs}")
string(REGEX REPLACE "([^;]+)" "leveldb/\\1" pdlfs-leveldb-tests
                                          "${pdlfs-leveldb-tests}")

#
# generate complete list of files and tests from the parts
#
set (pdlfs-all-srcs ${pdlfs-common-srcs} ${pdlfs-leveldb-srcs})
set (pdlfs-all-tests ${pdlfs-common-tests} ${pdlfs-leveldb-tests})

#
# create the library target (user can specify shared vs. static
# using BUILD_SHARED_LIBS).   arrange for users of our lib to
# get the correct -Is for our include directories.  (for building
# we need a -I pointing to the build dir for the expanded *.h.in files)
#
add_library (pdlfs-common ${pdlfs-all-srcs})
set_target_properties (pdlfs-common PROPERTIES OUTPUT_NAME "${PDLFS_NAME}")
target_compile_definitions (pdlfs-common PUBLIC -DPDLFS_CONFIG=${PDLFS_NAME})
target_include_directories (pdlfs-common PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/../include>
    $<INSTALL_INTERFACE:include>)
if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries (pdlfs-common "${CMAKE_THREAD_LIBS_INIT}")
endif ()

target_link_libraries (pdlfs-common ${pdlfs-xtra-libs})
# XXX: for mercury
target_include_directories (pdlfs-common PRIVATE ${pdlfs-xtra-incs})

set_target_properties(pdlfs-common
                      PROPERTIES VERSION ${PDLFS_COMMON_VERSION}
                      SOVERSION ${PDLFS_COMMON_VERSION_MAJOR})

#
# here's the compiler/machine/os dependent stuff...
#

# XXX: leveldb build_detect_platform complains about gcc 4.4.3 memcmp
# XXX: we duplicate the behavior, but assume that this isn't a problem
# XXX: with newer compilers
if (${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU")
    if (${CMAKE_CXX_COMPILER_VERSION} VERSION_LESS 4.4.4)
        target_compile_options (pdlfs-common PRIVATE -fno-builtin-memcmp)
        message ("**** NOTE: adding -fno-builtin-memcmp for old gcc")
    endif ()
endif ()

set (pdlfs-try-common-flags -Wpedantic -Wno-long-long -Wall
                            -Wno-sign-compare -pthread)
foreach (lcv ${pdlfs-try-common-flags})
    CHECK_CXX_COMPILER_FLAG (${lcv} flag${lcv})
    if (${flag${lcv}})
        target_compile_options (pdlfs-common PRIVATE ${lcv})
    endif ()
endforeach ()

# "-pthread" should take care of all thread related options.
# but if we don't have it, fall back to -D_REENTRANT
if (NOT flag-pthread)
    target_compile_definitions (pdlfs-common PUBLIC -D_REENTRANT)
endif ()

# XXX: untested solaris, does it need "-mt"?
# XXX: leveldb also added "-lrt" .. but is that really needed?
if (${PDLFS_OS_EDITED} STREQUAL "SOLARIS")
    CHECK_CXX_COMPILER_FLAG (-mt flag-mt)
    if (${flag-mt})
        target_compile_options (pdlfs-common PUBLIC -mt)
    endif ()
endif ()

#
# end of the compiler/machine/os dependent stuff!
#

#
# installation stuff (packaging and install commands)
#
write_basic_package_version_file(
    "${PDLFS_NAME}-config-version.cmake"
    VERSION ${PDLFS_COMMON_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
configure_file (pdlfs-common-config.cmake.in ${PDLFS_NAME}-config.cmake @ONLY)

#
# "make install" rules
#
install (TARGETS pdlfs-common EXPORT pdlfs-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
install (EXPORT pdlfs-targets
         DESTINATION ${pdlfs-pkg-loc}
         FILE "${PDLFS_NAME}-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/${PDLFS_NAME}-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/${PDLFS_NAME}-config-version.cmake"
         DESTINATION ${pdlfs-pkg-loc} )
install (DIRECTORY ../include/pdlfs-common
         DESTINATION include
         FILES_MATCHING PATTERN "*.h")
install (FILES
  "${CMAKE_CURRENT_BINARY_DIR}/../include/pdlfs-common/${PDLFS_NAME}_config_expand.h"
  "${CMAKE_CURRENT_BINARY_DIR}/../include/pdlfs-common/pdlfs_platform_expand.h"
         DESTINATION include/pdlfs-common)

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
foreach (lcv ${pdlfs-all-tests})

    # need a test name... use regex to extract the file basename into ${id}
    string (REGEX REPLACE ".*/" "" id ${lcv})
    string (REGEX REPLACE "([A-Za-z0-9_]+)\\.c+$" "\\1" id ${id})

    add_executable (${id} EXCLUDE_FROM_ALL ${lcv})
    target_link_libraries (${id} pdlfs-common)
    add_test (${id} ${id})
    add_dependencies (pdl-build-tests ${id})

endforeach ()

# mercury test also directly uses mercury API... add that to test target
if (TARGET mercury_test AND PDLFS_MERCURY_RPC)
    target_include_directories (mercury_test PRIVATE ${MERCURY_INCLUDE_DIR})
    target_link_libraries (mercury_test mercury)
endif ()

