#
# CMakeLists.txt  cmake file for server directory
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
# list of source files and tests
#

# main directory sources and tests
set (deltafs-srvr-srcs deltafs_server.cc)
set (deltafs-srvr-tests )

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
# build server
#
add_executable (deltafs-srvr ${deltafs-srvr-srcs})
target_link_libraries (deltafs-srvr deltafs)
if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries (deltafs-srvr "${CMAKE_THREAD_LIBS_INIT}")
endif ()

#
# "make install" rules
#
install (TARGETS deltafs-srvr 
         RUNTIME DESTINATION bin)

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

