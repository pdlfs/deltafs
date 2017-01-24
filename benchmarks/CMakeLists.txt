#
# CMakeLists.txt  cmake file for top benchmarks directory
# 30-Oct-2016  chuck@ece.cmu.edu
#

#
# first build a small static helper library "io_client" with common code in it.
# this library will have a dependency on MPI (causes all io_client users to
# also get MPI).
#
add_library (io_client STATIC io_client.cc io_deltafs.cc io_posix.cc)
target_link_libraries (io_client deltafs)

# plug in MPI
# mpich on ub14 gives a leading space that we need to trim off
string (REPLACE " " ";" mpicxx_flags "${MPI_CXX_COMPILE_FLAGS}")
foreach (lcv ${mpicxx_flags})
    if (NOT ${lcv} STREQUAL "")
        target_compile_options (io_client PUBLIC $<BUILD_INTERFACE:${lcv}>)
    endif ()
endforeach ()

# XXX: have to do this one dir at a time otherwise
foreach (lcv ${MPI_CXX_INCLUDE_PATH})
    target_include_directories (io_client PUBLIC $<BUILD_INTERFACE:${lcv}>)
endforeach ()
foreach (lcv ${MPI_CXX_LIBRARIES})
    target_link_libraries(io_client $<BUILD_INTERFACE:${lcv}>)
endforeach ()
set_property (TARGET io_client APPEND PROPERTY LINK_FLAGS ${MPI_CXX_LINK_FLAGS})
get_property (link_flags TARGET io_client PROPERTY LINK_FLAGS)

# plug in threads
set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package (Threads REQUIRED)
if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries (io_client "${CMAKE_THREAD_LIBS_INIT}")
endif ()

#
# now build and install the benchmarks binaries
#

# large_dir
add_executable (large_dir large_dir/large_dir.cc)
target_link_libraries (large_dir io_client)

# vpic_io
add_executable (vpic_io vpic_io/vpic_io.cc)
target_link_libraries (vpic_io io_client)

install (TARGETS large_dir vpic_io
         RUNTIME DESTINATION bin)

