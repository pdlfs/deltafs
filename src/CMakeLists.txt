#
# CMakeLists.txt  top-level cmake file for deltafs/src
# 22-Jun-2016  chuck@ece.cmu.edu
#
include (CheckCXXCompilerFlag)

#
# set some general deltafs compile options (this is a directory property)
#
set (deltafs-try-common-flags -Wpedantic -Wno-long-long -Wall
                              -Wno-sign-compare -pthread)
foreach (lcv ${deltafs-try-common-flags})
    CHECK_CXX_COMPILER_FLAG (${lcv} flag${lcv})
    if (${flag${lcv}})
        add_compile_options (${lcv})
    endif ()
endforeach ()

add_subdirectory (libdeltafs)
add_subdirectory (cmds)
add_subdirectory (server)

