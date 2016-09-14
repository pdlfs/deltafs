#
# find gflags library and set up an imported target for it since
# gflags doesn't provide this for us...
#

# 
# inputs:
#   - GFLAGS_INCLUDE_DIR: hint for finding gflags/gflags.h
#   - GFLAGS_LIBRARY_DIR: hint for finding gflags lib
#
# output:
#   - "gflags" library target 
#   - GFLAGS_FOUND  (set if found)
#

include (FindPackageHandleStandardArgs)

find_path (GFLAGS_INCLUDE gflags/gflags.h HINTS ${GFLAGS_INCLUDE_DIR})
find_library (GFLAGS_LIBRARY gflags HINTS ${GFLAGS_LIBRARY_DIR})

find_package_handle_standard_args (GFlags DEFAULT_MSG 
    GFLAGS_INCLUDE GFLAGS_LIBRARY)

mark_as_advanced (GFLAGS_INCLUDE GFLAGS_LIBRARY)

if (GFLAGS_FOUND AND NOT TARGET gflags)
    add_library (gflags UNKNOWN IMPORTED)
    set_target_properties (gflags PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${GFLAGS_INCLUDE}")
    set_property (TARGET gflags APPEND PROPERTY
        IMPORTED_LOCATION "${GFLAGS_LIBRARY}")
endif ()

