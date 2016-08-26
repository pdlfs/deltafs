#
# find RADOS library and set up an imported target for it since
# RADOS don't provide this for us...
#

# 
# inputs:
#   - RADOS_INCLUDE_DIR: hint for finding rado/librados.h
#   - RADOS_LIBRARY_DIR: hint for finding rados lib
#
# output:
#   - "rados" library target 
#   - RADOS_FOUND  (set if found)
#

include (FindPackageHandleStandardArgs)

find_path (RADOS_INCLUDE rados/librados.h HINTS ${RADOS_INCLUDE_DIR})
find_library (RADOS_LIBRARY rados HINTS ${RADOS_LIBRARY})

find_package_handle_standard_args (RADOS DEFAULT_MSG 
    RADOS_INCLUDE RADOS_LIBRARY)

mark_as_advanced (RADOS_INCLUDE RADOS_LIBRARY)

if (RADOS_FOUND AND NOT TARGET rados)
    add_library (rados UNKNOWN IMPORTED)
    set_target_properties (rados PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${RADOS_INCLUDE}")
    set_property (TARGET rados APPEND PROPERTY
        IMPORTED_LOCATION "${RADOS_LIBRARY}")
endif ()

