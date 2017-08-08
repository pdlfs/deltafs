#
# xpkg-import.cmake  cmake import for "pkg-config" based packages
# 11-Aug-2016  chuck@ece.cmu.edu
#

#
# have PkgConfig use CMAKE_PREFIX_PATH for searching for pkg config files.
# this is the default for cmake 3.1 and above, on some older versions
# you can turn it on.   otherwise you'll have to manually set PKG_CONFIG_PATH
#
if (${CMAKE_MINIMUM_REQUIRED_VERSION} VERSION_LESS 3.1)
    set (PKG_CONFIG_USE_CMAKE_PREFIX_PATH 1)
endif ()

find_package(PkgConfig REQUIRED)

#
# PkgConfig in newer versions of cmake (>= cmake 3.6) supports imported
# targets, but older versions do not.  if we have 3.6 or better, use
# the builtin imported target unless ${OLD_XPKGIMPORT} is defined.
# otherwise use the Sam Thursfield code below...
#
if (${CMAKE_VERSION} VERSION_LESS 3.6)
    set (OLD_XPKGIMPORT 1)  # can also set on cmdline -DOLD_XPKGIMPORT=1
endif ()

if (NOT OLD_XPKGIMPORT)
    #
    # xpkg_import_module(name args)
    #
    # create an imported target named "name" from a pkg-config module.
    # all remaining args are passed through to pkg_check_module().
    #
    # example:
    #    xpkg_import_module(glog::glog REQUIRED libglog>=3.0)
    #
    message (STATUS "note: using built-in pkg_check_module imported targets")
    function (xpkg_import_module name)
        if (NOT TARGET ${name})

            # PkgConfig only adds PREFIX_PATH to PKG_CONFIG_PATH.
            # seems like it should add INSTALL_PREFIX too, so we do it here.
            # (this doesn't impact the parent variable scope.)
            if (CMAKE_INSTALL_PREFIX)
                list (APPEND CMAKE_PREFIX_PATH ${CMAKE_INSTALL_PREFIX})
            endif ()

            # generate a namespace to put results of pkg_check_modules in.
            # newer versions of PkgConfig don't like this to have ":" in it,
            # so filter them out...
            string (REPLACE ":" "_" cleanname ${name})
            set (prefix _${cleanname})

            pkg_check_modules("${prefix}" IMPORTED_TARGET ${ARGN})
            if (${prefix}_FOUND)
                add_library(${name} INTERFACE IMPORTED)
                set_property (TARGET ${name} PROPERTY
                              INTERFACE_LINK_LIBRARIES PkgConfig::${prefix})
            endif ()

        endif ()
    endfunction ()

else (NOT OLD_XPKGIMPORT)

    #
    # BEGIN:
    # pkg-config stuff adapted from https://cmake.org/Bug/view.php?id=15804
    # by Sam Thursfield <sam.thursfield@codethink.co.uk>
    #

    #.rst:
    # PkgImportModule
    # ---------------
    #
    # CMake commands to create imported targets from pkg-config modules.

    #=========================================================================
    # Copyright 2016 Lautsprecher Teufel GmbH
    #
    # Distributed under the OSI-approved BSD License (the "License");
    # see accompanying file Copyright.txt for details.
    #
    # This software is distributed WITHOUT ANY WARRANTY; without even the
    # implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
    # See the License for more information.
    #=========================================================================
    # (To distribute this file outside of CMake, substitute the full
    #  License text for the above reference.)

    # ::
    #
    #     xpkg_import_module(<name> args)
    #
    # Creates an imported target named <name> from a pkg-config module. All
    # remaining arguments are passed through to pkg_check_module(). See the
    # documentation of the FindPkgConfig module for information.
    #
    # Example usage:
    #
    #   xpkg_import_module(Archive::Archive REQUIRED libarchive>=3.0)
    #   xpkg_import_module(Avahi::GObject REQUIRED avahi-gobject)
    #
    message (STATUS "note: using old xpkg_import_module imported targets")
    function(xpkg_import_module name)
        if (NOT TARGET ${name})

            # PkgConfig only adds PREFIX_PATH to PKG_CONFIG_PATH.
            # seems like it should add INSTALL_PREFIX too, so we append
            # it here (doesn't impact parent scope).
            if (CMAKE_INSTALL_PREFIX)
                list (APPEND CMAKE_PREFIX_PATH ${CMAKE_INSTALL_PREFIX})
            endif ()

            # generate a namespace to put results of pkg_check_modules in.
            # newer versions of PkgConfig don't like this to have ":" in it,
            # so filter them out...
            string (REPLACE ":" "_" cleanname ${name})
            set (prefix _${cleanname})

            pkg_check_modules("${prefix}" ${ARGN})

            _xpkg_import_module_add_imported_target(
                ${name}
                "${${prefix}_CFLAGS_OTHER}"
                "${${prefix}_INCLUDE_DIRS}"
                "${${prefix}_LIBRARY_DIRS}"
                "${${prefix}_LIBRARIES}"
                "${${prefix}_STATIC_LIBRARIES}"
                )
        endif ()
    endfunction()

    #
    # create imported target using info from pkg_check_modules
    #
    function(_xpkg_import_module_add_imported_target name compile_options include_dirs library_dirs libraries staticlibraries)

        # We must pass in the absolute paths to the libraries, otherwise, when
        # the library is installed in a non-standard prefix the linker won't be
        # able to find the libraries. CMake doesn't provide a way for us to keep
        # track of the library search path for a specific target, so we have to
        # do it this way.
        _xpkg_import_module_find_libraries(libraries_full_paths
            ${name} "${libraries}" "${library_dirs}")

        list(GET libraries_full_paths 0 imported_location)

        if (${imported_location} MATCHES ".a$")
            # unix guess
            add_library(${name} STATIC IMPORTED)
            #
            # XXXCDC
            # static libs do not contain their depends, so we need to fetch
            # them again for additional libs that are in ${staticlibraries}.
            # we do this by calling _xpkg_import_module_find_libraries() again.
            # libs found in the previous call should already be in the cache
            # due to earlier calls to find_library() so this should not slow
            # us down that much... (this call will update
            # ${libraries_full_paths})
            #
            _xpkg_import_module_find_libraries(libraries_full_paths
                ${name} "${staticlibraries}" "${library_dirs}")
        else ()
            # FIXME: we should really detect whether it's SHARED or STATIC,
            # instead of assuming SHARED. We can't just use UNKNOWN because
            # nothing can link against it then.
            add_library(${name} SHARED IMPORTED)
        endif ()

        #
        # gen INTERFACE_LINK_LIBRARIES, it doesn't need the first entry on
        # the list since that is already in ${imported_location}
        #
        set (if_link_libs ${libraries_full_paths})
        list (LENGTH if_link_libs n_iflinks)
        if (n_iflinks GREATER 0)
            list (REMOVE_AT if_link_libs 0)   # remove item 0 from list
        endif ()

        set_target_properties(${name} PROPERTIES
            IMPORTED_LOCATION ${imported_location}
            INTERFACE_COMPILE_OPTIONS "${compile_options}"
            INTERFACE_INCLUDE_DIRECTORIES "${include_dirs}"
            INTERFACE_LINK_LIBRARIES "${if_link_libs}"
            )
    endfunction()

    #
    # convert list of lib names and dirs into list of library files using
    # find_library()
    #
    function(_xpkg_import_module_find_libraries output_var prefix library_names library_dirs)
        foreach(libname ${library_names})
            # find_library stores its result in the cache, so we must pass
            # a unique variable name for each library that we look for.
            set(library_path_var "${prefix}_LIBRARY_${libname}")
            find_library(${library_path_var} ${libname} PATHS ${library_dirs})
            mark_as_advanced (${library_path_var})
            list(APPEND library_paths "${${library_path_var}}")
        endforeach()
        set(${output_var} ${library_paths} PARENT_SCOPE)
    endfunction()


# ::
#
#    xadd_object_library_dependencies(<name> <dep1> [<dep2> ...])
#
# This is a workaround for <http://public.kitware.com/Bug/view.php?id=14778>.
# It should be possible to delete this function and use target_link_libraries()
# as normal once that is fixed.
#
    function(xadd_object_library_dependencies object_library)
        set(dependencies ${ARGN})
        foreach(dependency ${dependencies})
            if(NOT TARGET ${dependency})
                message(FATAL_ERROR
                    "xadd_object_library_dependencies(): ${dependency} is not "
                    "a target.")
            endif()

            target_compile_definitions(
                ${object_library} PUBLIC
                $<TARGET_PROPERTY:${dependency},INTERFACE_COMPILE_DEFINITIONS>)
            target_compile_options(
                ${object_library} PUBLIC
                $<TARGET_PROPERTY:${dependency},INTERFACE_COMPILE_OPTIONS>)
            target_include_directories(
                ${object_library} PUBLIC
                $<TARGET_PROPERTY:${dependency},INTERFACE_INCLUDE_DIRECTORIES>)
        endforeach()

        set_property(TARGET ${object_library}
                     APPEND PROPERTY LINK_LIBRARIES ${dependencies})
    endfunction()

    #
    # pkg-config stuff adapted from https://cmake.org/Bug/view.php?id=15804
    # by Sam Thursfield <sam.thursfield@codethink.co.uk>
    # END
    #
endif (NOT OLD_XPKGIMPORT)

#
# some of the packages we want to import come with either "pkg-config"
# .pc files or with cmake config files, depending on how they were
# installed.   for example, glog installed with "apt-get install" on
# "libgoogle-glog-dev" puts in a "libglog.pc" while installing glog
# from source via github installs a cmake config file for glog.
#
# this is complicated by the fact that the pkg-config .pc file names
# the package "libglog" while the cmake config file from github defines
# a library target "glog::glog" that you load with find_package(glog).
#
# we'd like to be able to use either mechanism to find glog, and we'd
# like the resulting target name to match between the two mechanisms
# (i.e. we just want to use "glog::glog" for both cases).
#
# to address this we add a wrapper function that trys both way.
# we first use "find_package()" in hopes of picking up a cmake config.
# if that fails, we fall back to "xpkg_import_module()"...

#
#     xdual_import(names)
#
# Where "names" is: target-name,find-package-name,xpkg-names
#
# names can be omitted (target-name will be used)
#
# Example usage:
#
#   xdual_import (foo REQUIRED)
#   xdual_import (glog::glog,glog,libglog REQUIRED)
#
#

function (xdual_import names)

    # parse spec
    set (resid ${names})
    foreach (var target cm_pkg)
        string (FIND "${resid}" "," slash)
        if (${slash} EQUAL -1)
            set (${var} ${resid})
            set (resid "")
        else ()
            math (EXPR slashplus ${slash}+1)
            string (SUBSTRING "${resid}" 0 ${slash} ${var})
            string (SUBSTRING "${resid}" ${slashplus} -1 resid)
        endif ()
        if ("${${var}}" STREQUAL "")
            set (${var} ${target})
        endif ()
    endforeach ()
    if ("${resid}" STREQUAL "")
        set (xpkg ${target})
    else ()
        set (xpkg ${resid})
    endif ()

    # try to find it both ways
    if (NOT TARGET ${target})
        find_package (${cm_pkg} QUIET)
        if (NOT TARGET ${target})
            string (REPLACE "," " " xpkg "${xpkg}")
            xpkg_import_module (${target} ${ARGN} ${xpkg})
        endif ()
    endif ()
endfunction ()
