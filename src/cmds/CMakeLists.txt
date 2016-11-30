#
# CMakeLists.txt  cmake file for cmds directory
# 02-Sep-2016  chuck@ece.cmu.edu
#

add_executable (deltafs_shell deltafs_shell.cc)
target_link_libraries (deltafs_shell deltafs)

add_executable (deltafs_mkdir deltafs_mkdir.cc)
target_link_libraries (deltafs_mkdir deltafs)

add_executable (deltafs_mkdirplus deltafs_mkdirplus.cc)
target_link_libraries (deltafs_mkdirplus deltafs)

add_executable (deltafs_ls deltafs_ls.cc)
target_link_libraries (deltafs_ls deltafs)

add_executable (deltafs_touch deltafs_touch.cc)
target_link_libraries (deltafs_touch deltafs)

add_executable (deltafs_chown deltafs_chown.cc)
target_link_libraries (deltafs_chown deltafs)

add_executable (deltafs_unlink deltafs_unlink.cc)
target_link_libraries (deltafs_unlink deltafs)

add_executable (deltafs_stat deltafs_stat.cc)
target_link_libraries (deltafs_stat deltafs)

add_executable (deltafs_accessdir deltafs_accessdir.cc)
target_link_libraries (deltafs_accessdir deltafs)

add_executable (deltafs_access deltafs_access.cc)
target_link_libraries (deltafs_access deltafs)

#
# "make install" rules
#
install (TARGETS deltafs_shell deltafs_mkdir deltafs_mkdirplus
                 deltafs_ls deltafs_touch deltafs_unlink deltafs_stat
                 deltafs_accessdir deltafs_access
                 deltafs_chown
         RUNTIME DESTINATION bin)
