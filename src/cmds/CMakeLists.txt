#
# CMakeLists.txt  cmake file for cmds directory
# 02-Sep-2016  chuck@ece.cmu.edu
#

add_executable (deltafs_shell deltafs_shell.cc)
target_link_libraries (deltafs_shell deltafs)

add_executable (deltafs_mkdir deltafs_mkdir.cc)
target_link_libraries (deltafs_mkdir deltafs)

add_executable (deltafs_ls deltafs_ls.cc)
target_link_libraries (deltafs_ls deltafs)

#
# "make install" rules
#
install (TARGETS deltafs_shell deltafs_mkdir deltafs_ls
         RUNTIME DESTINATION bin)
