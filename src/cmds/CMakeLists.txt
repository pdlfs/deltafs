#
# CMakeLists.txt  cmake file for cmds directory
# 02-Sep-2016  chuck@ece.cmu.edu
#

add_executable (deltafs_shell deltafs_shell.cc)
target_link_libraries (deltafs_shell deltafs)

#
# "make install" rules
#
install (TARGETS deltafs_shell 
         RUNTIME DESTINATION bin)
