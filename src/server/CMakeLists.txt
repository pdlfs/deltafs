#
# CMakeLists.txt  cmake file for server directory
# 16-Jun-2016  chuck@ece.cmu.edu
#

add_executable (deltafs-srvr deltafs_server.cc)
target_link_libraries (deltafs-srvr deltafs)

#
# "make install" rules
#
install (TARGETS deltafs-srvr 
         RUNTIME DESTINATION bin)
