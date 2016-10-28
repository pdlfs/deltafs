#
# CMakeLists.txt  cmake file for top benchmarks directory
# 28-Oct-2016  chuck@ece.cmu.edu
#

add_executable (large_dir large_dir.cc)
target_link_libraries (large_dir deltafs)

#
# "make install" rules
#
install (TARGETS large_dir 
         RUNTIME DESTINATION bin)
