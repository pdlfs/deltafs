#
# CMakeLists.txt  cmake file for the pdlfs-common examples
# 09-Nov-2016  chuck@ece.cmu.edu
#


# XXX: example example, just add_executable with a target link to pdlfs-common
# add_executable (foo foo.c)
# target_link_libraries (foo pdlfs-common)

add_executable (leveldb_example leveldb_example.cc)
target_link_libraries (leveldb_example pdlfs-common)

add_executable (leveldb_bulk_example leveldb_bulk_example.cc)
target_link_libraries (leveldb_bulk_example pdlfs-common)
