#!/bin/sh -x

#
# designed to run in the top src directory:
#   $ ./dev/rebuild_project.sh
#
# Aug-05-2016 zhengq@cs.cmu.edu
#

mkdir -p build && cd build || exit 1

cmake -DBUILD_SHARED_LIBS=ON -DBUILD_TESTS=ON \
      -DPDLFS_MERCURY_RPC=ON \
      -DPDLFS_RADOS=ON \
      -DPDLFS_SNAPPY=ON \
      -DPDLFS_GFLAGS=ON \
      -DPDLFS_GLOG=ON \
      ..

sleep 1

make -j4 VERBOSE=1

exit 0

