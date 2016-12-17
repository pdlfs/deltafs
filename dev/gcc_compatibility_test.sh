#!/bin/bash -x

topdir=$(cd `dirname $0`/.. && pwd -P)

TEST_compile() {
  export CC="gcc-$1"; export CXX="g++-$1"
  builddir="$topdir/build_cxx_$1"
  rm -rf $builddir
  mkdir $builddir
  cd $builddir
  cmake .. && make
}

for ver in 4.4 4.6 4.8 5.4 6.1
do
  which "gcc-$ver"
  if [ $? -eq 0 ]; then 
    TEST_compile $ver
  fi
done

exit 0

