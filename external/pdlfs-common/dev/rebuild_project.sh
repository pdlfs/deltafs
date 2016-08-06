#!/bin/sh -x

#
# designed to run in the top src directory:
#   $ ./dev/rebuild_project.sh
#
# Aug-05-2016 zhengq@cs.cmu.edu
#

MAKE="make -f ./dev/Makefile"

$MAKE clean
$MAKE -j4
$MAKE -j4 tests

exit 0
