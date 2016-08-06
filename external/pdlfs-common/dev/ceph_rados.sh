#!/bin/bash
#
# Utility script for operating ceph rados.
#
# Aug-05-2016 zhengq@cs.cmu.edu
#
SUDO="sudo"

if test $# -lt 1; then
  echo "* Usage: $0 [start|stop|status|kill|check|watch|ls|stat]

=====================================================================
  start  | create a new rados cluster                  |   sudo
  stop   | shutdown the running cluster                |   sudo
  status | get the status of the running cluster       |   sudo
  kill   | terminate the running cluster               |   sudo
---------------------------------------------------------------------
  check  | print the version of ceph rados and exit    |
---------------------------------------------------------------------
  watch  | monitor the status of a running cluster     |
  ls     | list ALL objects in the cluster             |
  stat   | check the existence of a particular object  |
=====================================================================

* On Ubuntu, ceph rados could be installed by:

    sudo apt-get install ceph librados-dev

* Please use `basename $0` only for local testing and debugging 
* `basename $0` only works with ceph 0.80.x"
  exit 1
fi

set -x

me=$0
SCRIPT_DIR=$(cd -P -- `dirname $me` && pwd -P)
RADOS_RUN=/tmp/pdlfs-rados
TEMPLATE_CEPH_CONF=$SCRIPT_DIR/ceph.conf
CEPH_CONF=$RADOS_RUN/ceph.conf

prepare_rados() {
  $SUDO rm -rf $RADOS_RUN/*
  $SUDO mkdir -p $RADOS_RUN $RADOS_RUN/mon
  $SUDO mkdir -p $RADOS_RUN/osd/ceph-0 $RADOS_RUN/osd/ceph-1 $RADOS_RUN/osd/ceph-2
  $SUDO rm -f $CEPH_CONF
  $SUDO sed "s/@localhost@/`hostname -s`/g; \
             s/@rados_run@/${RADOS_RUN//\//\\\/}/g" \
      $TEMPLATE_CEPH_CONF | $SUDO tee $CEPH_CONF
  $SUDO rm -f /tmp/ceph.conf
  $SUDO ln -fs $CEPH_CONF /tmp/ceph.conf
}

fix_pools() {
  for pool in 'metadata'; do
    ceph -c $CEPH_CONF osd pool set $pool min_size 1
    ceph -c $CEPH_CONF osd pool set $pool size 1
  done
  for pool in 'data' 'rbd'; do
    ceph -c $CEPH_CONF osd pool delete $pool $pool --yes-i-really-really-mean-it
  done
}

kill_rados() {
  $SUDO killall -9 ceph-mon
  $SUDO killall -9 ceph-osd
}

force_kill_rados() {
  $SUDO killall -9 ceph-mon &>/dev/null
  $SUDO killall -9 ceph-osd &>/dev/null
}

COMMAND=$1
case $COMMAND in
  ls)
    rados -c $CEPH_CONF -p metadata ls
    ;;
  stat)
    rados -c $CEPH_CONF -p metadata stat $2
    ;;
  check)
    ceph --version
    ;;
  watch)
    ceph -c $CEPH_CONF -w
    ;;
  status)
    ceph -c $CEPH_CONF -s
    ;;
  start)
    force_kill_rados
    prepare_rados
    $SUDO mkcephfs -a -c $CEPH_CONF --no-copy-conf || exit 1
    $SUDO ceph-mon -i a -c $CEPH_CONF || exit 1
    fix_pools
    $SUDO ceph-osd -i 0 -c $CEPH_CONF || exit 1
    $SUDO ceph-osd -i 1 -c $CEPH_CONF || exit 1
    $SUDO ceph-osd -i 2 -c $CEPH_CONF || exit 1
    ;;
  stop|kill)
    kill_rados
    ;;
  *)
    echo "Unrecognized command '$COMMAND' -- oops"
    exit 1
    ;;
esac

exit 0
