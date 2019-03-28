**This document shows how to perform local deltafs tests against ceph rados.**

[![Build Status](https://travis-ci.org/pdlfs/deltafs.svg?branch=master)](https://travis-ci.org/pdlfs/deltafs)
[![GitHub (pre-)release](https://img.shields.io/github/release-pre/pdlfs/deltafs.svg)](https://github.com/pdlfs/deltafs/releases)
[![License](https://img.shields.io/badge/license-New%20BSD-blue.svg)](LICENSE.txt)

DeltaFS
=======

```
XXXXXXXXX
XX      XX                 XX                  XXXXXXXXXXX
XX       XX                XX                  XX
XX        XX               XX                  XX
XX         XX              XX   XX             XX
XX          XX             XX   XX             XXXXXXXXX
XX           XX  XXXXXXX   XX XXXXXXXXXXXXXXX  XX         XX
XX          XX  XX     XX  XX   XX       XX XX XX      XX
XX         XX  XX       XX XX   XX      XX  XX XX    XX
XX        XX   XXXXXXXXXX  XX   XX     XX   XX XX    XXXXXXXX
XX       XX    XX          XX   XX    XX    XX XX           XX
XX      XX      XX      XX XX   XX X    XX  XX XX         XX
XXXXXXXXX        XXXXXXX   XX    XX        XX  XX      XX
```

## Overview

  * All file system metadata will be stored as RADOS objects in a pre-created pool named `metadata`.
  * All file data will be stored as RADOS objects (one object per file) in a pre-created pool named `data`.
  * Ceph configuration file can be found at `/tmp/ceph.conf`.
  
## Deploy Ceph Rados on a local Linux machine

On Ubuntu 14.04 LTS, Ceph RADOS may be deployed and initialized using a helper script ([dev/ceph_rados.sh](external/pdlfs-common/dev/ceph_rados.sh)) shipped along with the DeltaFS source code. Currently, the script is only tested on Ubuntu 14.04 LTS and with the version of Ceph (`v0.80`) provided by Ubuntu's apt-get (`sudo apt-get install ceph`).

TODO

## Run DeltaFS

The following instructions will run two DeltaFS server instances and then use a DeltaFS shell to access the DeltaFS file system's namespace.

```bash
mpirun -n 2 env DELTAFS_EnvName=rados DELTAFS_FioName=rados \
    ./build/src/server/deltafs-srvr -v=1 -logtostderr
```

The DeltaFS file system's metadata information will be stored as objects in a pool named `metadata` in the ceph rados. All objects will be named as `_tmp_deltafs_outputs_xxx`. Unlike file system metadata, file data will be stored as objects in a pool named `data` in the ceph rados. The two DeltaFS server instances will begin listening on tcp port 10101 and 10102.

```bash
env "DELTAFS_MetadataSrvAddrs=127.0.0.1:10101&127.0.0.1:10102" "DELTAFS_NumOfMetadataSrvs=2" \
    ./build/src/cmds/deltafs-shell -v=1 -logtostderr
```

This will start a DeltaFS shell and instruct it to connect to DeltaFS servers we previously started. Currently, this is just a simple shell that allows us to create directories, copy files from the local file system to DeltaFS, and cat files in DeltaFS.
