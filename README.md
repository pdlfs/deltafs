**Transient file system service featuring highly paralleled indexing on both file data and file system metadata.**

[![Build Status](https://travis-ci.org/pdlfs/deltafs.svg?branch=master)](https://travis-ci.org/pdlfs/deltafs)
[![License](https://img.shields.io/badge/license-New%20BSD-blue.svg)](COPYING)

# DeltaFS

Please see the accompanying LICENSE.txt for license details. DeltaFS is still under development.

# Features
  * Serverless design featuring zero dedicated metadata servers and no global file system namespace.
  * Application-owned metadata service harnessing compute nodes to handle metadata and achieve highly agile scalability.
  * Freedom from unjustified synchronization among HPC applications that do not need to use the file system to communicate.
  * Write-optimized LSM-based metadata representation with file system namespace snapshots as the basis of inter-job data sharing and workflow execution.
  * A special directory type with an embedded striped-down streaming Map-Reduce pipeline.
  * A file system as no more than a thin service composed by each application at runtime to provide a temporary view of a private namespace backed by a stack of immutable snapshots and a collection of shared data objects.
  * Simplified data center storage consisting of multiple independent underlying object stores, providing flat namespaces of data objects, and oblivious of file system semantics.

# Platform

DeltaFS is able to run on Linux, Mac OS, as well as most UNIX platforms for both development and local testing purposes. To run DeltaFS in production, it must be a Linux. DeltaFS is mainly written by C++. C++11 is not required to compile DeltaFS code, but will be used if the compiler supports it. C++14 is not used.

# Documentation

Our paper [deltafs_pdsw15](http://www.cs.cmu.edu/~qingzhen/files/deltafs_pdsw15.pdf) provides an overview of DeltaFS, and our another paper [deltafs_pdsw17](http://www.cs.cmu.edu/~qingzhen/files/deltafs_pdsw17.pdf) provides an overview of DeltaFS Indexed Massive Directory.

# Software requirements

Compiling DeltaFS requires a recent C++ compiler, cmake, make, mpi, snappy, glog, and gflags. Compiling some of DeltaFS's dependencies requires a recent autoconf, automake, and libtool.

On Ubuntu 16 and 17 distributions, you may use the following to prepare the system environment for DeltaFS.

```
sudo apt-get update

sudo apt-get install -y gcc g++ make  # Alternatively, this can also be clang
sudo apt-get install -y autoconf automake libtool pkg-config
sudo apt-get install -y cmake cmake-curses-gui
sudo apt-get install -y libsnappy-dev libgflags-dev libgoogle-glog-dev
sudo apt-get install -y libmpich-dev  # Alternatively, this can also be libopenmpi-dev
sudo apt-get install -y mpich
```

## Object store

DeltaFS assumes an underlying object storage service to store file system metadata and file data. This underlying object store may just be a shared parallel file system such as Lustre, GPFS, PanFS, and HDFS. However, a scalable object storage service is suggested to ensure high performance and currently DeltaFS supports Ceph RADOS.

### RADOS

On Ubuntu 16 and 17 distributions, RADOS can be installed through apt-get.

```
sudo apt-get install -y librados-dev
sudo apt-get install -y ceph
```

## RPC

Distributed DeltaFS instances require an RPC library to communicate with each other. Currently, we use [Mercury](https://mercury-hpc.github.io/) and Mercury itself supports multiple network backends, such as MPI, bmi on tcp, and cci on a variety of underlying network abstractions including verbs, tcp, sock, and raw eth.

### Mercury

Please follow online Merury [documentation](https://github.com/mercury-hpc/mercury) to install Mercury and one or more of its backends. To start, we suggest using bmi as the network backend. Compiling Mercury may also require the installation of openpa, depending on the presence of `<stdatomic.h>`.

```
# BMI
git clone http://git.mcs.anl.gov/bmi.git && cd bmi
./prepare && ./configure --enable-shared --enable-bmi-only
make && sudo make install

# Mercury
git clone https://github.com/mercury-hpc/mercury.git && cd mercury
mkdir build && cd build
cmake ..
make && sudo make install
```

# Building

After all software dependencies are installed, we can proceed to build DeltaFS.
DeltaFS uses cmake and suggests you to do an out-of-source build. To do that, create a dedicated build directory and run 'ccmake' command from it:

```
cd deltafs
mkdir build
cd build
ccmake -DDELTAFS_COMMON_INTREE=ON ..
```

Type 'c' multiple times and choose suitable options. Recommended options are:

```
 BUILD_SHARED_LIBS                ON
 BUILD_TESTS                      ON
 CMAKE_BUILD_TYPE                 RelWithDebInfo
 CMAKE_INSTALL_PREFIX             /usr/local
 CMAKE_PREFIX_PATH
 DEBUG_SANITIZER                  Off
 DELTAFS_BBOS                     OFF
 DELTAFS_BENCHMARKS               OFF
 DELTAFS_COMMON_INTREE            ON
 DELTAFS_MPI                      ON
 PDLFS_GFLAGS                     ON
 PDLFS_GLOG                       ON
 PDLFS_HOST_OS                    Linux
 PDLFS_HOST_OS_VERSION            4.9.17-c9
 PDLFS_MARGO_RPC                  OFF
 PDLFS_MERCURY_RPC                ON
 PDLFS_PLATFORM                   POSIX
 PDLFS_RADOS                      OFF
 PDLFS_SNAPPY                     ON
 PDLFS_TARGET_OS                  Linux
 PDLFS_TARGET_OS_VERSION          4.9.17-c9
 PDLFS_VERBOSE                    0
 deltafs-common_DIR               deltafs-common_DIR-NOTFOUND
```

Once you exit the CMake configuration screen and are ready to build the targets, do:

```
make
```

## Local testing

To test DeltaFS on a local machine using the local file system to store file system metadata and file data, we can run two DeltaFS server instances and then use a DeltaFS shell to access the namespace.

```
mpirun -n 2 ./build/src/server/deltafs-srvr -v=1 -logtostderr
```

This will start two DeltaFS server instances that store file system metadata in /tmp/deltafs_outputs and file data in /tmp/deltafs_data. Please remove these two folders if they exist before running DeltaFS. The two DeltaFS server instances will begin listening on tcp port 10101 and 10102.

```
env DELTAFS_MetadataSrvAddrs="127.0.0.1:10101,127.0.0.1:10102" DELTAFS_NumOfMetadataSrvs="2" \
    ./build/src/cmds/deltafs-shell -v=1 -logtostderr
```

This will start a DeltaFS shell and instruct it to connect to DeltaFS servers we previously started. Currently, this is just a simple shell that allows us to create directories, copy files from the local file system to DeltaFS, and cat files in DeltaFS.

# DeltaFS app

Currently, applications have to explicitly link to DeltaFS user library (include/deltafs_api.h) in order to call DeltaFS. Alternatively, DeltaFS may be implicitly invoked by preloading fs calls made by an application and redirecting them to DeltaFS. We have developed one such library and it is available here, https://github.com/pdlfs/pdlfs-preload.
