**Transient user-space file system optimized for parallel scientific applications.**

[![Build Status](https://travis-ci.org/pdlfs/deltafs.svg?branch=master)](https://travis-ci.org/pdlfs/deltafs)
[![License](https://img.shields.io/badge/license-New%20BSD-blue.svg)

# Deltafs

Please see the accompanying COPYING file for license details.

# Features
  * Serverless design featuring zero dedicated metadata servers, no global file system namespace, and no ground truth.
  * Client-funded metadata service harnessing compute nodes to handle metadata and achieve highly agile scalability.
  * Freedom from unjustified synchronization among HPC applications that do not need to use the file system to communicate.
  * Write-optimized LSM-based metadata representation with file system namespace snapshots as the basis of inter-job data sharing and workflow execution.
  * A file system as no more than a thin service composed by each application at runtime to provide a temporary view of a private namespace backed by a stack of immutable snapshots and a collection of shared data objects.
  * Simplified data center storage consisting of multiple independent underlying object stores, providing flat namespaces of data objects, and oblivious of file system semantics.

# Platform

Deltafs supports Linux, Mac, and most UNIX platforms for development and local testing. To run Deltafs, we currently assume it is always a Linux box. Deltafs is mainly written by C++. C++11 is not required to compile Deltafs code, but will be used when the compiler supports it.

# Documentation

We have a short paper [deltafs_pdsw15]( http://www.cs.cmu.edu/~qingzhen/files/deltafs_pdsw15.pdf) that provides an overview of Deltafs.

# Software requirements

Compiling and running Deltafs requires recent versions of C++ compiler, cmake, mpi, snappy, glog, and gflags. Compiling Deltafs requirements usually require recent versions of autoconf, automake, and libtool.

On Ubuntu 16.04+ LTS, please use the following to prepare the envrionment for Deltafs.

```
sudo apt-get install -y gcc g++ make pkg-config
sudo apt-get install -y libsnappy-dev libgflags-dev libgoogle-glog-dev
sudo apt-get install -y autoconf automake libtool
sudo apt-get install -y cmake cmake-curses-gui
sudo apt-get install -y libmpich-dev mpich
```

## Object store

Deltafs assumes an underlying object storage service to store file system metadata and file data. This underlying object store may just be a shared parallel file system such as Lustre, GPFS, PanFS, and HDFS. However, a scalable object storage service is suggested to ensure high performance and currently Deltafs supports Rados.

### Rados

On Ubuntu 16.04+ LTS, rados may be installed directly through apt-get.

```
sudo apt-get install -y librados-dev ceph
```

## RPC

Distributed deltafs instances requires an RPC library to communicate with each other. Currently, we use [Mercury](https://mercury-hpc.github.io/) and Mercury itself supports multiple network backends, such as MPI, bmi on tcp, and cci on a variety of underlying network abstractions including verbs, tcp, sock, and raw eth.

### Mercury

Please follow online Merury [documentation](https://github.com/mercury-hpc/mercury) to install Mercury and one or more of its backends. To start, we suggest using bmi as the network backend. Both the Mercury and bmi codebase are linked by Deltafs as git submodules. Compiling Mercury may also require the installation of openpa.

```
git submodule update --init deps/bmi
cd deps/bmi
./prepare && ./configure --enable-shared --enable-bmi-only
make && make install
```
```
git submodule update --init deps/mercury
git submodule update --init deps/openpa
```

# Building

After all software dependencies are installed, we can proceed to build Deltafs.
Deltafs uses cmake and suggests you to do an out-of-source build. To do that, create a dedicated build directory and run 'ccmake' command from it:

```
cd deltafs
mkdir build
cd build
ccmake ..
```

Type 'c' multiple times and choose suitable options. Recommended options are:

```
 BUILD_SHARED_LIBS                ON
 BUILD_TESTS                      ON
 CMAKE_BUILD_TYPE                 RelWithDebInfo
 CMAKE_INSTALL_PREFIX             /usr/local
 CMAKE_PREFIX_PATH
 DEBUG_SANITIZER                  Off
 DELTAFS_MPI                      ON
 PDLFS_GFLAGS                     ON
 PDLFS_GLOG                       ON
 PDLFS_MARGO_RPC                  OFF
 PDLFS_MERCURY_RPC                ON
 PDLFS_RADOS                      ON
 PDLFS_SNAPPY                     ON
```

Once you exit the CMake configuration screen and are ready to build the targets, do:

```
make
```

## Local testing

To test Deltafs on a local machine using the local file system to store file system metadata and file data, we can run two Deltafs server instances and then use a Deltafs shell to access the namespace.

```
mpirun -n 2 ./build/src/server/deltafs-srvr -logtostderr
```

This will start two Deltafs server instances that store file system metadata in /tmp/deltafs_outputs and file data in /tmp/deltafs_data. Please remove these two folders if they exist before running Deltafs. The two Deltafs server instances will begin listening on tcp port 10101 and 10102.

```
env DELTAFS_MetadataSrvAddrs="127.0.0.1:10101&127.0.0.1:10102" DELTAFS_NumOfMetadataSrvs="2" \
    ./build/src/cmds/deltafs_shell -logtostderr
```

This will start a Deltafs shell and instruct it to connect to Deltafs servers we previously started. Currently, this is just a simple shell that allows us to create directories, copy files from the local file system to Deltafs, and cat files in Deltafs.

# Deltafs app

Currently, applications have to explicitly link to Deltafs user libbrary (include/deltafs_api.h) in order to call Deltafs. Alternatively, Deltafs may be implicitly invoked by preloading fs calls made by an application and redirecting them to Deltafs. We have developped one such library and it is available here, https://github.com/pdlfs/pdlfs-preload. 
