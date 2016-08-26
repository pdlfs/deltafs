**Transient user-space file system optimized for parallel scientific applications.**

[![Build Status](https://travis-ci.org/pdlfs/deltafs.svg?branch=master)](https://travis-ci.org/pdlfs/deltafs)

# Deltafs

Please see the accompanying COPYING file for license details.

# Features
  * Serverless design featuring zero dedicated metadata servers, no global file system namespace, and no ground truth.
  * Client-funded metadata service harnessing compute nodes to handle metadata and achieve highly agile scalability.
  * Freedom from unjustified synchronization among HPC applications that do not need to use the file system to communicate.
  * Write-optimized LSM-based metadata representation with file system namespace snapshots as the basis of interjob data sharing and workflow execution.
  * A file system as no more than a thin service composed by each application at runtime to provide a temporary view of a private namespace backed by a stack of immutable snapshots and a collection of shared data objects.
  * Simplified data center storage consisting of multiple independent underlying object stores, providing flat namespaces of data objects, and oblivious of file system semantics.

# Platform

Deltafs supports Linux, Mac, and most UNIX platforms for development and local testing. To run Deltafs, we currently assume it is always Linux. Deltafs is mainly written by C++. C++11 is not required to compile and run Deltafs, but is used when the compiler supports it.

# Documentation

Not ready now but here is a short paper [deltafs_pdsw15]( http://www.cs.cmu.edu/~qingzhen/files/deltafs_pdsw15.pdf) that provides an overview of Deltafs.

# Software requirements

Compiling and running Deltafs requires recent versions of a C++ compiler, cmake, mpi, snappy, glog, and gflags. Compiling Deltafs requirements usually requires recent versions of autoconf, automake, and libtool.

On Ubuntu (14.04+), use the following to prepare the envrionment for Deltafs.

```
sudo apt-get install -y gcc g++ make pkg-config
sudo apt-get install -y libsnappy-dev libgflags-dev libgoogle-glog-dev
sudo apt-get install -y autoconf automake libtool
sudo apt-get install -y cmake cmake-curses-gui
sudo apt-get install -y libmpich2-dev mpich
```

Deltafs assumes an underlying object storage service to store file system metadata and file data. This underlying object store may just be a shared parallel file system such as Lustre, GPFS, PanFS, and HDFS. However, a scalable object storage service is suggested to ensure optimal performance and currently Deltafs supports Rados.

## Rados

On Ubuntu (14.04+), rados can be installed directly through apt-get.

```
sudo apt-get install -y librados-dev ceph
```

Deltafs requires an RPC library to communicate among distributed Deltafs client and server instances. Currently, we use [Mercury](https://mercury-hpc.github.io/) and Mercury itself supports multiple network backends, such as MPI, bmi on tcp, and cci on a variety of underlying network abstractions including verbs, tcp, sock, and raw eth.

Please follow online Merury [documentation](https://github.com/mercury-hpc/mercury) to install Mercury and one of its backends. To start, we suggest bmi as the network backend.

# Building

After all software dependencies are installed, please use the following to build Deltafs.

```
make -f ./dev/Makefile
```

