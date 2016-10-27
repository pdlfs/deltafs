**Internal code shared by several top-level projects inside PDL including deltafs, indexfs, and tablefs.**

[![Build Status](https://travis-ci.org/pdlfs/pdlfs-common.svg?branch=master)](https://travis-ci.org/pdlfs/pdlfs-common)

# Building

We use cmake and suggest you to do an out-of-source build. To install cmake and other build tools on Ubuntu 16.04+ LTS:

```
sudo apt-get update
sudo apt-get install gcc g++ make pkg-config
sudo apt-get install cmake cmake-curses-gui
```

Then, create a dedicated build directory and run 'ccmake' command from it:

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
 PDLFS_GFLAGS                     OFF
 PDLFS_GLOG                       OFF
 PDLFS_MARGO_RPC                  OFF
 PDLFS_MERCURY_RPC                OFF
 PDLFS_RADOS                      OFF
 PDLFS_SNAPPY                     OFF
```

Once you exit the cmake configuration screen and are ready to build the targets, do:

```
make
```
