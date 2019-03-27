**This document shows how to make mac ready for single-node deltafs development using gcc.**

[![Build Status](https://travis-ci.org/pdlfs/deltafs.svg?branch=master)](https://travis-ci.org/pdlfs/deltafs)
[![GitHub (pre-)release](https://img.shields.io/github/release-pre/pdlfs/deltafs.svg)](https://github.com/pdlfs/deltafs/releases)
[![License](https://img.shields.io/badge/license-New%20BSD-blue.svg)](LICENSE.txt)

# Step 1

The first step is to have xcode command line tools ready by invoking `xcode-select --install`. Then, install the [HomeBrew](https://brew.sh/) package manager. Next, use `brew` to install `git`, `gcc`, `cmake`, `automake`, and other C/C++ stuff. After that, use `brew` to install (with --build-from-source) `mpich`, `snappy`, `gflags`, and `glog`, exactly in this order.

```bash
brew install git gcc cmake automake autoconf libtool pkg-config
CC=/usr/local/bin/gcc-<n> CXX=/usr/local/bin/g++-<n> \
brew install -v --build-from-source \
  mpich snappy gflags glog
```

Once this is done, `libmpich.dylib`, `libsnappy.dylib`, `libgflags.dylib`, and `libglog.dylib` should be found at `/usr/local/lib`. Use `otool -L` to check whether each of them links back to the `gcc` we just installed. Lastly, use `brew cleanup` to reclaim space.

# Step 2

The next step is to install [mercury RPC](https://mercury-hpc.github.io/). To do this, select a latest mercury release from its [github site](https://github.com/mercury-hpc/mercury/releases). Then, use `cmake` to build it. The following code uses `mercury v1.0.1` and installs it to `/usr/local/mercury`.

```bash
wget https://github.com/mercury-hpc/mercury/releases/download/v1.0.1/mercury-1.0.1.tar.bz2
tar xzf mercury-1.0.1.tar.bz2 -C .
cd mercury-1.0.1
mkdir build
cd build
ccmake -DCMAKE_INSTALL_PREFIX=/usr/local/mercury \
-DBUILD_SHARED_LIBS=ON \
-DMERCURY_USE_CHECKSUMS=OFF \
..
```

Type 'c' a few times and check the following.

```bash
BUILD_DOCUMENTATION             *OFF
BUILD_EXAMPLES                  *OFF
BUILD_SHARED_LIBS               *ON  ## <--- make sure this is ON
BUILD_TESTING                   *OFF
CMAKE_ARCHIVE_OUTPUT_DIRECTORY  */proj/mercury-1.0.1/build/bin
CMAKE_BUILD_TYPE                *RelWithDebInfo
CMAKE_EXECUTABLE_FORMAT         *MACHO
CMAKE_INSTALL_PREFIX            */usr/local/mercury  ## <--- make sure this is something you desire
CMAKE_LIBRARY_OUTPUT_DIRECTORY  */proj/mercury-1.0.1/build/bin
CMAKE_RUNTIME_OUTPUT_DIRECTORY  */proj/mercury-1.0.1/build/bin
MCHECKSUM_ENABLE_VERBOSE_ERROR  *ON
MCHECKSUM_USE_ISAL              *OFF
MCHECKSUM_USE_SSE4_2            *ON
MCHECKSUM_USE_ZLIB              *OFF
MERCURY_ENABLE_COVERAGE         *OFF
MERCURY_ENABLE_STATS            *OFF
MERCURY_ENABLE_VERBOSE_ERROR    *ON
MERCURY_USE_BOOST_PP            *OFF
MERCURY_USE_CHECKSUMS           *OFF  ## <--- make sure this is OFF
MERCURY_USE_EAGER_BULK          *ON
MERCURY_USE_SELF_FORWARD        *OFF
MERCURY_USE_SM_ROUTING          *OFF
MERCURY_USE_SYSTEM_MCHECKSUM    *OFF
MERCURY_USE_XDR                 *OFF
NA_USE_BMI                      *OFF
NA_USE_CCI                      *OFF
NA_USE_MPI                      *OFF
NA_USE_OFI                      *OFF
NA_USE_SM                       *ON
```

If everything matches, type 'g' and do `make`.

```bash
make && make install
```

# Step 3

TODO
