**This document shows how to make mac ready for deltafs development using gcc.**

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

Once this is done, `libmpich.dylib`, `libsnappy.dylib`, `libgflags.dylib`, and `libglog.dylib` should be found at `/usr/local/lib`. Use `otool -L` to check whether each of them links back to the `gcc` we just installed.

# Step 2

TODO
