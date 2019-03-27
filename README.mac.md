**This document shows how to make mac ready for deltafs development**

# Step 0

The first step is to have xcode command line tools ready by invoking `xcode-select --install`. Then, install the [HomeBrew](https://brew.sh/) package manager. Next, use `brew` to install `gcc`, `cmake`, `automake`, and other C/C++ stuff. After that, use `brew` to install (with --build-from-source) `mpich`, `snappy`, `gflags`, and `glog`, exactly in this order.

```bash
brew install gcc cmake automake autoconf libtool pkg-config
brew install --verbose --build-from-source \
    mpich snappy gflags glog
```
