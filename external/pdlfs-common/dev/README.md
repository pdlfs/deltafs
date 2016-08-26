Quick Installation Guide
------------------------

This guide assumes an Ubuntu 14.04 LTS OS image

### Basic

```
sudo apt-get install -y gcc g++ make pkg-config
sudo apt-get install -y autoconf automake libtool
sudo apt-get install -y cmake cmake-curses-gui
sudo apt-get install -y zlib1g-dev libssl-dev libbz2-dev libsnappy-dev
sudo apt-get install -y libgflags-dev libgflags2
sudo apt-get install -y libgoogle-glog-dev libgoogle-glog0
```

### MPI

This can be either MPICH (recommended),

```
sudo apt-get install -y libmpich2-dev mpich
```

or OpenMPI

```
sudo apt-get install -y libopenmpi-dev openmpi-bin
```

### RADOS

```
sudo apt-get install -y librados-dev ceph
```

### RPC

TODO

### Compile deltafs

```
sh ./dev/rebuild_project.sh
```
