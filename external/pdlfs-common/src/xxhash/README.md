**This file is copied and modified from xxHash's original [readme](https://raw.githubusercontent.com/Cyan4973/xxHash/dev/README.md) file.**

xxHash - Extremely fast hash algorithm
======================================

This is xxHash v0.6.5.

xxHash is an Extremely fast Hash algorithm developed by Yann Collet, capable of running at RAM speed limits.
It successfully completes the [SMHasher](http://code.google.com/p/smhasher/wiki/SMHasher) test suite
which evaluates collision, dispersion and randomness qualities of hash functions.
Code is highly portable, and hashes are identical on all platforms (little / big endian).

### License

The library files `xxhash.c` and `xxhash.h` are BSD licensed under Yann Collet.

Benchmarks
-------------------------

The benchmark uses SMHasher speed test, compiled with Visual 2010 on a Windows Seven 32-bit box.
The reference system uses a Core 2 Duo @3GHz


| Name          |   Speed     | Quality | Author            |
|---------------|-------------|:-------:|-------------------|
| [xxHash]      | 5.4 GB/s    |   10    | Y.C.              |
| MurmurHash 3a | 2.7 GB/s    |   10    | Austin Appleby    |
| SBox          | 1.4 GB/s    |    9    | Bret Mulvey       |
| Lookup3       | 1.2 GB/s    |    9    | Bob Jenkins       |
| CityHash64    | 1.05 GB/s   |   10    | Pike & Alakuijala |
| FNV           | 0.55 GB/s   |    5    | Fowler, Noll, Vo  |
| CRC32         | 0.43 GB/s † |    9    |                   |
| MD5-32        | 0.33 GB/s   |   10    | Ronald L.Rivest   |
| SHA1-32       | 0.28 GB/s   |   10    |                   |

[xxHash]: http://www.xxhash.com

Note †: SMHasher's CRC32 implementation is known to be slow. Faster implementations exist.

Q.Score is a measure of quality of the hash function.
It depends on successfully passing SMHasher test set.
10 is a perfect score.
Algorithms with a score < 5 are not listed on this table.

A more recent version, XXH64, has been created thanks to [Mathias Westerdahl](https://github.com/JCash),
which offers superior speed and dispersion for 64-bit systems.
Note however that 32-bit applications will still run faster using the 32-bit version.

SMHasher speed test, compiled using GCC 4.8.2, on Linux Mint 64-bit.
The reference system uses a Core i5-3340M @2.7GHz

| Version    | Speed on 64-bit | Speed on 32-bit |
|------------|------------------|------------------|
| XXH64      | 13.8 GB/s        |  1.9 GB/s        |
| XXH32      |  6.8 GB/s        |  6.0 GB/s        |

