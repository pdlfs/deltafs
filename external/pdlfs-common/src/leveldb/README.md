**LevelDB is a fast key-value storage library that provides an ordered mapping from string keys to string values.**

[![Build Status](https://travis-ci.org/pdlfs/pdlfs-common.svg?branch=master)](https://travis-ci.org/pdlfs/pdlfs-common)

LevelDB is originally developed by Sanjay Ghemawat (sanjay@google.com) and Jeff Dean (jeff@google.com)

# Features
  * Keys and values are arbitrary byte arrays.
  * Data is stored sorted by key.
  * Callers can provide a custom comparison function to override the sort order.
  * The basic operations are `Put(key,value)`, `Get(key)`, `Delete(key)`.
  * Multiple changes can be made in one atomic batch.
  * Users can create a transient snapshot to get a consistent view of data.
  * Forward and backward iteration is supported over the data.
  * Data is automatically compressed using the [Snappy compression library](http://google.github.io/snappy/).
  * External activity (file system operations etc.) is relayed through a virtual interface so users can customize the operating system interactions.

# Enhancements
  * A readonly DB implementation that can share a same db-image with a read-write DB instance and can follow it to access new updates.
  * A new bulk insertion interface that enables efficient data injection through the native data representation (`SSTable`).
  * A new dump interface that allows a range of keys to be extracted as SSTables.
  * Extended `Get(key)` operators that can return only a prefix of the value and can store data in a direct buffer pre-allocated by applications.
  * A more customizable DB interface that drives better integration with applications and can better adapt to different application needs.

# Documentation
  The original [LevelDB library documentation](https://rawgit.com/google/leveldb/master/doc/index.html) is online.

# Limitations
  * This is not a SQL database.  It does not have a relational data model, it does not support SQL queries, and it has no support for indexes.
  * There is no client-server support builtin to the library.  An application that needs such support will have to wrap their own server around the library.
  * All DB operators are synchronous and will block until the operation is finished.
  * There is only a single background compaction process.
  * Only a single writer can be active at any moment.

## Repository contents

See doc/index.html for more explanation. See doc/impl.html for a brief overview of the implementation.

The public interface is in include/pdlfs-common/leveldb/*.h.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Guide to header files:

* **include/pdlfs-common/leveldb/db/db.h**: Main interface to the DB: Start here

* **include/pdlfs-common/leveldb/db/options.h**: Control over the behavior of an entire database,
and also control over the behavior of individual reads and writes.

* **include/pdlfs-common/leveldb/comparator.h**: Abstraction for user-specified comparison function. 
If you want just bytewise comparison of keys, you can use the default
comparator, but clients can write their own comparator implementations if they
want custom ordering (e.g. to handle different character encodings, etc.)

* **include/pdlfs-common/leveldb/iterator.h**: Interface for iterating over data. You can get
an iterator from a DB object.

* **include/pdlfs-common/leveldb/db/write_batch.h**: Interface for atomically applying multiple
updates to a database.

* **include/pdlfs-common/slice.h**: A simple module for maintaining a pointer and a
length into some other byte array.

* **include/pdlfs-common/status.h**: Status is returned from many of the public interfaces
and is used to report success and various kinds of errors.

* **include/pdlfs-common/env.h**: 
Abstraction of the OS environment.  A posix implementation of this interface is
in src/env_posix.cc

* **include/pdlfs-common/leveldb/table.h, include/pdlfs-common/leveldb/table_builder.h**: Lower-level modules that most
clients probably won't use directly
