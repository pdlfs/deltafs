/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/slice.h"
#include "pdlfs-common/status.h"

#include <stdint.h>
#include <string>

namespace pdlfs {

class SequentialFile;
class RandomAccessFile;
class WritableFile;

class Env;

// An Osd is an abstract interface used by high-level systems to access a shared
// underlying object storage service, such as Ceph RADOS, Amazon S3, LinkedIn
// Ambry, or Openstack Swift.
//
// All Osd implementation should be safe for concurrent access from
// multiple threads without any external synchronization.
class Osd {
 public:
  Osd() {}
  virtual ~Osd();

  // Create an Osd instance on top of an existing Env instance. The caller may
  // specify a directory prefix so that all objects will be stored under that
  // path. If "env" is NULL, the result of Env::Default() will be used. The
  // caller must delete the result when it is no longer needed. The "*env" must
  // remain live while the result is in use.
  static Osd* FromEnv(const char* prefix, Env* env = NULL);

  // Create a brand new sequentially-readable object with the specified name.
  // On success, stores a pointer to the new object in *r and returns OK.
  // On failure stores NULL in *r and returns non-OK.  If the object does not
  // exist, returns a non-OK status.
  //
  // The returned object can only be accessed by one thread at a time.
  virtual Status NewSequentialObj(const char* name, SequentialFile** r) = 0;

  // Create a brand new random access read-only object with the specified name.
  // On success, stores a pointer to the new object in *r and returns OK.
  // On failure stores NULL in *r and returns non-OK.  If the object does not
  // exist, returns a non-OK status.
  //
  // The returned object may be concurrently accessed by multiple threads.
  virtual Status NewRandomAccessObj(const char* name, RandomAccessFile** r) = 0;

  // Create a handle that writes to a new object with the specified name.
  // Deletes any existing object with the same name and creates a new object.
  // On success, stores a pointer to the new object in *r and returns OK.
  // On failure stores NULL in *r and returns non-OK.
  //
  // The returned object can only be accessed by one thread at a time.
  virtual Status NewWritableObj(const char* name, WritableFile** r) = 0;

  // Return true iff the named object exists.
  virtual bool Exists(const char* name) = 0;

  // Store the size of a named object in *obj_size.
  virtual Status Size(const char* name, uint64_t* obj_size) = 0;

  // Delete the named object.
  virtual Status Delete(const char* name) = 0;

  // Create a new object with the specified data.
  virtual Status Put(const char* name, const Slice& data) = 0;

  // Obtain the contents of a given object.
  virtual Status Get(const char* name, std::string* data) = 0;

  // Copy src to dst.
  virtual Status Copy(const char* src, const char* dst) = 0;

 private:
  // No copying allowed
  void operator=(const Osd&);
  Osd(const Osd&);
};

}  // namespace pdlfs
