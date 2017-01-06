#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdint.h>
#include <string>

#include "pdlfs-common/slice.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

class SequentialFile;
class RandomAccessFile;
class WritableFile;

// An OSD is an abstract interface used by high-level systems to
// access a shared underlying object storage service, such as
// Ceph RADOS, LinkedIn Ambry, or Openstack Swift.
//
// All OSD implementation should be safe for concurrent access from
// multiple threads without any external synchronization.
class OSD {
 public:
  OSD() {}
  virtual ~OSD();

  // Create a brand new sequentially-readable object with the specified name.
  // On success, stores a pointer to the new object in *result and returns OK.
  // On failure stores NULL in *result and returns non-OK.  If the object
  // does not exist, returns a non-OK status.
  //
  // The returned object can only be accessed by one thread at a time.
  virtual Status NewSequentialObj(const Slice& name,
                                  SequentialFile** result) = 0;

  // Create a brand new random access read-only object with the
  // specified name.  On success, stores a pointer to the new object in
  // *result and returns OK.  On failure stores NULL in *result and
  // returns non-OK.  If the object does not exist, returns a non-OK
  // status.
  //
  // The returned object may be concurrently accessed by multiple threads.
  virtual Status NewRandomAccessObj(const Slice& name,
                                    RandomAccessFile** result) = 0;

  // Create a handle that writes to a new object with the specified
  // name.  Deletes any existing object with the same name and creates a
  // new object.  On success, stores a pointer to the new object in
  // *result and returns OK.  On failure stores NULL in *result and
  // returns non-OK.
  //
  // The returned object can only be accessed by one thread at a time.
  virtual Status NewWritableObj(const Slice& name, WritableFile** result) = 0;

  // Return true iff the named object exists.
  virtual bool Exists(const Slice& name) = 0;

  // Store the size of a named object in *obj_size.
  virtual Status Size(const Slice& name, uint64_t* obj_size) = 0;

  // Delete the named object.
  virtual Status Delete(const Slice& name) = 0;

  // Create a new object with the specified data.
  virtual Status Put(const Slice& name, const Slice& data) = 0;

  // Obtain the contents of a given object.
  virtual Status Get(const Slice& name, std::string* data) = 0;

  // Copy src to dst.
  virtual Status Copy(const Slice& src, const Slice& dst) = 0;

 private:
  // No copying allowed
  OSD(const OSD&);
  void operator=(const OSD&);
};

}  // namespace pdlfs
