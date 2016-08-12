#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdint.h>
#include <string>
#include <vector>

#include "pdlfs-common/slice.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

class OSD;
class Env;
class SequentialFile;
class RandomAccessFile;
class WritableFile;

// Create an OSD adaptor atop an existing Env instance. The caller may
// specify a prefix so that all objects will be stored under that path.
// If "env" is NULL, the result of Env::Default() will be used.
// The caller must delete the result when it is no longer needed.
// The "*env" must remain live while the result is in use.
extern OSD* NewOSDAdaptor(const Slice& prefix = Slice("/"), Env* env = NULL);

struct MountOptions {
  MountOptions();

  // Mount a file set read-only. A read-only file set is immutable
  // so no membership updates are allowed.  It is safe for multiple
  // processes to simultaneously mount a same set read-only.
  // Concurrent write-sharing to a single set is not supported.
  // Default: false.
  bool read_only;

  // If no such file set exists, creates a new one.
  // This option is ignored when read_only is set to true.
  // Default: true.
  bool create_if_missing;

  // Force creating a new file set.
  // This option is ignored when read_only is set to true.
  // Default: false;
  bool error_if_exists;

  // By default, the name of a file set is the same as the last
  // component of its mount point. Use a non-empty name to override this
  // default naming mechanism.
  // Default: Slice().
  Slice set_name;

  // If true, all set membership updates are sent to
  // storage synchronously.
  // Default: false.
  bool sync;

  // If true, the implementation will do aggressive checking of the
  // data it is processing and will stop early if it detects any errors.
  // Default: false.
  bool paranoid_checks;
};

struct UnmountOptions {
  UnmountOptions();

  // If true, the file set will be physically removed after it is unmounted.
  // One cannot delete a set that still contains files.
  // Default: false.
  bool deletion;
};

// A utility routine: write "data" to the named file.
// The file is actually backed by an object.
extern Status WriteStringToFile(OSD* osd, const Slice& data, const Slice& name);
// A utility routine: write "data" to the named file and Sync() it.
// The file is actually backed by an object.
extern Status WriteStringToFileSync(OSD* osd, const Slice& data,
                                    const Slice& name);

// A utility routine: read contents from a named file into *data.
// The file is actually backed by an object.
extern Status ReadFileToString(OSD* osd, const Slice& name, std::string* data);

// We use the OSDEnv to bridge the OSD world to the traditional file system
// world. This is achieved by mapping each "file" to an underlying object
// and each "directory" to a special container object that holds references
// to other objects. The traditional file system namespace is emulated
// by dynamically mounting container objects at specific tree paths,
// although we don't expect or allow nested containers.
class OSDEnv {
 public:
  OSDEnv(OSD* osd);
  ~OSDEnv();

  // Return true iff the named file exists in a mounted file set.
  // Note: the real identify of the file is subject to
  // how file sets are dynamically mounted.
  bool FileExists(const Slice& fname);

  // Return true iff there is a file set mounted at the given path.
  bool FileSetExists(const Slice& dirname);

  // Delete the named file in a mounted file set.
  // If there is no file set mounted at the given path or if the file set
  // does not have the file, a NotFound status is returned.
  Status DeleteFile(const Slice& fname);

  // Mount a file set to the specified path.
  // Return OK on success, and a non-OK status on errors.
  Status MountFileSet(const MountOptions& options, const Slice& dirname);

  // Unmount the file set currently mounted at the specified path.
  // Return OK on success, and a non-OK status on errors.
  Status UnmountFileSet(const UnmountOptions& options, const Slice& dirname);

  // Retrieve all files within a mounted file set.
  // Return OK on success, and a non-OK status on errors.
  Status GetChildren(const Slice& dirname, std::vector<std::string>* names);

  // Return the size of the named file.
  Status GetFileSize(const Slice& fname, uint64_t* size);

  // Read contents of the named file into *data in a single I/O operation.
  Status ReadFileToString(const Slice& fname, std::string* data);

  // Write data to the named file in a single I/O operation.
  Status WriteStringToFile(const Slice& fname, const Slice& data);

  // Copy src to dst.
  Status CopyFile(const Slice& src, const Slice& dst);

  // Sync a specific file set mounted at the given path.
  // Return OK on success, and a non-OK status on errors.
  Status SynFileSet(const Slice& dirname);

  // Create a brand new sequentially-readable file with the specified name.
  Status NewSequentialFile(const Slice& fname, SequentialFile** result);

  // Create a brand new random access read-only file with the specified name.
  Status NewRandomAccessFile(const Slice& fname, RandomAccessFile** result);

  // Create a new append-only file with the specified name.
  Status NewWritableFile(const Slice& fname, WritableFile** result);

  // Return the name of the underlying object associated with the given file.
  std::string TEST_LookupFile(const Slice& fname);

 private:
  // No copying allowed
  void operator=(const OSDEnv&);
  OSDEnv(const OSDEnv&);

  struct ResolvedPath {
    Slice mntptr;
    Slice base;
  };

  class Impl;
  Impl* impl_;
};

}  // namespace pdlfs
