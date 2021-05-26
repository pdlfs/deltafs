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
#include <vector>

namespace pdlfs {

class SequentialFile;
class RandomAccessFile;
class WritableFile;
class Logger;
class Osd;
class Env;

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
  Slice name;

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
extern Status WriteStringToFile(Osd* osd, const Slice& data, const char* name);
// A utility routine: write "data" to the named file and Sync() it.
// The file is actually backed by an object.
extern Status WriteStringToFileSync(Osd* osd, const Slice& data,
                                    const char* name);

// A utility routine: read contents from a named file into *data.
// The file is actually backed by an object.
extern Status ReadFileToString(Osd* osd, const char* name, std::string* data);

// Options controlling an ofs.
struct OfsOptions {
  OfsOptions();
  // Defer garbage collection until the next directory mount.
  // Default: false
  bool deferred_gc;
  // Sync log when unmounting a directory.
  // Default: false
  bool sync_log_on_close;
  // Logger for ofs internal/error information.
  // Default: NULL
  Logger* info_log;
};

// We use Ofs to bridge the Osd world to the traditional file system world. This
// is achieved by mapping each "file" to an underlying object and each
// "directory" to a special container object that holds references to other
// objects. The traditional file system namespace is emulated by dynamically
// mounting container objects at specific tree paths, although we don't expect
// or allow nested containers.
class Ofs {
 public:
  Ofs(const OfsOptions& options, Osd* osd);
  ~Ofs();

  // Return true iff the named file exists in a mounted file set.
  // Note: the real identify of the file is subject to
  // how file sets are dynamically mounted.
  bool FileExists(const char* fname);

  // Return true iff there is a file set mounted at the given path.
  bool FileSetExists(const char* dirname);

  // Delete the named file in a mounted file set.
  // If there is no file set mounted at the given path or if the file set
  // does not have the file, a NotFound status is returned.
  Status DeleteFile(const char* fname);

  // Mount a file set to the specified path.
  // Return OK on success, and a non-OK status on errors.
  Status MountFileSet(const MountOptions& options, const char* dirname);

  // Unmount the file set currently mounted at the specified path.
  // Return OK on success, and a non-OK status on errors.
  Status UnmountFileSet(const UnmountOptions& options, const char* dirname);

  // Retrieve all files within a mounted file set.
  // Return OK on success, and a non-OK status on errors.
  Status GetChildren(const char* dirname, std::vector<std::string>* names);

  // Return the size of the named file.
  Status GetFileSize(const char* fname, uint64_t* size);

  // Read contents of the named file into *data in a single I/O operation.
  Status ReadFileToString(const char* fname, std::string* data);

  // Write data to the named file in a single I/O operation.
  Status WriteStringToFile(const char* fname, const Slice& data);

  // Copy src to dst.
  Status CopyFile(const char* src, const char* dst);

  // Rename src to dst.
  Status Rename(const char* src, const char* dst);

  // Sync a specific file set mounted at the given path.
  // Return OK on success, and a non-OK status on errors.
  Status SynFileSet(const char* dirname);

  // Create a brand new sequentially-readable file with the specified name.
  Status NewSequentialFile(const char* fname, SequentialFile** result);

  // Create a brand new random access read-only file with the specified name.
  Status NewRandomAccessFile(const char* fname, RandomAccessFile** result);

  // Create a new append-only file with the specified name.
  Status NewWritableFile(const char* fname, WritableFile** result);

  // Return the name of the underlying object associated with the given file.
  std::string TEST_LookupFile(const char* fname);

 private:
  // No copying allowed
  void operator=(const Ofs&);
  Ofs(const Ofs&);

  struct ResolvedPath {
    Slice mntptr;
    Slice base;
  };

  class Impl;
  Impl* impl_;
};

}  // namespace pdlfs
