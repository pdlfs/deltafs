#pragma once

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

#include "pdlfs-common/slice.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

class Env;
class Fio;

// Adaptor that allows access to a file system snapshot that is
// stored in a particular underlying storage system.
class Stor {
 public:
  Stor() {}
  virtual ~Stor();

  // Open an adaptor instance using the specified setting.
  // Return OK on success, or a non-OK status on errors.
  static Status Open(const std::string& conf, Stor** ptr);

  // True if only read access is allowed.
  virtual bool IsReadOnly() const = 0;

  // The ideal size of data packed within each read/write request.
  // Return 0 if such size is not known to us.
  virtual uint64_t IdealReqSize() const = 0;

  // Return the path to raw file system metadata.
  virtual std::string MetadataHome() const = 0;

  // Return the Env instance for accessing raw file system metadata.
  // The returned Env will remain valid for the lifetime of this adaptor
  // and cannot be deleted by the caller.
  // The result of metadata_env() should never be NULL.
  virtual Env* MetadataEnv() const = 0;

  // Return the path to raw file data.
  virtual std::string DataHome() const = 0;

  // Return the Env instance for access raw file data.
  // The returned Env will remain valid for the lifetime of this adaptor
  // and cannot be deleted by the caller.
  // The result of data_env() should never be NULL.
  virtual Env* DataEnv() const = 0;

  // Return the Fio instance for managing file data.
  // The returned Fio will remain valid for the lifetime of this adaptor
  // and cannot be deleted by the caller.
  // The result of fio() should never be NULL.
  virtual Fio* FileIO() const = 0;

 private:
  // No copying allowed
  void operator=(const Stor&);
  Stor(const Stor&);
};

}  // namespace pdlfs
