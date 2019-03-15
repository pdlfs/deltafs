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

#include "pdlfs-common/status.h"

namespace pdlfs {
namespace ioclient {

struct IOClientOptions {
  int argc;
  char** argv;
  // Configuration string to initialize the client.
  std::string conf_str;
  // Optionally set the id of the client
  std::string id;
  // The rank of the client within a communication group
  int rank;
  // Total number of clients
  int comm_sz;
};

// Abstract dir handle;
struct Dir {
 protected:
  virtual ~Dir();
};

// Abstract FS client interface
class IOClient {
 public:
  static IOClient* Factory(const IOClientOptions&);
  // Open a client backed by the local FS
  static IOClient* Default(const IOClientOptions&);
  // Open a client backed by Deltafs
  static IOClient* Deltafs(const IOClientOptions&);

  IOClient() {}
  virtual ~IOClient();
  virtual Status Init() = 0;
  virtual Status Dispose() = 0;

  // Common FS operations
  virtual Status NewFile(const std::string& path) = 0;
  virtual Status DelFile(const std::string& path) = 0;
  virtual Status GetAttr(const std::string& path) = 0;
  virtual Status OpenDir(const std::string& path, Dir**) = 0;
  virtual Status AppendAt(Dir* dir, const std::string& file, const char* data,
                          size_t size) = 0;
  virtual Status FlushEpoch(Dir* dir) = 0;
  virtual Status CloseDir(Dir* dir) = 0;
  virtual Status MakeDir(const std::string& path) = 0;

 private:
  // No copying allowed
  void operator=(const IOClient&);
  IOClient(const IOClient&);
};

}  // namespace ioclient
}  // namespace pdlfs
