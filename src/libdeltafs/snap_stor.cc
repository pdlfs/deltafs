/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/env.h"
#include "pdlfs-common/fio.h"
#include "pdlfs-common/logging.h"
#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/pdlfs_platform.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/strutil.h"

#include "snap_stor.h"

#include <map>

namespace pdlfs {

class StorImpl : public Stor {
 public:
  virtual ~StorImpl() {
    if (!metadata_env_is_system_) {
      delete metadata_env_;
    }
    if (!data_env_is_system_) {
      delete data_env_;
    }

    delete fio_;
  }

  virtual std::string MetadataHome() const {
    assert(metadata_home_.size() != 0);
    return metadata_home_;
  }

  virtual Env* MetadataEnv() const {
    assert(metadata_env_ != NULL);
    return metadata_env_;
  }

  virtual std::string DataHome() const {
    assert(data_home_.size() != 0);
    return data_home_;
  }

  virtual Env* DataEnv() const {
    assert(data_env_ != NULL);
    return data_env_;
  }

  virtual Fio* FileIO() const {
    assert(fio_ != NULL);
    return fio_;
  }

  virtual uint64_t IdealReqSize() const { return io_size_; }

  virtual bool IsReadOnly() const { return readonly_; }

 private:
  StorImpl();
  friend class Stor;
  uint64_t io_size_;
  bool readonly_;
  std::string metadata_home_;
  bool metadata_env_is_system_;
  Env* metadata_env_;
  std::string data_home_;
  bool data_env_is_system_;
  Env* data_env_;
  Fio* fio_;
};

Stor::~Stor() {}

StorImpl::StorImpl() {
  io_size_ = 0;
  readonly_ = false;
  metadata_env_is_system_ = false;
  metadata_env_ = NULL;
  data_env_is_system_ = false;
  data_env_ = NULL;
  fio_ = NULL;
}

static void LogMetadataPath(const std::string& path) {
#if VERBOSE >= 1
  Verbose(__LOG_ARGS__, 1, "snap_stor.metadata_path -> %s", path.c_str());
#endif
}

static void LogDataPath(const std::string& path) {
#if VERBOSE >= 1
  Verbose(__LOG_ARGS__, 1, "snap_stor.data_path -> %s", path.c_str());
#endif
}

static void ParseOptions(std::map<std::string, std::string>* map,
                         const char* input) {
  std::vector<std::string> confs;
  size_t n = SplitString(&confs, input, ',');
  for (size_t i = 0; i < n; i++) {
    std::vector<std::string> pair;
    SplitString(&pair, confs[i].c_str(), '=');
    if (pair.size() == 2) {
      (*map)[pair[0]] = pair[1];
    }
  }
}

static Status ParseNumber(const std::map<std::string, std::string>& options,
                          const std::string& key, uint64_t* result) {
  if (!ParsePrettyNumber(options.at(key), result)) {
    return Status::InvalidArgument(key, options.at(key));
  } else {
    return Status::OK();
  }
}

static Status ParseBool(const std::map<std::string, std::string>& options,
                        const std::string& key, bool* result) {
  if (!ParsePrettyBool(options.at(key), result)) {
    return Status::InvalidArgument(key, options.at(key));
  } else {
    return Status::OK();
  }
}

//   type                         conf
// ---------|-----------------------------------------------
//  common  |           type=posixfs|rados|hdfs
//          |             readonly=true|false
// ---------|-----------------------------------------------
//  posixfs |         mode=unbufferedio|directio
//          |             root=/path/to/root
//          |           metadata_folder=dirname
//          |             data_folder=dirname
// ---------|-----------------------------------------------
//   rados  |                     TODO
// ---------|-----------------------------------------------
Status Stor::Open(const std::string& conf, Stor** ptr) {
  *ptr = NULL;
  std::map<std::string, std::string> options;
  ParseOptions(&options, conf.c_str());
#if VERBOSE >= 1
  std::map<std::string, std::string>::iterator it;
  for (it = options.begin(); it != options.end(); ++it) {
    Verbose(__LOG_ARGS__, 1, "snap_stor.%s -> %s", it->first.c_str(),
            it->second.c_str());
  }
#endif
  Status s;
  std::string type = "posixfs";
  bool readonly = false;
  uint64_t io_size = 128 << 10;
  if (options.count("type") != 0) {
    type = options["type"];
  }
  if (options.count("readonly") != 0) {
    s = ParseBool(options, "readonly", &readonly);
    if (!s.ok()) {
      return s;
    }
  }
  if (options.count("io_size") != 0) {
    s = ParseNumber(options, "io_size", &io_size);
    if (!s.ok()) {
      return s;
    }
  }

// RADOS
#if defined(PDLFS_RADOS)
  if (type == "rados") {
    s = Status::NotSupported(Slice());
  }
#endif
// POSIX
#if defined(PDLFS_PLATFORM_POSIX)
  if (type == "posixfs" || type == "posix") {
    StorImpl* impl = new StorImpl;
    std::string env_type = "posix";
    std::string root = "/tmp";
    std::string metadata = "deltafs_metadata";
    std::string data = "deltafs_data";
    std::string fio_type = "posix";
    std::string fio_conf = "root=";
    if (options.count("root") != 0) {
      root = options["root"];
      if (root.empty()) {
        root = ".";
      }
    }

    {
      // Metadata path
      if (options.count("metatdata_folder") != 0) {
        metadata = options["metadata_folder"];
      }
      if (metadata.empty()) {
        metadata = root;
      } else if (metadata[0] != '/') {
        metadata = root + "/" + metadata;
      }

      LogMetadataPath(metadata);
    }

    {
      // Data path
      if (options.count("data_folder") != 0) {
        data = options["data_folder"];
      }
      if (data.empty()) {
        data = root;
      } else if (data[0] != '/') {
        data = root + "/" + data;
      }

      LogDataPath(data);
      fio_conf += data;
    }

    if (options.count("mode") != 0) {
      env_type += ".";
      env_type += options["mode"];
    }

    impl->io_size_ = io_size;
    impl->readonly_ = readonly;

    impl->fio_ = Fio::Open(fio_type.c_str(), fio_conf.c_str());
    impl->metadata_env_ =
        Env::Open(env_type.c_str(), NULL, &impl->metadata_env_is_system_);
    impl->data_env_ =
        Env::Open(env_type.c_str(), NULL, &impl->data_env_is_system_);

    impl->metadata_home_ = metadata;
    impl->data_home_ = data;

    if (impl->fio_ == NULL) {
      s = Status::NotSupported("no such fio type", fio_type);
    } else if (impl->metadata_env_ == NULL) {
      s = Status::NotSupported("no sucn env type", env_type);
    } else if (impl->data_env_ == NULL) {
      s = Status::NotSupported("no such env type", env_type);
    }

    if (!s.ok()) {
      delete impl;
    } else {
      *ptr = impl;
    }
  }
#endif

  return s;
}

}  // namespace pdlfs
