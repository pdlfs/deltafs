/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "bbos_env.h"

namespace pdlfs {
namespace bbos {

// Convert a hierarchical file path to a flattened bbos object name
std::string BbosName(const char* fname) {
  std::string result(fname);
  std::string::size_type start_pos = 0;
  while ((start_pos = result.find("/", start_pos)) != std::string::npos) {
    result.replace(start_pos, 1, "--");
    start_pos += 2;
  }
  return result;
}

#if defined(DELTAFS_BBOS)
// Map bbos errors to deltafs errors
Status BbosError(const std::string& err_msg, int err_num) {
  switch (err_num) {
    case BB_INVALID_READ:
      return Status::IOError(err_msg, "past EOF (BB_INVALID_READ)");
    case BB_ERROBJ:
      return Status::IOError(err_msg, "unable to create object (BB_ERROBJ)");
    case BB_ENOCONTAINER:
      return Status::NotFound(
          err_msg, "underlying container not found (BB_ENOCONTAINER)");
    case BB_ENOOBJ:
      return Status::NotFound(err_msg, "no such object (BB_ENOOBJ)");
    case BB_FAILED:
      return Status::IOError(err_msg, "general error (BB_FAILED)");
    default:
      return Status::Corruption(err_msg);
  }
}

// A partial Env implementation using bbos api to redirect plfsdir I/O to an
// underlying bbos service. Not all Env operations are supported.
class BbosEnv : public Env {
 private:
  bbos_handle_t bb_handle_;

 public:
  explicit BbosEnv(bbos_handle_t bb_handle) : bb_handle_(bb_handle) {}

  virtual ~BbosEnv() { bbos_finalize(bb_handle_); }

  virtual Status NewSequentialFile(const char* fname, SequentialFile** r) {
    *r = new BbosSequentialFile(BbosName(fname), bb_handle_);
    return Status::OK();
  }

  virtual Status NewRandomAccessFile(const char* fname, RandomAccessFile** r) {
    *r = new BbosRandomAccessFile(BbosName(fname), bb_handle_);
    return Status::OK();
  }

  virtual Status NewWritableFile(const char* fname, WritableFile** r) {
    const std::string obj_name = BbosName(fname);
    const BbosType obj_type = TryResolveBbosType(obj_name);
    int ret = bbos_mkobj(bb_handle_, obj_name.c_str(),
                         static_cast<bbos_mkobj_flag_t>(obj_type));
    if (ret != BB_SUCCESS) {
      std::string bbos_err_msg("cannot create bbos object ");
      bbos_err_msg += obj_name;
      bbos_err_msg += " (type=";
      bbos_err_msg +=
          (obj_type == BbosType::kIndex) ? "READ_OPTIMIZED" : "WRITE_OPTIMIZED";
      bbos_err_msg += ")";
      *r = NULL;
      return BbosError(bbos_err_msg, ret);
    } else {
      *r = new BbosWritableFile(obj_name, bb_handle_);
      return Status::OK();
    }
  }

  virtual Status DeleteFile(const char* fname) {
    return Status::OK();  // Noop
  }

  virtual Status GetFileSize(const char* fname, uint64_t* file_size) {
    const std::string obj_name = BbosName(fname);
    off_t ret = bbos_get_size(bb_handle_, obj_name.c_str());
    if (ret < 0) {
      std::string bbos_err_msg("cannot get bbos object length ");
      bbos_err_msg += obj_name;
      *file_size = 0;
      return BbosError(bbos_err_msg, ret);
    } else {
      *file_size = static_cast<uint64_t>(ret);
      return Status::OK();
    }
  }

  virtual bool FileExists(const char* fname) {
    uint64_t ignored_size;
    Status s = GetFileSize(fname, &ignored_size);
    return s.ok();
  }

  virtual Status CreateDir(const char* dirname) { return Status::OK(); }
  virtual Status AttachDir(const char* dirname) { return Status::OK(); }
  virtual Status DeleteDir(const char* dirname) { return Status::OK(); }
  virtual Status DetachDir(const char* dirname) { return Status::OK(); }

  virtual Status GetChildren(const char* dirname, std::vector<std::string>*) {
    return Status::OK();
  }

  virtual Status CopyFile(const char* src, const char* dst) {
    return Status::NotSupported(Slice());
  }

  virtual Status RenameFile(const char* src, const char* dst) {
    return Status::NotSupported(Slice());
  }

  virtual Status LockFile(const char* fname, FileLock** lock) {
    return Status::NotSupported(Slice());
  }

  virtual Status UnlockFile(FileLock* lock) {
    return Status::NotSupported(Slice());
  }

  virtual void Schedule(void (*function)(void*), void* arg) {
    Env::Default()->Schedule(function, arg);
  }

  virtual void StartThread(void (*function)(void*), void* arg) {
    Env::Default()->StartThread(function, arg);
  }

  virtual Status GetTestDirectory(std::string* path) {
    return Env::Default()->GetTestDirectory(path);
  }

  virtual Status NewLogger(const char* fname, Logger** result) {
    return Env::Default()->NewLogger(fname, result);
  }

  virtual uint64_t NowMicros() { return Env::Default()->NowMicros(); }

  virtual void SleepForMicroseconds(int micros) {
    Env::Default()->SleepForMicroseconds(micros);
  }

  virtual Status FetchHostname(std::string* hostname) {
    return Env::Default()->FetchHostname(hostname);
  }

  virtual Status FetchHostIPAddrs(std::vector<std::string>* ips) {
    return Env::Default()->FetchHostIPAddrs(ips);
  }
};

// The following method creates a new bbos Env object
// each time it is called. The result should be deleted by the caller
// when it is no longer needed.
// Both hg_class and hg_ctx may be NULL.
Status BbosInit(Env** result, const char* hg_local, const char* hg_remote,
                void* hg_class, void* hg_ctx) {
  *result = NULL;
  bbos_handle_t bb_handle;
  Status s;

  if (hg_local == NULL) {
    s = Status::InvalidArgument("mercury uri is null");
  } else if (hg_remote == NULL) {
    s = Status::InvalidArgument("mercury remote uri is null");
  } else if (hg_class != NULL && hg_ctx != NULL) {
    int ret = bbos_init_ext(hg_local, hg_remote, hg_class, hg_ctx, &bb_handle);
    if (ret != BB_SUCCESS) {
      s = BbosError(
          "cannot create bbos handle with an existing mercury context", ret);
    }
  } else {
    int ret = bbos_init(hg_local, hg_remote, &bb_handle);
    if (ret != BB_SUCCESS) {
      s = BbosError("cannot create bbos handle", ret);
    }
  }

  if (s.ok()) {
    *result = new BbosEnv(bb_handle);
  }

  return s;
}
#endif

}  // namespace bbos
}  // namespace pdlfs
