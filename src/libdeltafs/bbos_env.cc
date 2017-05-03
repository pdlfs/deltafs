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

// Map bbos errors to deltafs errors
Status BbosError(const std::string& err_msg, int err_num) {
  switch (err_num) {
    case BB_INVALID_READ:
      return Status::IOError(err_msg, "read past EOF");
    case BB_ERROBJ:
      return Status::IOError(
          err_msg, "cannot create object");  // XXX: object already exists?
    case BB_ENOCONTAINER:
      return Status::NotFound(err_msg, "container object not found");
    case BB_ENOOBJ:
      return Status::NotFound(err_msg, "no such object");
    default:
      return Status::Corruption(err_msg, "bbos error");
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

  virtual Status NewSequentialFile(const Slice& fname,
                                   SequentialFile** result) {
    // XXX: do we need to check object existence and how?
    *result = new BbosSequentialFile(BbosName(fname.c_str()), bb_handle_);
    return Status::OK();
  }

  virtual Status NewRandomAccessFile(const Slice& fname,
                                     RandomAccessFile** result) {
    // XXX: do we need to check object existence and how?
    *result = new BbosRandomAccessFile(BbosName(fname.c_str()), bb_handle_);
    return Status::OK();
  }

  virtual Status NewWritableFile(const Slice& fname, WritableFile** result) {
    std::string obj_name = BbosName(fname.c_str());
    int ret = bbos_mkobj(
        bb_handle_, obj_name.c_str(),
        static_cast<bbos_mkobj_flag_t>(TryResolveBbosType(obj_name)));
    if (ret != BB_SUCCESS) {
      std::string bbos_err_msg("cannot create bbos object '");
      bbos_err_msg += obj_name;
      bbos_err_msg += "'";
      *result = NULL;
      return BbosError(bbos_err_msg, ret);
    } else {
      *result = new BbosWritableFile(obj_name, bb_handle_);
      return Status::OK();
    }
  }

  virtual Status DeleteFile(const Slice& fname) {
    return Status::OK();  // Noop
  }

  virtual Status GetFileSize(const Slice& fname, uint64_t* file_size) {
    std::string obj_name = BbosName(fname.c_str());
    off_t ret = bbos_get_size(bb_handle_, obj_name.c_str());
    if (ret < 0) {
      std::string bbos_err_msg("cannot get bbos object length '");
      bbos_err_msg += obj_name;
      bbos_err_msg += "'";
      *file_size = 0;
      return BbosError(bbos_err_msg, ret);
    } else {
      *file_size = static_cast<uint64_t>(ret);
      return Status::OK();
    }
  }

  virtual bool FileExists(const Slice& fname) {
    uint64_t ignored_size;
    Status s = GetFileSize(fname, &ignored_size);
    if (s.ok()) {
      return true;
    } else {
      return false;
    }
  }

  virtual Status CreateDir(const Slice& dirname) { return Status::OK(); }
  virtual Status AttachDir(const Slice& dirname) { return Status::OK(); }
  virtual Status DeleteDir(const Slice& dirname) { return Status::OK(); }
  virtual Status DetachDir(const Slice& dirname) { return Status::OK(); }

  virtual Status GetChildren(const Slice& dir, std::vector<std::string>*) {
    return Status::OK();
  }

  virtual Status CopyFile(const Slice& src, const Slice& target) {
    return Status::NotSupported(Slice());
  }

  virtual Status RenameFile(const Slice& src, const Slice& target) {
    return Status::NotSupported(Slice());
  }

  virtual Status LockFile(const Slice& fname, FileLock** lock) {
    return Status::NotSupported(Slice());
  }

  virtual Status UnlockFile(FileLock* lock) {
    return Status::NotSupported(Slice());
  }

  virtual void Schedule(void (*function)(void* arg), void* arg) {
    Env::Default()->Schedule(function, arg);
  }

  virtual void StartThread(void (*function)(void* arg), void* arg) {
    Env::Default()->StartThread(function, arg);
  }

  virtual Status GetTestDirectory(std::string* path) {
    return Env::Default()->GetTestDirectory(path);
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
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
Status CreateNewBbosEnv(Env** result, const char* hg_local,
                        const char* hg_remote, void* hg_class, void* hg_ctx) {
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
      s = BbosError("cannot create bbos handle", ret);
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

}  // namespace bbos
}  // namespace pdlfs
