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
#include "rados_db_env.h"

#include "pdlfs-common/env_files.h"

namespace pdlfs {
namespace rados {

RadosDbEnvWrapper::RadosDbEnvWrapper(  ///
    const RadosDbEnvOptions& options, Env* base_env)
    : EnvWrapper(base_env), options_(options), owns_env_(false), env_(NULL) {
  if (!options_.info_log) {
    options_.info_log = Logger::Default();
  }
}

RadosDbEnvWrapper::~RadosDbEnvWrapper() {
  if (owns_env_) {
    delete env_;
  }
}

bool RadosDbEnvWrapper::PathOnRados(const char* pathname) {
  const size_t n = rados_root_->size();
  if (n == 1) return true;
  if (strncmp(pathname, rados_root_->c_str(), n) != 0) return false;
  return (pathname[n] == '/' || pathname[n] == '\0');
}

bool RadosDbEnvWrapper::FileOnRados(const char* fname) {
  if (PathOnRados(fname)) return TypeOnRados(TryResolveFileType(fname));
  return false;
}

bool RadosDbEnvWrapper::FileExists(const char* fname) {
  if (FileOnRados(fname)) return env_->FileExists(fname);
  return target()->FileExists(fname);
}

Status RadosDbEnvWrapper::GetFileSize(const char* fname, uint64_t* size) {
  if (FileOnRados(fname)) return env_->GetFileSize(fname, size);
  return target()->GetFileSize(fname, size);
}

Status RadosDbEnvWrapper::DeleteFile(const char* fname) {
  if (FileOnRados(fname)) return env_->DeleteFile(fname);
  return target()->DeleteFile(fname);
}

Status RadosDbEnvWrapper::NewSequentialFile(  ///
    const char* fname, SequentialFile** r) {
  if (FileOnRados(fname)) return env_->NewSequentialFile(fname, r);
  return target()->NewSequentialFile(fname, r);
}

Status RadosDbEnvWrapper::NewRandomAccessFile(  ///
    const char* fname, RandomAccessFile** r) {
  if (FileOnRados(fname)) return env_->NewRandomAccessFile(fname, r);
  return target()->NewRandomAccessFile(fname, r);
}

Status RadosDbEnvWrapper::LocalCreateOrAttachDir(  ///
    const char* dirname, const Status& remote_status) {
  Status s;
  // After this call, we want to make sure that the local dir exists
  // if the remote dir has been, or has already been, created.
  if (remote_status.ok() || remote_status.IsAlreadyExists()) {
    s = target()->CreateDir(dirname);
#if VERBOSE >= 3
    Log(options_.info_log, 1, "Creating dir %s: %s (rados), %s (lo)", dirname,
        remote_status.ToString().c_str(), s.ToString().c_str());
#endif
    if (s.ok() || s.IsAlreadyExists()) {
      s = remote_status;  // Return the remote status to caller
    }
  }
  return s;
}

Status RadosDbEnvWrapper::CreateDir(const char* dirname) {
  if (PathOnRados(dirname)) {
    return LocalCreateOrAttachDir(dirname, env_->CreateDir(dirname));
  }
  return target()->CreateDir(dirname);
}

Status RadosDbEnvWrapper::AttachDir(const char* dirname) {
  if (PathOnRados(dirname)) {
    return LocalCreateOrAttachDir(dirname, env_->AttachDir(dirname));
  }
  return target()->AttachDir(dirname);
}

// Return not found when neither the local nor the remote dir has been found.
// Return OK if either the local or the remote dir has been found and has been
// successfully deleted. Return a non-OK status if deleting either one resulted
// in an error.
Status RadosDbEnvWrapper::LocalDeleteDir(  ///
    const char* dirname, const Status& remote_status) {
  Status s, local_status;
  if (remote_status.ok()) {
    local_status = target()->DeleteDir(dirname);
    s = local_status;
    if (s.IsNotFound()) {
      s = remote_status;  // OK for local to miss when the remote exists
    }
  } else if (remote_status.IsNotFound()) {
    local_status = target()->DeleteDir(dirname);
    s = local_status;
  }
#if VERBOSE >= 3
  Log(options_.info_log, 1, "Deleting dir %s: %s (rados), %s (lo)", dirname,
      remote_status.ToString().c_str(), local_status.ToString().c_str());
#endif
  return s;
}

Status RadosDbEnvWrapper::DeleteDir(const char* dirname) {
  if (PathOnRados(dirname)) {
    return LocalDeleteDir(dirname, env_->DeleteDir(dirname));
  }
  return target()->DeleteDir(dirname);
}

Status RadosDbEnvWrapper::DetachDir(const char* dirname) {
  if (PathOnRados(dirname)) return env_->DetachDir(dirname);
  return target()->DetachDir(dirname);
}

Status RadosDbEnvWrapper::CopyFile(const char* src, const char* dst) {
  const bool src_on_rados = FileOnRados(src);
  const bool dst_on_rados = FileOnRados(dst);
  if (src_on_rados ^ dst_on_rados)
    return Status::NotSupported("Cannot copy files across env boundaries");
  if (src_on_rados) return env_->CopyFile(src, dst);
  return target()->CopyFile(src, dst);
}

Status RadosDbEnvWrapper::RenameFile(const char* src, const char* dst) {
  const bool src_on_rados = FileOnRados(src);
  const bool dst_on_rados = FileOnRados(dst);
  if (TryResolveFileType(src) == kTempFile && !src_on_rados && dst_on_rados)
    return RenameLocalTmpToRados(src, dst);
  if (src_on_rados ^ dst_on_rados)
    return Status::NotSupported("Cannot rename files across env boundaries");
  if (src_on_rados) return env_->RenameFile(src, dst);
  return target()->RenameFile(src, dst);
}

Status RadosDbEnvWrapper::GetChildren(const char* dirname,
                                      std::vector<std::string>* r) {
  r->clear();
  Status s = target()->GetChildren(dirname, r);
  if (PathOnRados(dirname)) {
    std::vector<std::string> rr;
    // The previous status is over-written because it is all right for
    // local directory listing to fail since it is possible for a directory
    // to not have a local mirror.
    s = env_->GetChildren(dirname, &rr);
    if (s.ok()) {
      r->insert(r->end(), rr.begin(), rr.end());
    }
  }
  return s;
}

Status RadosDbEnvWrapper::NewWritableFile(const char* fname, WritableFile** r) {
  if (FileOnRados(fname)) {
    Status s = env_->NewWritableFile(fname, r);
    if (s.ok()) {
      uint64_t buf_sz = 0;
      switch (TryResolveFileType(fname)) {
        case kLogFile:
          buf_sz = options_.write_ahead_log_buf_size;
          break;
        case kTableFile:
          buf_sz = options_.table_file_buf_size;
          break;
        default:  // No buffers
          break;
      }
      if (buf_sz) {
        *r = new MinMaxBufferedWritableFile(*r, buf_sz, buf_sz);
      }
    }
    return s;
  } else {
    return target()->NewWritableFile(fname, r);
  }
}

Status RadosDbEnvWrapper::RenameLocalTmpToRados(const char* tmp,
                                                const char* dst) {
  std::string contents;
  Status s = ReadFileToString(target(), tmp, &contents);
  if (s.ok()) {
    s = env_->TEST_GetOfs()->WriteStringToFile(dst, contents);
    if (s.ok()) {
      target()->DeleteFile(tmp);
    }
  }
  return s;
}

}  // namespace rados
}  // namespace pdlfs
