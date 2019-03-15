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

#include "rados_env.h"

#include "pdlfs-common/env_files.h"

namespace pdlfs {
namespace rados {

RadosEnv::~RadosEnv() {
  if (owns_osd_) delete osd_;
  delete ofs_;
}

Status RadosEnv::MountDir(const char* dirname, bool create_dir) {
  Slice path = dirname;
  assert(path.starts_with(rados_root_));
  if (rados_root_.size() > 1) path.remove_prefix(rados_root_.size());
  if (path.empty()) path = "/";
  std::string name = path.ToString();
  for (size_t i = 0; i < name.size(); i++)
    if (name[i] == '/') name[i] = '_';

  MountOptions options;
  options.read_only = !create_dir;
  options.create_if_missing = create_dir;
  options.error_if_exists = false;
  options.name = name;
  Status s = ofs_->MountFileSet(options, dirname);
  if (s.IsAlreadyExists()) {
    // Force a local mirror
    target()->CreateDir(dirname);  // Ignore errors
  } else if (s.ok()) {
    s = target()->CreateDir(dirname);
  }

  return s;
}

Status RadosEnv::UnmountDir(const char* dirname, bool delete_dir) {
  Status s;
  UnmountOptions options;
  options.deletion = delete_dir;
  if (!delete_dir) s = ofs_->SynFileSet(dirname);
  if (s.ok()) s = ofs_->UnmountFileSet(options, dirname);
  if (s.IsNotFound()) {
    // Remove local mirror
    target()->DeleteDir(dirname);  // Ignore errors
  } else if (s.ok()) {
    s = target()->DeleteDir(dirname);
  }

  return s;
}

bool RadosEnv::PathOnRados(const char* pathname) {
  const size_t n = rados_root_.length();
  assert(n != 0);
  if (n == 1) return true;
  if (strncmp(pathname, rados_root_.c_str(), n) != 0) return false;
  return (pathname[n] == '/' || pathname[n] == '\0');
}

bool RadosEnv::FileOnRados(const char* fname) {
  if (PathOnRados(fname)) return TypeOnRados(TryResolveFileType(fname));
  return false;
}

bool RadosEnv::FileExists(const char* fname) {
  if (FileOnRados(fname)) return ofs_->FileExists(fname);
  return target()->FileExists(fname);
}

Status RadosEnv::GetFileSize(const char* fname, uint64_t* size) {
  if (FileOnRados(fname)) return ofs_->GetFileSize(fname, size);
  return target()->GetFileSize(fname, size);
}

Status RadosEnv::DeleteFile(const char* fname) {
  if (FileOnRados(fname)) return ofs_->DeleteFile(fname);
  return target()->DeleteFile(fname);
}

Status RadosEnv::NewSequentialFile(const char* fname, SequentialFile** r) {
  if (FileOnRados(fname)) return ofs_->NewSequentialFile(fname, r);
  return target()->NewSequentialFile(fname, r);
}

Status RadosEnv::NewRandomAccessFile(const char* fname, RandomAccessFile** r) {
  if (FileOnRados(fname)) return ofs_->NewRandomAccessFile(fname, r);
  return target()->NewRandomAccessFile(fname, r);
}

Status RadosEnv::CreateDir(const char* dirname) {
  if (PathOnRados(dirname)) return MountDir(dirname, true);
  return target()->CreateDir(dirname);
}

Status RadosEnv::AttachDir(const char* dirname) {
  if (PathOnRados(dirname)) return MountDir(dirname, false);
  return target()->AttachDir(dirname);
}

Status RadosEnv::DeleteDir(const char* dirname) {
  if (PathOnRados(dirname)) return UnmountDir(dirname, true);
  return target()->DeleteDir(dirname);
}

Status RadosEnv::DetachDir(const char* dirname) {
  if (PathOnRados(dirname)) return UnmountDir(dirname, false);
  return target()->DetachDir(dirname);
}

Status RadosEnv::CopyFile(const char* src, const char* dst) {
  const int src_on_rados = FileOnRados(src);
  const int dst_on_rados = FileOnRados(dst);
  if (src_on_rados ^ dst_on_rados) return Status::NotSupported(Slice());
  if (src_on_rados) return ofs_->CopyFile(src, dst);
  return target()->CopyFile(src, dst);
}

Status RadosEnv::RenameFile(const char* src, const char* dst) {
  const int src_on_rados = FileOnRados(src);
  const int dst_on_rados = FileOnRados(dst);
  if (TryResolveFileType(src) == kTempFile && !src_on_rados && dst_on_rados)
    return RenameLocalTmpToRados(src, dst);
  if (!src_on_rados && !dst_on_rados) return target()->RenameFile(src, dst);
  return Status::NotSupported(Slice());
}

Status RadosEnv::GetChildren(const char* dirname, std::vector<std::string>* r) {
  r->clear();
  Status s = target()->GetChildren(dirname, r);
  if (PathOnRados(dirname)) {
    std::vector<std::string> rr;
    // The previous status is over-written because it is all right for
    // local directory listing to fail since it is possible for a directory
    // to not have a local mirror.
    s = ofs_->GetChildren(dirname, &rr);
    if (s.ok()) {
      r->insert(r->end(), rr.begin(), rr.end());
    }
  }
  return s;
}

Status RadosEnv::NewWritableFile(const char* fname, WritableFile** r) {
  const size_t buf_size = wal_buf_size_;
  if (FileOnRados(fname)) {
    Status s = ofs_->NewWritableFile(fname, r);
    if (s.ok() && buf_size > 0 && TryResolveFileType(fname) == kLogFile) {
      *r = new MinMaxBufferedWritableFile(*r, buf_size, buf_size);
    }
    return s;
  } else {
    return target()->NewWritableFile(fname, r);
  }
}

Status RadosEnv::RenameLocalTmpToRados(const char* tmp, const char* dst) {
  std::string contents;
  Status s = ReadFileToString(target(), tmp, &contents);
  if (s.ok()) {
    s = ofs_->WriteStringToFile(dst, contents);  // Atomic
    if (s.ok()) {
      target()->DeleteFile(tmp);
    }
  }
  return s;
}

}  // namespace rados
}  // namespace pdlfs
