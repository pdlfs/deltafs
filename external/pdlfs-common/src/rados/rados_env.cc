/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
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
  delete osd_env_;
  if (owns_osd_) {
    delete osd_;
  }
}

Status RadosEnv::MountDir(const Slice& dirname, bool create_dir) {
  Status s;
  assert(dirname.starts_with(rados_root_));
  Slice path = dirname;
  if (rados_root_.size() > 1) path.remove_prefix(rados_root_.size());
  if (path.empty()) path = "/";
  std::string name = path.ToString();
  for (size_t i = 0; i < name.size(); i++)
    if (name[i] == '/') name[i] = '_';

  MountOptions options;
  options.set_name = name;
  options.read_only = !create_dir;
  options.create_if_missing = create_dir;
  options.error_if_exists = false;
  s = osd_env_->MountFileSet(options, dirname);
  if (s.IsAlreadyExists()) {
    target()->CreateDir(dirname);  // Force a local mirror
  } else if (s.ok()) {
    s = target()->CreateDir(dirname);
  }

  return s;
}

Status RadosEnv::UnmountDir(const Slice& dirname, bool delete_dir) {
  Status s;
  assert(dirname.starts_with(rados_root_));
  UnmountOptions options;
  options.deletion = delete_dir;
  if (!delete_dir) {
    s = osd_env_->SynFileSet(dirname);
  }
  if (s.ok()) {
    s = osd_env_->UnmountFileSet(options, dirname);
  }
  if (s.IsNotFound()) {
    target()->DeleteDir(dirname);
  } else if (s.ok()) {
    s = target()->DeleteDir(dirname);
  }
  return s;
}

bool RadosEnv::FileOnRados(const Slice& fname) {
  if (fname.starts_with(rados_root_)) {
    FileType type = TryResolveFileType(fname);
    return OnRados(type);
  } else {
    return false;
  }
}

bool RadosEnv::FileExists(const Slice& fname) {
  if (FileOnRados(fname)) {
    return osd_env_->FileExists(fname);
  } else {
    return target()->FileExists(fname);
  }
}

Status RadosEnv::GetFileSize(const Slice& fname, uint64_t* size) {
  if (FileOnRados(fname)) {
    return osd_env_->GetFileSize(fname, size);
  } else {
    return target()->GetFileSize(fname, size);
  }
}

Status RadosEnv::DeleteFile(const Slice& fname) {
  if (FileOnRados(fname)) {
    return osd_env_->DeleteFile(fname);
  } else {
    return target()->DeleteFile(fname);
  }
}

Status RadosEnv::NewSequentialFile(const Slice& fname, SequentialFile** r) {
  if (FileOnRados(fname)) {
    return osd_env_->NewSequentialFile(fname, r);
  } else {
    return target()->NewSequentialFile(fname, r);
  }
}

Status RadosEnv::NewRandomAccessFile(const Slice& fname, RandomAccessFile** r) {
  if (FileOnRados(fname)) {
    return osd_env_->NewRandomAccessFile(fname, r);
  } else {
    return target()->NewRandomAccessFile(fname, r);
  }
}

static inline bool IsLogFile(const Slice& fname) {
  return TryResolveFileType(fname) == kLogFile;
}

Status RadosEnv::NewWritableFile(const Slice& fname, WritableFile** r) {
  if (FileOnRados(fname)) {
    Status s = osd_env_->NewWritableFile(fname, r);
    if (s.ok() && wal_buf_size_ > 0) {
      assert(*r != NULL);
      if (IsLogFile(fname)) {
        *r = new UnsafeBufferedWritableFile(*r, wal_buf_size_);
      }
    }
    return s;
  } else {
    return target()->NewWritableFile(fname, r);
  }
}

Status RadosEnv::CreateDir(const Slice& dirname) {
  if (dirname.starts_with(rados_root_)) {
    return MountDir(dirname, true);
  } else {
    return target()->CreateDir(dirname);
  }
}

Status RadosEnv::AttachDir(const Slice& dirname) {
  if (dirname.starts_with(rados_root_)) {
    return MountDir(dirname, false);
  } else {
    return target()->AttachDir(dirname);
  }
}

Status RadosEnv::DeleteDir(const Slice& dirname) {
  if (dirname.starts_with(rados_root_)) {
    return UnmountDir(dirname, true);
  } else {
    return target()->DeleteDir(dirname);
  }
}

Status RadosEnv::DetachDir(const Slice& dirname) {
  if (dirname.starts_with(rados_root_)) {
    return UnmountDir(dirname, false);
  } else {
    return target()->DetachDir(dirname);
  }
}

Status RadosEnv::CopyFile(const Slice& src, const Slice& dst) {
  bool src_on_rados = FileOnRados(src);
  bool dst_on_rados = FileOnRados(dst);

  Status s;
  if (((unsigned char)src_on_rados) ^ ((unsigned char)dst_on_rados)) {
    s = Status::NotSupported(Slice());
  } else if (src_on_rados) {
    s = osd_env_->CopyFile(src, dst);
  } else {
    s = target()->CopyFile(src, dst);
  }

  return s;
}

Status RadosEnv::RenameFile(const Slice& src, const Slice& dst) {
  bool src_on_rados = FileOnRados(src);
  bool dst_on_rados = FileOnRados(dst);

  Status s;
  if (TryResolveFileType(src) == kTempFile && !src_on_rados && dst_on_rados) {
    std::string contents;
    s = ReadFileToString(target(), src, &contents);
    if (s.ok()) {
      // This is an atomic operation.
      s = osd_env_->WriteStringToFile(dst, contents);
      if (s.ok()) {
        // This step may fail.
        target()->DeleteFile(src);
      }
    }
  } else if (!src_on_rados && !dst_on_rados) {
    s = target()->RenameFile(src, dst);
  } else {
    s = Status::NotSupported(Slice());
  }

  return s;
}

Status RadosEnv::GetChildren(const Slice& dirname,
                             std::vector<std::string>* r) {
  Status s = target()->GetChildren(dirname, r);
  if (dirname.starts_with(rados_root_)) {
    // The previous status is over-written because it is all right for
    // local directory listing to fail since some directories
    // may not have a local mirror.
    s = osd_env_->GetChildren(dirname, r);
  }
  return s;
}

}  // namespace rados
}  // namespace pdlfs
