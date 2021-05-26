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

namespace pdlfs {
namespace rados {

RadosEnv::RadosEnv(const RadosEnvOptions& options)
    : options_(options), ofs_(NULL), owns_osd_(false), osd_(NULL) {
  if (!options_.info_log) {
    options_.info_log = Logger::Default();
  }
}

RadosEnv::~RadosEnv() {
  if (owns_osd_) delete osd_;
  delete ofs_;
}

namespace {
inline Status OutOfRadosMountPoint(const char* dirname) {
  return Status::AssertionFailed("Path out of rados root", dirname);
}
}  // namespace

Status RadosEnv::MountDir(const char* dirname, bool create_dir) {
  Slice path = dirname;
  if (!path.starts_with(options_.rados_root))
    return OutOfRadosMountPoint(dirname);
  if (options_.rados_root.size() > 1)
    path.remove_prefix(options_.rados_root.size());
  if (path.empty()) path = "/";
  std::string name = path.ToString();
  for (size_t i = 0; i < name.size(); i++) {
    if (name[i] == '/') {
      name[i] = '_';
    }
  }
  MountOptions options;
  options.read_only = !create_dir;
  options.create_if_missing = create_dir;
  options.error_if_exists = false;
  options.name = name;
  Status s = ofs_->MountFileSet(options, dirname);
#if VERBOSE >= 2
  Log(options_.info_log, 1, "Mounting %s using rados obj %s (create=%d): %s",
      dirname, name.c_str(), create_dir, s.ToString().c_str());
#endif
  return s;
}

Status RadosEnv::UnmountDir(const char* dirname, bool also_delete_dir) {
  Status s;
  UnmountOptions options;
  options.deletion = also_delete_dir;
  if (!also_delete_dir) s = ofs_->SynFileSet(dirname);
  if (s.ok()) {
    s = ofs_->UnmountFileSet(options, dirname);
  }
#if VERBOSE >= 2
  Log(options_.info_log, 1, "Unmounting %s (delete=%d): %s", dirname,
      also_delete_dir, s.ToString().c_str());
#endif
  return s;
}

Status RadosEnv::AttachDir(const char* dirname) {
  return MountDir(dirname, false);
}

Status RadosEnv::DetachDir(const char* dirname) {
  return UnmountDir(dirname, false);
}

Status RadosEnv::GetChildren(const char* dirname, std::vector<std::string>* r) {
  return ofs_->GetChildren(dirname, r);
}

Status RadosEnv::CreateDir(const char* dirname) {
  return MountDir(dirname, true);
}

Status RadosEnv::DeleteDir(const char* dirname) {
  return UnmountDir(dirname, true);
}

Status RadosEnv::CopyFile(const char* src, const char* dst) {
  return ofs_->CopyFile(src, dst);
}

Status RadosEnv::RenameFile(const char* src, const char* dst) {
  return ofs_->Rename(src, dst);
}

bool RadosEnv::FileExists(const char* fname) { return ofs_->FileExists(fname); }

Status RadosEnv::GetFileSize(const char* fname, uint64_t* size) {
  return ofs_->GetFileSize(fname, size);
}

Status RadosEnv::DeleteFile(const char* fname) {
  return ofs_->DeleteFile(fname);
}

Status RadosEnv::NewSequentialFile(const char* fname, SequentialFile** r) {
  return ofs_->NewSequentialFile(fname, r);
}

Status RadosEnv::NewRandomAccessFile(const char* fname, RandomAccessFile** r) {
  return ofs_->NewRandomAccessFile(fname, r);
}

Status RadosEnv::NewWritableFile(const char* fname, WritableFile** r) {
  return ofs_->NewWritableFile(fname, r);
}

namespace {
inline Status NotSupportedByRadosEnv() {
  return Status::NotSupported("Not supported by rados env");
}
}  // namespace

Status RadosEnv::LockFile(const char* f, FileLock** l) {
  return NotSupportedByRadosEnv();
}

Status RadosEnv::UnlockFile(FileLock* l) { return NotSupportedByRadosEnv(); }

void RadosEnv::Schedule(void (*f)(void*), void* a) {
  Env::Default()->Schedule(f, a);
}

void RadosEnv::StartThread(void (*f)(void*), void* a) {
  Env::Default()->StartThread(f, a);
}

Status RadosEnv::GetTestDirectory(std::string* path) {
  return NotSupportedByRadosEnv();
}

Status RadosEnv::NewLogger(const char* fname, Logger** result) {
  return NotSupportedByRadosEnv();
}

}  // namespace rados
}  // namespace pdlfs
