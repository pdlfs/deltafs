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

#include "ofs_impl.h"

namespace pdlfs {

Ofs::Ofs(Osd* osd) { impl_ = new Impl(osd); }

Ofs::~Ofs() { delete impl_; }

MountOptions::MountOptions()
    : read_only(false),
      create_if_missing(true),
      error_if_exists(false),
      sync(false),
      paranoid_checks(false) {}

UnmountOptions::UnmountOptions() : deletion(false) {}

static bool ResolvePath(const char* path, Slice* parent, Slice* base) {
  const size_t n = strlen(path);
  if (n > 1 && path[0] == '/') {
    const char* a = path;
    const char* b = strrchr(a, '/');
    *base = Slice(b + 1, a + n - b - 1);
    if (b - a != 0) {
      *parent = Slice(a, b - a);
    } else {
      *parent = Slice("/");
    }
    return !base->empty();
  } else {
    return false;
  }
}

static inline Status PathError(const char* path) {
  return Status::InvalidArgument("Cannot resolve path", path);
}

bool Ofs::FileSetExists(const char* dirname) {
  return impl_->HasFileSet(dirname);
}

bool Ofs::FileExists(const char* fname) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return false;
  }
  return impl_->HasFile(fp);
}

Status Ofs::ReadFileToString(const char* fname, std::string* data) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return PathError(fname);
  }
  return impl_->GetFile(fp, data);
}

Status Ofs::WriteStringToFile(const char* fname, const Slice& data) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return PathError(fname);
  }
  return impl_->PutFile(fp, data);
}

Status Ofs::GetFileSize(const char* fname, uint64_t* size) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return PathError(fname);
  }
  return impl_->FileSize(fp, size);
}

Status Ofs::DeleteFile(const char* fname) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return PathError(fname);
  }
  return impl_->DeleteFile(fp);
}

Status Ofs::CopyFile(const char* src, const char* dst) {
  ResolvedPath sfp, dfp;
  if (!ResolvePath(src, &sfp.mntptr, &sfp.base)) return PathError(src);
  if (!ResolvePath(dst, &dfp.mntptr, &dfp.base)) return PathError(dst);
  return impl_->CopyFile(sfp, dfp);
}

Status Ofs::MountFileSet(const MountOptions& options, const char* dirname) {
  Slice ignored_parent;
  Slice name;
  if (!options.name.empty()) {
    name = options.name;
  } else {
    if (!ResolvePath(dirname, &ignored_parent, &name)) {
      return PathError(dirname);
    }
  }
  FileSet* const fset = new FileSet(options, name);
  Status s = impl_->LinkFileSet(dirname, fset);
  if (!s.ok()) delete fset;
  return s;
}

Status Ofs::UnmountFileSet(const UnmountOptions& options, const char* dirname) {
  return impl_->UnlinkFileSet(dirname, options.deletion);
}

Status Ofs::GetChildren(const char* dirname, std::vector<std::string>* names) {
  return impl_->ListFileSet(dirname, names);
}

Status Ofs::SynFileSet(const char* dirname) {
  return impl_->SynFileSet(dirname);
}

Status Ofs::NewSequentialFile(const char* fname, SequentialFile** r) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return PathError(fname);
  }
  return impl_->NewSequentialFile(fp, r);
}

Status Ofs::NewRandomAccessFile(const char* fname, RandomAccessFile** r) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return PathError(fname);
  }
  return impl_->NewRandomAccessFile(fp, r);
}

Status Ofs::NewWritableFile(const char* fname, WritableFile** r) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return PathError(fname);
  }
  return impl_->NewWritableFile(fp, r);
}

std::string Ofs::TEST_LookupFile(const char* fname) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return std::string();
  } else {
    return impl_->TEST_GetObjectName(fp);
  }
}

static Status DoWriteStringToFile(Osd* osd, const Slice& data, const char* name,
                                  bool should_sync) {
  WritableFile* file;
  Status s = osd->NewWritableObj(name, &file);
  if (!s.ok()) {
    return s;
  }
  s = file->Append(data);
  if (s.ok() && should_sync) {
    s = file->Sync();
  }
  if (s.ok()) {
    s = file->Close();
  }
  delete file;  // Will auto-close if we did not close above
  if (!s.ok()) {
    osd->Delete(name);
  }
  return s;
}

Status WriteStringToFile(Osd* osd, const Slice& data, const char* name) {
  return DoWriteStringToFile(osd, data, name, false);
}

Status WriteStringToFileSync(Osd* osd, const Slice& data, const char* name) {
  return DoWriteStringToFile(osd, data, name, true);
}

Status ReadFileToString(Osd* osd, const char* name, std::string* data) {
  data->clear();
  SequentialFile* file;
  Status s = osd->NewSequentialObj(name, &file);
  if (!s.ok()) {
    return s;
  }
  const size_t io_size = 8192;
  char* space = new char[io_size];
  while (true) {
    Slice fragment;
    s = file->Read(io_size, &fragment, space);
    if (!s.ok()) {
      break;
    }
    AppendSliceTo(data, fragment);
    if (fragment.empty()) {
      break;
    }
  }
  delete[] space;
  delete file;
  return s;
}

}  // namespace pdlfs
