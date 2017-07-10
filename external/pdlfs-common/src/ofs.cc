/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
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

static bool ResolvePath(const Slice& path, Slice* parent, Slice* base) {
  if (path.size() > 1 && path[0] == '/') {
    const char* a = path.c_str();
    const char* b = strrchr(a, '/');
    *base = Slice(b + 1, a + path.size() - b - 1);
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

bool Ofs::FileSetExists(const Slice& dirname) {
  return impl_->HasFileSet(dirname);
}

bool Ofs::FileExists(const Slice& fname) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return false;
  } else {
    return impl_->HasFile(fp);
  }
}

Status Ofs::ReadFileToString(const Slice& fname, std::string* data) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return Status::InvalidArgument(fname, "path cannot be resolved");
  } else {
    return impl_->GetFile(fp, data);
  }
}

Status Ofs::WriteStringToFile(const Slice& fname, const Slice& data) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return Status::InvalidArgument(fname, "path cannot be resolved");
  } else {
    return impl_->PutFile(fp, data);
  }
}

Status Ofs::GetFileSize(const Slice& fname, uint64_t* size) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return Status::InvalidArgument(fname, "path cannot be resolved");
  } else {
    return impl_->FileSize(fp, size);
  }
}

Status Ofs::MountFileSet(const MountOptions& options, const Slice& dirname) {
  Slice name;
  if (!options.set_name.empty()) {
    name = options.set_name;
  } else {
    Slice parent;
    if (!ResolvePath(dirname, &parent, &name)) {
      return Status::InvalidArgument(dirname, "path cannot be resolved");
    }
  }
  FileSet* fset = new FileSet(options, name);
  Status s = impl_->LinkFileSet(dirname, fset);
  if (!s.ok()) {
    delete fset;
  }
  return s;
}

Status Ofs::UnmountFileSet(const UnmountOptions& options,
                           const Slice& dirname) {
  return impl_->UnlinkFileSet(dirname, options.deletion);
}

Status Ofs::GetChildren(const Slice& dirname, std::vector<std::string>* names) {
  return impl_->ListFileSet(dirname, names);
}

Status Ofs::SynFileSet(const Slice& dirname) {
  return impl_->SynFileSet(dirname);
}

Status Ofs::DeleteFile(const Slice& fname) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return Status::InvalidArgument(fname, "path cannot be resolved");
  } else {
    return impl_->DeleteFile(fp);
  }
}

Status Ofs::CopyFile(const Slice& src, const Slice& dst) {
  ResolvedPath sfp, dfp;
  if (!ResolvePath(src, &sfp.mntptr, &sfp.base)) {
    return Status::InvalidArgument(src, "path cannot be resolved");
  } else if (!ResolvePath(dst, &dfp.mntptr, &dfp.base)) {
    return Status::InvalidArgument(dst, "path cannot be resolved");
  } else {
    return impl_->CopyFile(sfp, dfp);
  }
}

Status Ofs::NewSequentialFile(const Slice& fname, SequentialFile** result) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return Status::InvalidArgument(fname, "path cannot be resolved");
  } else {
    return impl_->NewSequentialFile(fp, result);
  }
}

Status Ofs::NewRandomAccessFile(const Slice& fname, RandomAccessFile** result) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return Status::InvalidArgument(fname, "path cannot be resolved");
  } else {
    return impl_->NewRandomAccessFile(fp, result);
  }
}

Status Ofs::NewWritableFile(const Slice& fname, WritableFile** result) {
  ResolvedPath fp;
  if (!ResolvePath(fname, &fp.mntptr, &fp.base)) {
    return Status::InvalidArgument(fname, "path cannot be resolved");
  } else {
    return impl_->NewWritableFile(fp, result);
  }
}

std::string Ofs::TEST_LookupFile(const Slice& fname) {
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
