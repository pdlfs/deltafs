/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "osd_internal.h"

#include "pdlfs-common/env.h"
#include "pdlfs-common/osd.h"
#include "pdlfs-common/osd_env.h"

namespace pdlfs {

Osd::~Osd() {}

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

class EnvOsd : public Osd {
 public:
  EnvOsd(Env* env, const char* prefix) : env_(env) {
    prefix_ = prefix;
    env_->CreateDir(prefix_.c_str());
    prefix_.append("/obj_");
  }

  virtual ~EnvOsd() {}

  virtual Status NewSequentialObj(const char* name, SequentialFile** r) {
    const std::string fp = prefix_ + name;
    return env_->NewSequentialFile(fp.c_str(), r);
  }

  virtual Status NewRandomAccessObj(const char* name, RandomAccessFile** r) {
    const std::string fp = prefix_ + name;
    return env_->NewRandomAccessFile(fp.c_str(), r);
  }

  virtual Status NewWritableObj(const char* name, WritableFile** r) {
    const std::string fp = prefix_ + name;
    return env_->NewWritableFile(fp.c_str(), r);
  }

  virtual bool Exists(const char* name) {
    const std::string fp = prefix_ + name;
    return env_->FileExists(fp.c_str());
  }

  virtual Status Size(const char* name, uint64_t* obj_size) {
    const std::string fp = prefix_ + name;
    return env_->GetFileSize(fp.c_str(), obj_size);
  }

  virtual Status Delete(const char* name) {
    const std::string fp = prefix_ + name;
    return env_->DeleteFile(fp.c_str());
  }

  virtual Status Put(const char* name, const Slice& data) {
    const std::string fp = prefix_ + name;
    return WriteStringToFile(env_, data, fp.c_str());
  }

  virtual Status Get(const char* name, std::string* data) {
    const std::string fp = prefix_ + name;
    return ReadFileToString(env_, fp.c_str(), data);
  }

  virtual Status Copy(const char* src, const char* dst) {
    const std::string fp1 = prefix_ + src;
    const std::string fp2 = prefix_ + dst;
    return env_->CopyFile(fp1.c_str(), fp2.c_str());
  }

 private:
  // No copying allowed
  void operator=(const EnvOsd&);
  EnvOsd(const EnvOsd&);

  std::string prefix_;
  Env* env_;
};

Osd* Osd::FromEnv(const char* prefix, Env* env) {
  if (env == NULL) env = Env::Default();
  return new EnvOsd(env, prefix);
}

}  // namespace pdlfs
