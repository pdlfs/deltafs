/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/osd_env.h"

#include "rados_env.h"

namespace pdlfs {
namespace rados {

class RadosEnv : public EnvWrapper {
 public:
  RadosEnv(const RadosOptions& options, const std::string& rados_root, OSD* osd,
           Env* base_env);

  virtual ~RadosEnv() {
    delete rados_env_;
    if (owns_osd_) {
      delete osd_;
    }
  }

  virtual Status NewSequentialFile(const Slice& fname, SequentialFile** f) {
    assert(fname.starts_with(rados_root_));
    FileType type = TryResolveFileType(fname);
    if (OnRados(type)) {
      return rados_env_->NewSequentialFile(fname, f);
    } else {
      return target()->NewSequentialFile(fname, f);
    }
  }

  virtual Status NewRandomAccessFile(const Slice& fname, RandomAccessFile** f) {
    assert(fname.starts_with(rados_root_));
    FileType type = TryResolveFileType(fname);
    if (OnRados(type)) {
      return rados_env_->NewRandomAccessFile(fname, f);
    } else {
      return target()->NewRandomAccessFile(fname, f);
    }
  }

  virtual Status NewWritableFile(const Slice& fname, WritableFile** f) {
    assert(fname.starts_with(rados_root_));
    FileType type = TryResolveFileType(fname);
    if (OnRados(type)) {
      Status s = rados_env_->NewWritableFile(fname, f);
      if (s.ok() && write_buffer_ > 0) {
        if (type == kLogFile || type == kTableFile) {
          *f = new BufferedWritableFile(*f, write_buffer_);
        }
      }
      return s;
    } else {
      return target()->NewWritableFile(fname, f);
    }
  }

  virtual bool FileExists(const Slice& fname) {
    assert(fname.starts_with(rados_root_));
    FileType type = TryResolveFileType(fname);
    if (OnRados(type)) {
      return rados_env_->FileExists(fname);
    } else {
      return target()->FileExists(fname);
    }
  }

  virtual Status GetChildren(const Slice& dir, std::vector<std::string>* r) {
    assert(dir.starts_with(rados_root_));
    // It's all right for local directory listing to fail, some directories
    // only exist in rados, but not local.
    target()->GetChildren(dir, r);
    return rados_env_->GetChildren(dir, r);
  }

  virtual Status DeleteFile(const Slice& fname) {
    assert(fname.starts_with(rados_root_));
    FileType type = TryResolveFileType(fname);
    if (OnRados(type)) {
      return rados_env_->DeleteFile(fname);
    } else {
      return target()->DeleteFile(fname);
    }
  }

  Status DoCreateDir(const Slice& dir, bool soft_create) {
    assert(dir.starts_with(rados_root_));
    Slice path = dir;
    if (rados_root_.size() > 1) path.remove_prefix(rados_root_.size());
    if (path.empty()) path = "/";
    std::string name;
    AppendSliceTo(&name, path);
    for (size_t i = 0; i < name.size(); i++)
      if (name[i] == '/') name[i] = '_';

    MountOptions mount_options;
    mount_options.set_name = name;
    mount_options.read_only = soft_create;
    mount_options.create_if_missing = !soft_create;
    Status s = rados_env_->MountFileSet(mount_options, dir);
    if (s.ok()) {
      s = target()->CreateDir(dir);
    } else if (s.IsAlreadyExists()) {
      target()->CreateDir(dir);
    }
    return s;
  }

  virtual Status CreateDir(const Slice& dir) { return DoCreateDir(dir, false); }

  Status SoftCreateDir(const Slice& dir) { return DoCreateDir(dir, true); }

  Status DoDeleteDir(const Slice& dir, bool soft_delete) {
    assert(dir.starts_with(rados_root_));
    UnmountOptions unmount_options;
    unmount_options.deletion = !soft_delete;

    Status s;
    if (soft_delete) s = rados_env_->SynFileSet(dir);
    if (s.ok()) {
      s = rados_env_->UnmountFileSet(unmount_options, dir);
      if (s.ok()) {
        s = target()->DeleteDir(dir);
      } else if (s.IsNotFound()) {
        target()->DeleteDir(dir);
      }
    }
    return s;
  }

  virtual Status DeleteDir(const Slice& dir) { return DoDeleteDir(dir, false); }

  Status SoftDeleteDir(const Slice& dir) { return DoDeleteDir(dir, true); }

  virtual Status GetFileSize(const Slice& fname, uint64_t* size) {
    assert(fname.starts_with(fname));
    FileType type = TryResolveFileType(fname);
    if (OnRados(type)) {
      return rados_env_->GetFileSize(fname, size);
    } else {
      return target()->GetFileSize(fname, size);
    }
  }

  virtual Status CopyFile(const Slice& src, const Slice& dst) {
    assert(src.starts_with(rados_root_) && dst.starts_with(rados_root_));
    FileType src_type = TryResolveFileType(src);
    bool src_on_rados = OnRados(src_type);
    FileType dst_type = TryResolveFileType(dst);
    bool dst_on_rados = OnRados(dst_type);

    if (src_on_rados && dst_on_rados) {
      return rados_env_->CopyFile(src, dst);
    } else if (!src_on_rados && !dst_on_rados) {
      return target()->CopyFile(src, dst);
    } else {
      return Status::NotSupported(Slice());
    }
  }

  virtual Status RenameFile(const Slice& src, const Slice& dst) {
    assert(src.starts_with(rados_root_) && dst.starts_with(rados_root_));
    FileType src_type = TryResolveFileType(src);
    bool src_on_rados = OnRados(src_type);
    FileType dst_type = TryResolveFileType(dst);
    bool dst_on_rados = OnRados(dst_type);

    if (!src_on_rados && !dst_on_rados) {
      return target()->RenameFile(src, dst);
    } else if (src_type == kTempFile && dst_on_rados) {
      std::string contents;
      Status s = ReadFileToString(target(), src, &contents);
      if (s.ok()) {
        s = rados_env_->WriteStringToFile(dst, contents);
        if (s.ok()) {
          s = target()->DeleteFile(src);
        }
      }
      return s;
    } else {
      return Status::NotSupported(Slice());
    }
  }

 private:
  std::string rados_root_;
  OSDEnv* rados_env_;
  OSD* osd_;
  bool owns_osd_;
  int write_buffer_;

  // No copying allowed
  RadosEnv(const RadosEnv&);
  void operator=(const RadosEnv&);
};

RadosEnv::RadosEnv(const RadosOptions& options, const std::string& rados_root,
                   OSD* osd, Env* base_env)
    : EnvWrapper(base_env == NULL ? Env::Default() : base_env),
      rados_root_(rados_root),
      osd_(osd),
      write_buffer_(options.write_buffer) {
  if (osd_ == NULL) {
    osd_ = NewRadosOSD(options);
    owns_osd_ = true;
  } else {
    owns_osd_ = false;
  }
  rados_env_ = new OSDEnv(osd_);
}

Env* NewRadosEnv(const RadosOptions& options, const std::string& rados_root,
                 OSD* osd, Env* base_env) {
  assert(!rados_root.empty());
  assert(rados_root[0] == '/');
  return new RadosEnv(options, rados_root, osd, base_env);
}

Status SoftCreateDir(Env* env, const Slice& dir) {
  return reinterpret_cast<RadosEnv*>(env)->SoftCreateDir(dir);
}

Status SoftDeleteDir(Env* env, const Slice& dir) {
  return reinterpret_cast<RadosEnv*>(env)->SoftDeleteDir(dir);
}

}  // namespace rados
}  // namespace pdlfs
