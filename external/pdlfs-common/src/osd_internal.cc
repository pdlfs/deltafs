/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "osd_internal.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/log_scanner.h"
#include "pdlfs-common/mutexlock.h"

namespace pdlfs {

static Status Access(const std::string& name, OSD* osd, uint64_t* time) {
  *time = 0;
  SequentialFile* file;
  Status s = osd->NewSequentialObj(name, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::OK();
    } else {
      return s;
    }
  }

  log::Scanner sc(file);
  if (!sc.Valid()) {
    return sc.status();
  } else {
    Slice r = sc.record();
    if (r.size() < 8) {
      return Status::Corruption("Too short to be a record");
    } else {
      *time = DecodeFixed64(r.data());
      return s;
    }
  }
}

static bool Execute(Slice* input, HashSet* files, HashSet* garbage) {
  Slice fname;
  if (!input->empty()) {
    int type = (*input)[0];
    input->remove_prefix(1);
    if (type <= kMaxRecordType && GetLengthPrefixedSlice(input, &fname) &&
        !fname.empty()) {
      if (type == FileSet::kTryNewFile) {
        garbage->Insert(fname);
      } else if (type == FileSet::kTryDelFile) {
        files->Erase(fname);
        garbage->Insert(fname);
      } else if (type == FileSet::kNewFile) {
        files->Insert(fname);
        garbage->Erase(fname);
      } else if (type == FileSet::kDelFile) {
        garbage->Erase(fname);
      }
      return true;
    }
  }
  return false;
}

static Status Redo(const Slice& record, FileSet* fset, HashSet* garbage) {
  Slice input = record;
  if (input.size() < 8) {
    return Status::Corruption("Too short to be a record");
  } else {
    input.remove_prefix(8);
  }

  HashSet* files = &fset->files;
  uint32_t num_ops;
  bool error = input.size() < 4;
  if (!error) {
    num_ops = DecodeFixed32(input.data());
    input.remove_prefix(4);
    while (num_ops > 0) {
      error = !Execute(&input, files, garbage);
      if (!error) {
        num_ops--;
      } else {
        break;
      }
    }
  }
  if (error || num_ops != 0) {
    return Status::Corruption("Bad log record");
  } else {
    return Status::OK();
  }
}

static Status RecoverFileSet(OSD* osd, FileSet* fset, HashSet* garbage,
                             std::string* next_log_name) {
  Status s;
  const std::string& fset_name = fset->name;
  assert(!fset_name.empty());
  uint64_t time1, time2;
  if (s.ok()) {
    s = Access(fset_name + "_1", osd, &time1);
    if (s.ok()) {
      s = Access(fset_name + "_2", osd, &time2);
    }
  }
  if (!s.ok()) {
    return s;
  }
  if (time1 == 0 && time2 == 0) {
    return Status::NotFound(Slice());
  } else if (time1 == time2) {
    return Status::Corruption("Indistinguishable log records");
  }

  std::string log_name = fset_name;
  if (time1 > time2) {
    log_name.append("_1");
  } else {
    log_name.append("_2");
  }

  SequentialFile* file;
  s = osd->NewSequentialObj(log_name, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      s = Status::IOError(s.ToString());
    }
    return s;
  }
  log::Scanner sc(file);
  for (; sc.Valid() && s.ok(); sc.Next()) {
    s = Redo(sc.record(), fset, garbage);
  }
  if (s.ok()) {
    s = sc.status();
    if (s.ok()) {
      std::string::reverse_iterator it = log_name.rbegin();
      *it = (*it == '1') ? '2' : '1';
      next_log_name->swap(log_name);
    }
  }
  return s;
}

static void MakeSnapshot(std::string* result, FileSet* fset, HashSet* garbage) {
  result->resize(8 + 4);

  struct Visitor : public HashSet::Visitor {
    int* num_ops;
    std::string* scratch;
    FileSet::RecordType type;
    virtual void visit(const Slice& fname) {
      PutOpRecord(scratch, fname, type);
      *num_ops = *num_ops + 1;
    }
  };
  int num_ops = 0;
  Visitor v;
  v.num_ops = &num_ops;
  v.scratch = result;
  v.type = FileSet::kNewFile;
  HashSet* files = &fset->files;
  files->VisitAll(&v);
  v.type = FileSet::kTryDelFile;
  garbage->VisitAll(&v);

  uint64_t time = Env::Default()->NowMicros();
  EncodeFixed64(&(*result)[0], time);
  EncodeFixed32(&(*result)[8], num_ops);
}

static Status OpenFileSetForWriting(const std::string& log_name, OSD* osd,
                                    FileSet* fset, HashSet* garbage) {
  Status s;
  WritableFile* file;
  s = osd->NewWritableObj(log_name, &file);
  if (!s.ok()) {
    return s;
  }

  log::Writer* log = new log::Writer(file);
  std::string record;
  MakeSnapshot(&record, fset, garbage);
  s = log->AddRecord(record);
  if (!s.ok()) {
    delete log;
    file->Close();
    delete file;
    osd->Delete(log_name);
    return s;
  }

  // Perform a garbage collection pass. If things go well,
  // all garbage can be purged here. Otherwise, we will re-attempt
  // another pass the next time the set is loaded.
  struct Visitor : public HashSet::Visitor {
    OSD* osd;
    FileSet* fset;
    virtual void visit(const Slice& fname) {
      Status s = osd->Delete(fname);
      if (s.ok()) {
        fset->DeleteFile(fname);
      } else if (s.IsNotFound()) {
        fset->DeleteFile(fname);
      }
    }
  };
  fset->xfile = file;
  fset->xlog = log;
  Visitor v;
  v.osd = osd;
  v.fset = fset;
  garbage->VisitAll(&v);
  return s;
}

bool OSDEnv::Impl::HasFileSet(const Slice& mntptr) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(mntptr);
  if (fset == NULL) {
    return false;
  } else {
    return true;
  }
}

bool OSDEnv::Impl::HasFile(const ResolvedPath& fp) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) {
    return false;
  } else {
    std::string internal_name = InternalObjectName(fset, fp.base);
    if (!fset->files.Contains(internal_name)) {
      return false;
    } else {
      return true;
    }
  }
}

Status OSDEnv::Impl::SynFileSet(const Slice& mntptr) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(mntptr);
  if (fset == NULL) {
    return Status::NotFound(Slice());
  } else {
    if (fset->xfile != NULL) {
      return fset->xfile->Sync();
    } else {
      return Status::OK();
    }
  }
}

Status OSDEnv::Impl::ListFileSet(const Slice& mntptr,
                                 std::vector<std::string>* names) {
  struct Visitor : public HashSet::Visitor {
    size_t prefix;
    std::vector<std::string>* names;
    virtual void visit(const Slice& fname) {
      names->push_back(fname.substr(prefix));
    }
  };

  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(mntptr);
  if (fset == NULL) {
    return Status::NotFound(Slice());
  } else {
    const size_t prefix = fset->name.size() + 1;
    Visitor v;
    v.prefix = prefix;
    v.names = names;
    fset->files.VisitAll(&v);
    return Status::OK();
  }
}

Status OSDEnv::Impl::LinkFileSet(const Slice& mntptr, FileSet* fset) {
  MutexLock l(&mutex_);
  if (mtable_.Contains(mntptr)) {
    return Status::AlreadyExists(Slice());
  } else {
    // Try recovering from previous logs and determines the next log name.
    HashSet garbage;
    std::string next_log_name;
    Status s = RecoverFileSet(osd_, fset, &garbage, &next_log_name);
    if (s.ok()) {
      if (fset->error_if_exists) {
        return Status::AlreadyExists(Slice());
      }
    } else if (s.IsNotFound()) {
      if (fset->read_only) {
        return s;
      } else if (!fset->create_if_missing) {
        return s;
      }
      next_log_name = fset->name + "_1";
      // To be created
      s = Status::OK();
    }
    if (s.ok() && !fset->read_only) {
      s = OpenFileSetForWriting(next_log_name, osd_, fset, &garbage);
    }
    if (s.ok()) {
      mtable_.Insert(mntptr, fset);
    }
    return s;
  }
}

Status OSDEnv::Impl::UnlinkFileSet(const Slice& mntptr, bool deletion) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(mntptr);
  if (fset == NULL) {
    return Status::NotFound(Slice());
  } else {
    if (deletion) {
      if (!fset->files.Empty()) {
        return Status::DirNotEmpty(Slice());
      }
    }
    std::string fset_name = fset->name;
    mtable_.Erase(mntptr);
    delete fset;
    if (deletion) {
      Status s1 = osd_->Delete(Slice(fset_name + "_1"));
      if (s1.IsNotFound()) {
        s1 = Status::OK();
      }
      Status s2 = osd_->Delete(Slice(fset_name + "_2"));
      if (s2.IsNotFound()) {
        s2 = Status::OK();
      }
      if (!s1.ok()) {
        return s1;
      } else {
        return s2;
      }
    } else {
      return Status::OK();
    }
  }
}

std::string OSDEnv::Impl::TEST_GetObjectName(const ResolvedPath& fp) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) {
    return std::string();
  } else {
    return InternalObjectName(fset, fp.base);
  }
}

Status OSDEnv::Impl::GetFile(const ResolvedPath& fp, std::string* data) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) {
    return Status::NotFound(Slice());
  } else {
    std::string internal_name = InternalObjectName(fset, fp.base);
    if (!fset->files.Contains(internal_name)) {
      return Status::NotFound(Slice());
    } else {
      return osd_->Get(internal_name, data);
    }
  }
}

Status OSDEnv::Impl::PutFile(const ResolvedPath& fp, const Slice& data) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) {
    return Status::NotFound(Slice());
  } else {
    std::string internal_name = InternalObjectName(fset, fp.base);
    Status s = fset->TryNewFile(internal_name);
    if (s.ok()) {
      s = osd_->Put(internal_name, data);
      if (s.ok()) {
        s = fset->NewFile(internal_name);
        if (!s.ok()) {
          osd_->Delete(internal_name);
        }
      }
    }
    return s;
  }
}

Status OSDEnv::Impl::FileSize(const ResolvedPath& fp, uint64_t* result) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) {
    return Status::NotFound(Slice());
  } else {
    std::string internal_name = InternalObjectName(fset, fp.base);
    if (!fset->files.Contains(internal_name)) {
      return Status::NotFound(Slice());
    } else {
      return osd_->Size(internal_name, result);
    }
  }
}

Status OSDEnv::Impl::NewSequentialFile(const ResolvedPath& fp,
                                       SequentialFile** result) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) {
    return Status::NotFound(Slice());
  } else {
    std::string internal_name = InternalObjectName(fset, fp.base);
    if (!fset->files.Contains(internal_name)) {
      return Status::NotFound(Slice());
    } else {
      return osd_->NewSequentialObj(internal_name, result);
    }
  }
}

Status OSDEnv::Impl::NewRandomAccessFile(const ResolvedPath& fp,
                                         RandomAccessFile** result) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) {
    return Status::NotFound(Slice());
  } else {
    std::string internal_name = InternalObjectName(fset, fp.base);
    if (!fset->files.Contains(internal_name)) {
      return Status::NotFound(Slice());
    } else {
      return osd_->NewRandomAccessObj(internal_name, result);
    }
  }
}

Status OSDEnv::Impl::DeleteFile(const ResolvedPath& fp) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) {
    return Status::NotFound(Slice());
  } else {
    std::string internal_name = InternalObjectName(fset, fp.base);
    if (!fset->files.Contains(internal_name)) {
      return Status::NotFound(Slice());
    } else {
      Status s = fset->TryDeleteFile(internal_name);
      if (s.ok()) {
        // OK if we fail in the following steps as we will redo
        // this delete operation the next time the file set is reloaded.
        s = osd_->Delete(internal_name);
        if (s.ok()) {
          fset->DeleteFile(internal_name);
        } else {
          s = Status::OK();
        }
      }
      return s;
    }
  }
}

Status OSDEnv::Impl::NewWritableFile(const ResolvedPath& fp,
                                     WritableFile** result) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) {
    return Status::NotFound(Slice());
  } else {
    std::string internal_name = InternalObjectName(fset, fp.base);
    Status s = fset->TryNewFile(internal_name);
    if (s.ok()) {
      s = osd_->NewWritableObj(internal_name, result);
      if (s.ok()) {
        s = fset->NewFile(internal_name);
        if (!s.ok()) {
          osd_->Delete(internal_name);
          WritableFile* f = *result;
          *result = NULL;
          f->Close();
          delete f;
        }
      }
    }
    return s;
  }
}

Status OSDEnv::Impl::CopyFile(const ResolvedPath& src,
                              const ResolvedPath& dst) {
  MutexLock l(&mutex_);
  FileSet* src_fset = mtable_.Lookup(src.mntptr);
  if (src_fset == NULL) {
    return Status::NotFound(Slice());
  }
  FileSet* dst_fset = mtable_.Lookup(dst.mntptr);
  if (dst_fset == NULL) {
    return Status::NotFound(Slice());
  }
  std::string src_name = InternalObjectName(src_fset, src.base);
  if (!src_fset->files.Contains(src_name)) {
    return Status::NotFound(Slice());
  }

  std::string dst_name = InternalObjectName(dst_fset, dst.base);
  Status s = dst_fset->TryNewFile(dst_name);
  if (s.ok()) {
    s = osd_->Copy(src_name, dst_name);
    if (s.ok()) {
      s = dst_fset->NewFile(dst_name);
      if (!s.ok()) {
        osd_->Delete(dst_name);
      }
    }
  }
  return s;
}

}  // namespace pdlfs
