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

#include "pdlfs-common/log_scanner.h"
#include "pdlfs-common/mutexlock.h"

namespace pdlfs {

std::string Ofs::Impl::OfsName(const FileSet* fset, const Slice& name) {
  Slice parent = fset->name;
  size_t n = parent.size() + name.size() + 1;
  std::string result;
  result.reserve(n);
  result.append(parent.data(), parent.size());
  result.push_back('_');
  result.append(name.data(), name.size());
  return result;
}

static Status Access(const std::string& name, Osd* osd, uint64_t* time) {
  *time = 0;
  SequentialFile* file;
  Status s = osd->NewSequentialObj(name.c_str(), &file);
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
  if (input->empty()) {
    return false;
  }
  unsigned char type = static_cast<unsigned char>((*input)[0]);
  input->remove_prefix(1);
  Slice fname;
  if (!GetLengthPrefixedSlice(input, &fname)) {
    return false;
  }
  if (fname.empty()) {
    return false;
  }
  switch (type) {
    case FileSet::kTryNewFile:
      garbage->Insert(fname);
      return true;
    case FileSet::kTryDelFile:
      files->Erase(fname);
      garbage->Insert(fname);
      return true;
    case FileSet::kNewFile:
      files->Insert(fname);
      garbage->Erase(fname);
      return true;
    case FileSet::kDelFile:
      garbage->Erase(fname);
      return true;
    case FileSet::kNoOp:
      return true;
    default:
      return false;
  }
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

static Status RecoverFileSet(Osd* osd, FileSet* fset, HashSet* garbage,
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
  s = osd->NewSequentialObj(log_name.c_str(), &file);
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
      PutOp(scratch, fname, type);
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

static Status OpenFileSetForWriting(const std::string& log_name, Osd* osd,
                                    FileSet* fset, HashSet* garbage) {
  Status s;
  WritableFile* file;
  s = osd->NewWritableObj(log_name.c_str(), &file);
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
    osd->Delete(log_name.c_str());
    return s;
  }

  // Perform a garbage collection pass. If things go well,
  // all garbage can be purged here. Otherwise, we will re-attempt
  // another pass the next time the set is loaded.
  struct Visitor : public HashSet::Visitor {
    Osd* osd;
    FileSet* fset;
    virtual void visit(const Slice& key) {
      const std::string fname = key.ToString();
      Status s = osd->Delete(fname.c_str());
      if (s.ok()) {
        fset->DeleteFile(fname);
      } else if (s.IsNotFound()) {
        fset->DeleteFile(fname);
      } else {
        // Future work
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

bool Ofs::Impl::HasFileSet(const Slice& mntptr) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(mntptr);
  if (fset == NULL) {
    return false;
  } else {
    return true;
  }
}

bool Ofs::Impl::HasFile(const ResolvedPath& fp) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) {
    return false;
  } else {
    std::string internal_name = OfsName(fset, fp.base);
    if (!fset->files.Contains(internal_name)) {
      return false;
    } else {
      return true;
    }
  }
}

Status Ofs::Impl::SynFileSet(const Slice& mntptr) {
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

Status Ofs::Impl::ListFileSet(const Slice& mntptr,
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

Status Ofs::Impl::LinkFileSet(const Slice& mntptr, FileSet* fset) {
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

Status Ofs::Impl::UnlinkFileSet(const Slice& mntptr, bool deletion) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(mntptr);
  if (fset == NULL) {
    return Status::NotFound(Slice());
  } else {
    if (deletion) {
      if (!fset->files.Empty()) {
        return Status::DirNotEmpty(Slice());
      }
    }
    std::string parent = fset->name;
    mtable_.Erase(mntptr);
    delete fset;
    if (deletion) {
      std::string obj1 = parent + "_1";
      Status s1 = osd_->Delete(obj1.c_str());
      if (s1.IsNotFound()) {
        s1 = Status::OK();
      }
      std::string obj2 = parent + "_2";
      Status s2 = osd_->Delete(obj2.c_str());
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

std::string Ofs::Impl::TEST_GetObjectName(const ResolvedPath& fp) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (fset != NULL) {
    return OfsName(fset, fp.base);
  } else {
    return std::string();
  }
}

Status Ofs::Impl::PutFile(const ResolvedPath& fp, const Slice& data) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) {
    return Status::NotFound(Slice());
  } else {
    const std::string name = OfsName(fset, fp.base);
    Status s = fset->TryNewFile(name);
    if (s.ok()) {
      s = osd_->Put(name.c_str(), data);
      if (s.ok()) {
        s = fset->NewFile(name);
        if (!s.ok()) {
          osd_->Delete(name.c_str());
        }
      }
    }
    return s;
  }
}

Status Ofs::Impl::DeleteFile(const OfsPath& fp) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) {
    return Status::NotFound(Slice());
  } else {
    const std::string name = OfsName(fset, fp.base);
    if (!fset->files.Contains(name)) {
      return Status::NotFound(Slice());
    } else {
      Status s = fset->TryDeleteFile(name);
      if (s.ok()) {
        // OK if we fail in the following steps as we will redo
        // this delete operation the next time the file set is reloaded.
        s = osd_->Delete(name.c_str());
        if (s.ok()) {
          fset->DeleteFile(name);
        } else {
          s = Status::OK();
        }
      }
      return s;
    }
  }
}

Status Ofs::Impl::NewWritableFile(const OfsPath& fp, WritableFile** r) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) {
    return Status::NotFound(Slice());
  } else {
    const std::string name = OfsName(fset, fp.base);
    Status s = fset->TryNewFile(name);
    if (s.ok()) {
      s = osd_->NewWritableObj(name.c_str(), r);
      if (s.ok()) {
        s = fset->NewFile(name);
        if (!s.ok()) {
          osd_->Delete(name.c_str());
          WritableFile* f = *r;
          *r = NULL;
          f->Close();
          delete f;
        }
      }
    }
    return s;
  }
}

Status Ofs::Impl::GetFile(const OfsPath& fp, std::string* data) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) return Status::NotFound(Slice());
  const std::string name = OfsName(fset, fp.base);
  if (fset->files.Contains(name)) return osd_->Get(name.c_str(), data);
  return Status::NotFound(Slice());
}

Status Ofs::Impl::FileSize(const OfsPath& fp, uint64_t* result) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) return Status::NotFound(Slice());
  const std::string name = OfsName(fset, fp.base);
  if (fset->files.Contains(name)) return osd_->Size(name.c_str(), result);
  return Status::NotFound(Slice());
}

Status Ofs::Impl::NewSequentialFile(const OfsPath& fp, SequentialFile** r) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) return Status::NotFound(Slice());
  const std::string name = OfsName(fset, fp.base);
  if (!fset->files.Contains(name)) return Status::NotFound(Slice());
  return osd_->NewSequentialObj(name.c_str(), r);
}

Status Ofs::Impl::NewRandomAccessFile(const OfsPath& fp, RandomAccessFile** r) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) return Status::NotFound(Slice());
  const std::string name = OfsName(fset, fp.base);
  if (!fset->files.Contains(name)) return Status::NotFound(Slice());
  return osd_->NewRandomAccessObj(name.c_str(), r);
}

Status Ofs::Impl::CopyFile(const OfsPath& sp, const OfsPath& dp) {
  MutexLock l(&mutex_);
  FileSet* const sset = mtable_.Lookup(sp.mntptr);
  if (sset == NULL) return Status::NotFound(Slice());
  FileSet* const dset = mtable_.Lookup(dp.mntptr);
  if (dset == NULL) return Status::NotFound(Slice());

  const std::string src = OfsName(sset, sp.base);
  if (!sset->files.Contains(src)) return Status::NotFound(Slice());
  const std::string dst = OfsName(dset, dp.base);

  Status s = dset->TryNewFile(dst);
  if (s.ok()) {
    s = osd_->Copy(src.c_str(), dst.c_str());
    if (s.ok()) {
      s = dset->NewFile(dst);
      if (!s.ok()) {
        osd_->Delete(dst.c_str());
      }
    }
  }
  return s;
}

}  // namespace pdlfs
