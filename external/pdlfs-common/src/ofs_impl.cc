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
std::string Ofs::Impl::TEST_GetObjectName(const OfsPath& fp) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (!fset) {
    return std::string();
  } else {
    std::string r;
    char* c = fset->files.Lookup(fp.base);
    if (!c) {
      r = c;
    }
    return r;
  }
}

namespace {
inline std::string ObjName(const FileSet* fset, const Slice& base) {
  std::string rv;
  const size_t n = fset->name.size() + 1 + base.size();
  rv.reserve(n);
  rv += fset->name;
  rv.push_back('_');
  rv.append(base.data(), base.size());
  return rv;
}

Status Access(const std::string& name, Osd* osd, uint64_t* time) {
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

bool Execute(Slice* input, HashMap<char>* const files, HashSet* const garbage) {
  if (input->empty()) {
    return false;
  }
  unsigned char type = static_cast<unsigned char>((*input)[0]);
  input->remove_prefix(1);
  Slice name1;
  Slice name2;
  if (!GetLengthPrefixedSlice(input, &name1) ||
      !GetLengthPrefixedSlice(input, &name2)) {
    return false;
  }
  switch (type) {
    case FileSet::kTryCreateObj:
    case FileSet::kDelObj:
      if (name1.empty()) return false;
      garbage->Insert(name1);
      return true;
    case FileSet::kLink: {
      if (name1.empty() || name2.empty()) return false;
      char* const c = files->Insert(name1, strndup(name2.data(), name2.size()));
      if (c) {
        free(c);
      }
      garbage->Erase(name2);
      return true;
    }
    case FileSet::kUnlinkAndDel: {
      if (name1.empty() || name2.empty()) return false;
      char* const c = files->Erase(name1);
      if (c) {
        free(c);
      }
      garbage->Insert(name2);
      return true;
    }
    case FileSet::kUnlink: {
      if (name1.empty()) return false;
      char* const c = files->Erase(name1);
      if (c) {
        free(c);
      }
      return true;
    }
    case FileSet::kObjDeleted:
      if (name1.empty()) return false;
      garbage->Erase(name1);
      return true;
    case FileSet::kNoOp:
      return true;
    default:
      return false;
  }
}

Status RedoUndo(const Slice& record, FileSet* fset, HashSet* garbage) {
  Slice input = record;
  if (input.size() < 8) {
    return Status::Corruption("Too short to be a record");
  } else {
    input.remove_prefix(8);
  }

  HashMap<char>* files = &fset->files;
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

Status RecoverFileSet(Osd* osd, FileSet* fset, HashSet* garbage,
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
    s = RedoUndo(sc.record(), fset, garbage);
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

void MakeSnapshot(std::string* result, FileSet* fset, HashSet* garbage) {
  result->resize(8 + 4);
  int num_ops = 0;
  {
    struct Visitor : public FileSet::Visitor {
      std::string* scratch;
      int* num_ops;
      virtual void visit(const Slice& key, char* value) {
        if (value) {
          PutOp(scratch, FileSet::kLink, key, value);
          *num_ops = *num_ops + 1;
        }
      }
    };
    Visitor v;
    v.num_ops = &num_ops;
    v.scratch = result;
    HashMap<char>* files = &fset->files;
    files->VisitAll(&v);
  }
  {
    struct Visitor : public HashSet::Visitor {
      std::string* scratch;
      int* num_ops;
      virtual void visit(const Slice& key) {
        PutOp(scratch, FileSet::kDelObj, key);
        *num_ops = *num_ops + 1;
      }
    };
    Visitor v;
    v.num_ops = &num_ops;
    v.scratch = result;
    garbage->VisitAll(&v);
  }
  EncodeFixed64(&(*result)[0], CurrentMicros());
  EncodeFixed32(&(*result)[8], num_ops);
}

Status OpenFileSetForWriting(  ///
    const std::string& log_name, Osd* osd, FileSet* const fset,
    HashSet* const garbage) {
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
  // Perform a garbage collection pass. If things go well, all garbage can be
  // purged here. Otherwise, we will re-attempt another pass the next time the
  // set is loaded.
  struct Visitor : public HashSet::Visitor {
    Osd* osd;
    FileSet* fset;
    virtual void visit(const Slice& key) {
      const std::string objname = key.ToString();
      Status s = osd->Delete(objname.c_str());
      if (s.ok() || s.IsNotFound()) {
        fset->DeletedObject(objname);  // Mark as deleted
      } else {
        // Empty
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
}  // namespace

bool Ofs::Impl::HasFileSet(const Slice& mntptr) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(mntptr);
  if (fset == NULL) {
    return false;
  } else {
    return true;
  }
}

bool Ofs::Impl::HasFile(const OfsPath& fp) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(fp.mntptr);
  if (fset == NULL) {
    return false;
  } else {
    if (!fset->files.Contains(fp.base)) {
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
    return Status::NotFound("Dir not mounted", mntptr);
  } else {
    if (fset->xfile != NULL) {
      return fset->xfile->Sync();
    } else {
      return Status::OK();
    }
  }
}

Status Ofs::Impl::ListFileSet(  ///
    const Slice& mntptr, std::vector<std::string>* names) {
  MutexLock l(&mutex_);
  FileSet* fset = mtable_.Lookup(mntptr);
  if (fset == NULL) {
    return Status::NotFound("Dir not mounted", mntptr);
  } else {
    struct Visitor : public FileSet::Visitor {
      std::vector<std::string>* names;
      virtual void visit(const Slice& key, char* const c) {
        names->push_back(key.ToString());
      }
    };
    Visitor v;
    v.names = names;
    fset->files.VisitAll(&v);
    return Status::OK();
  }
}

Status Ofs::Impl::LinkFileSet(const Slice& mntptr, FileSet* fset) {
  MutexLock l(&mutex_);
  if (mtable_.Contains(mntptr)) {
    return Status::AlreadyExists("Dir already mounted", mntptr);
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
  if (!fset) {
    return Status::NotFound("Dir not mounted", mntptr);
  } else {
    if (deletion) {
      if (!fset->files.Empty()) {
        return Status::DirNotEmpty(mntptr);
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

// Atomically insert a named file into an underlying object store. Return OK on
// success, or a non-OK status on errors.
Status Ofs::Impl::PutFile(const OfsPath& fp, const Slice& data) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (!fset) {
    return Status::NotFound("Parent dir not mounted", fp.mntptr);
  } else {
    std::string objname;
    char* const c = fset->files.Lookup(fp.base);
    if (!c) {
      objname = ObjName(fset, fp.base);
    } else {
      objname = c;
    }
    Status s = fset->TryCreateObject(objname);
    if (s.ok()) {
      s = osd_->Put(objname.c_str(), data);
      if (s.ok()) {
        s = fset->Link(fp.base, objname);
        if (!s.ok()) {
          Log(options_.info_log, 0,
              "Cannot commit the mapping of a newly created file %s->%s: %s",
              fp.base.ToString().c_str(), objname.c_str(),
              s.ToString().c_str());
          if (!options_.deferred_gc) {
            osd_->Delete(objname.c_str());
          }
        }
      }
    }
    return s;
  }
}

Status Ofs::Impl::DeleteFile(const OfsPath& fp) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (!fset) {
    return Status::NotFound("Parent dir not mounted", fp.mntptr);
  } else {
    std::string objname;
    char* const c = fset->files.Lookup(fp.base);
    if (!c) {
      return Status::NotFound("No such file", fp.base);
    } else {
      objname = c;
    }
    Status s = fset->UnlinkAndDelete(fp.base, objname);
    if (s.ok()) {
      // It's okay if we fail the deletion or the logging of it as we can
      // redo these operations the next time the fileset is mounted.
      s = osd_->Delete(objname.c_str());
      if (s.ok()) {
        fset->DeletedObject(objname);
      } else {
        Log(options_.info_log, 0,
            "Fail to delete object %s: %s; will retry at the next mount",
            fp.base.ToString().c_str(), s.ToString().c_str());
        s = Status::OK();
      }
    }
    return s;
  }
}

Status Ofs::Impl::NewWritableFile(const OfsPath& fp, WritableFile** r) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (!fset) {
    return Status::NotFound("Parent dir not mounted", fp.mntptr);
  } else {
    std::string objname;
    char* const c = fset->files.Lookup(fp.base);
    if (!c) {
      objname = ObjName(fset, fp.base);
    } else {
      objname = c;
    }
    Status s = fset->TryCreateObject(objname);
    if (s.ok()) {
      s = osd_->NewWritableObj(objname.c_str(), r);
      if (s.ok()) {
        s = fset->Link(fp.base, objname);
        if (!s.ok()) {
          Log(options_.info_log, 0,
              "Cannot commit the object mapping of a newly created file "
              "%s->%s: %s",
              fp.base.ToString().c_str(), objname.c_str(),
              s.ToString().c_str());
          WritableFile* const f = *r;
          delete f;  // This will close the file
          *r = NULL;
          if (!options_.deferred_gc) {
            osd_->Delete(objname.c_str());
          }
        }
      }
    }
    return s;
  }
}

Status Ofs::Impl::GetFile(const OfsPath& fp, std::string* data) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (!fset) return Status::NotFound("Parent dir not mounted", fp.mntptr);
  char* const c = fset->files.Lookup(fp.base);
  if (!c) return Status::NotFound("No such file", fp.base);
  return osd_->Get(c, data);
}

Status Ofs::Impl::FileSize(const OfsPath& fp, uint64_t* result) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (!fset) return Status::NotFound("Parent dir not mounted", fp.mntptr);
  char* const c = fset->files.Lookup(fp.base);
  if (!c) return Status::NotFound("No such file", fp.base);
  return osd_->Size(c, result);
}

Status Ofs::Impl::NewSequentialFile(const OfsPath& fp, SequentialFile** r) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (!fset) return Status::NotFound("Parent dir not mounted", fp.mntptr);
  char* const c = fset->files.Lookup(fp.base);
  if (!c) return Status::NotFound("No such file", fp.base);
  return osd_->NewSequentialObj(c, r);
}

Status Ofs::Impl::NewRandomAccessFile(const OfsPath& fp, RandomAccessFile** r) {
  MutexLock l(&mutex_);
  FileSet* const fset = mtable_.Lookup(fp.mntptr);
  if (!fset) return Status::NotFound("Parent dir not mounted", fp.mntptr);
  char* const c = fset->files.Lookup(fp.base);
  if (!c) return Status::NotFound("No such file", fp.base);
  return osd_->NewRandomAccessObj(c, r);
}

Status Ofs::Impl::Rename(const OfsPath& sp, const OfsPath& dp) {
  MutexLock l(&mutex_);
  FileSet* const sset = mtable_.Lookup(sp.mntptr);
  if (!sset) return Status::NotFound("Parent dir not mounted", sp.mntptr);
  FileSet* const dset = mtable_.Lookup(dp.mntptr);
  if (!dset) return Status::NotFound("Parent dir not mounted", dp.mntptr);
  std::string objname;
  char* const c = sset->files.Lookup(sp.base);
  if (!c) {
    return Status::NotFound("No such file", sp.base);
  } else {
    objname = c;
  }
  if (dset->files.Contains(dp.base)) {
    return Status::AlreadyExists("File already exists", dp.base);
  }
  Status s = dset->Link(dp.base, objname);
  if (s.ok()) {
    s = sset->Unlink(sp.base);
  }
  return s;
}

Status Ofs::Impl::CopyFile(const OfsPath& sp, const OfsPath& dp) {
  MutexLock l(&mutex_);
  FileSet* const sset = mtable_.Lookup(sp.mntptr);
  if (!sset) return Status::NotFound("Parent dir not mounted", sp.mntptr);
  FileSet* const dset = mtable_.Lookup(dp.mntptr);
  if (!dset) return Status::NotFound("Parent dir not mounted", dp.mntptr);
  std::string sname;
  char* const c = sset->files.Lookup(sp.base);
  if (!c) {
    return Status::NotFound("No such file", sp.base);
  } else {
    sname = c;
  }
  std::string dname;
  char* const d = dset->files.Lookup(dp.base);
  if (!d) {
    dname = ObjName(dset, dp.base);
  } else {
    dname = d;
  }
  Status s = dset->TryCreateObject(dname);
  if (s.ok()) {
    s = osd_->Copy(sname.c_str(), dname.c_str());
    if (s.ok()) {
      s = dset->Link(dp.base, dname);
      if (!s.ok()) {
        Log(options_.info_log, 0,
            "Cannot commit the object mapping of a newly created file "
            "%s->%s: %s",
            dp.base.ToString().c_str(), dname.c_str(), s.ToString().c_str());
        if (!options_.deferred_gc) {
          osd_->Delete(dname.c_str());
        }
      }
    }
  }
  return s;
}

}  // namespace pdlfs
