#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/coding.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/log_reader.h"
#include "pdlfs-common/log_writer.h"
#include "pdlfs-common/map.h"
#include "pdlfs-common/osd.h"
#include "pdlfs-common/osd_env.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

class FileSet {
 public:
  enum RecordType {
    kNoOp = 0,

    // Op Committed
    kNewFile = 1,
    kDelFile = 2,

    // Undo required during recovery
    kTryNewFile = 3,
    // Redo required during recovery
    kTryDelFile = 4
  };

  explicit FileSet(const MountOptions& options, const Slice& name)
      : sync(options.sync),
        paranoid_checks(options.paranoid_checks),
        read_only(options.read_only),
        create_if_missing(options.create_if_missing),
        error_if_exists(options.error_if_exists),
        name(name.ToString()),
        xfile(NULL),
        xlog(NULL) {}

  ~FileSet() {
    delete xlog;
    if (xfile != NULL) {
      xfile->Close();
    }
    delete xfile;
  }

  Status TryNewFile(const Slice& fname) {
    if (xlog == NULL) {
      return Status::ReadOnly(Slice());
    } else {
      assert(!read_only);
      Status s = xlog->AddRecord(LogRecord(fname, kTryNewFile));
      if (s.ok() && sync) {
        s = xfile->Sync();
      }
      return s;
    }
  }

  Status NewFile(const Slice& fname) {
    if (xlog == NULL) {
      return Status::ReadOnly(Slice());
    } else {
      assert(!read_only);
      Status s = xlog->AddRecord(LogRecord(fname, kNewFile));
      if (s.ok() && sync) {
        s = xfile->Sync();
      }
      if (s.ok()) {
        files.Insert(fname);
      }
      return s;
    }
  }

  Status TryDeleteFile(const Slice& fname) {
    if (xlog == NULL) {
      return Status::ReadOnly(Slice());
    } else {
      assert(!read_only);
      Status s = xlog->AddRecord(LogRecord(fname, kTryDelFile));
      if (s.ok() && sync) {
        s = xfile->Sync();
      }
      if (s.ok()) {
        files.Erase(fname);
      }
      return s;
    }
  }

  Status DeleteFile(const Slice& fname) {
    if (xlog == NULL) {
      return Status::ReadOnly(Slice());
    } else {
      assert(!read_only);
      Status s = xlog->AddRecord(LogRecord(fname, kDelFile));
      if (s.ok() && sync) {
        s = xfile->Sync();
      }
      return s;
    }
  }

  bool sync;
  bool paranoid_checks;
  bool read_only;
  bool create_if_missing;
  bool error_if_exists;
  std::string name;
  HashSet files;

  // Tx
  WritableFile* xfile;  // The file backing the write-ahead log
  log::Writer* xlog;    // The write-ahead logger

  static std::string LogRecord(const Slice& fname, RecordType type);

 private:
  // No copying allowed
  FileSet(const FileSet&);
  void operator=(const FileSet&);
};

struct OSDEnv::ResolvedPath {
  Slice mntptr;
  Slice base;
};

class OSDEnv::InternalImpl {
 public:
  explicit InternalImpl(OSD* osd) : osd_(osd) {}

  ~InternalImpl() { assert(mtable_.Empty()); }

  bool HasFileSet(const Slice& mntptr);
  Status LinkFileSet(const Slice& mntptr, FileSet* fset);
  Status UnlinkFileSet(const Slice& mntptr, bool deletion);
  Status ListFileSet(const Slice& mntptr, std::vector<std::string>* names);
  Status SynFileSet(const Slice& mntptr);

  bool HasFile(const ResolvedPath& fp);
  Status GetFile(const ResolvedPath& fp, std::string* data);
  Status PutFile(const ResolvedPath& fp, const Slice& data);
  Status FileSize(const ResolvedPath& fp, uint64_t* size);
  Status DeleteFile(const ResolvedPath& fp);
  Status NewSequentialFile(const ResolvedPath& fp, SequentialFile** result);
  Status NewRandomAccessFile(const ResolvedPath& fp, RandomAccessFile** result);
  Status NewWritableFile(const ResolvedPath& fp, WritableFile** result);
  std::string TEST_GetObjectName(const ResolvedPath& fp);
  Status CopyFile(const ResolvedPath& src, const ResolvedPath& dst);

 private:
  OSD* osd_;
  port::Mutex mutex_;
  HashMap<FileSet> mtable_;

  static std::string InternalObjectName(const FileSet*, const Slice& name);

  // No copying allowed
  InternalImpl(const InternalImpl&);
  void operator=(const InternalImpl&);
};

inline std::string OSDEnv::InternalImpl::InternalObjectName(const FileSet* fset,
                                                            const Slice& name) {
  std::string result;
  const std::string& set_name = fset->name;
  result.reserve(set_name.size() + 1 + name.size());
  result.append(set_name);
  result.push_back('_');
  AppendSliceTo(&result, name);
  return result;
}

// Type value larger than this is invalid.
static const int kMaxRecordType = FileSet::kTryDelFile;

inline void PutOpRecord(std::string* dst, const Slice& fname,
                        FileSet::RecordType type) {
  dst->push_back(static_cast<char>(type));
  PutLengthPrefixedSlice(dst, fname);
}

inline std::string FileSet::LogRecord(const Slice& fname,
                                      FileSet::RecordType type) {
  size_t record_size = 0;
  record_size += 8 +  // timestamp
                 4 +  // num_ops
                 1 +  // op_type
                 4 +  // fname length
                 fname.size();
  std::string record;
  record.reserve(record_size);
  PutFixed64(&record, Env::Default()->NowMicros());
  PutFixed32(&record, 1);
  PutOpRecord(&record, fname, type);
  return record;
}

}  // namespace pdlfs
