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
#pragma once

#include "pdlfs-common/coding.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/hashmap.h"
#include "pdlfs-common/log_reader.h"
#include "pdlfs-common/log_writer.h"
#include "pdlfs-common/ofs.h"
#include "pdlfs-common/osd.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

class FileSet {
 public:
  // Values are unsigned
  enum RecordType {
    kNoOp = 0x00,  // Paddings that should be ignored

    // Will perform garbage collection (undo) if operation is not committed
    kTryCreateObj = 0x01,
    // Will redo if operation is not committed
    kUnlinkAndDel = 0x02,
    kDelObj = 0x03,

    kUnlink = 0xf0,  // Metadata-only operation
    // Mark operation as committed, canceling undo and preventing redo
    kLink = 0xf1,
    kObjDeleted = 0xf2
  };

  FileSet(const MountOptions& options, const Slice& name, bool sync_on_close)
      : paranoid_checks(options.paranoid_checks),
        read_only(options.read_only),
        create_if_missing(options.create_if_missing),
        error_if_exists(options.error_if_exists),
        sync_on_close(sync_on_close),
        sync(options.sync),
        name(name.ToString()),
        xfile(NULL),
        xlog(NULL) {}

  ~FileSet() {
    struct Visitor : public FileSet::Visitor {
      virtual void visit(const Slice& k, char* const v) {
        if (v) {
          free(v);
        }
      }
    };
    Visitor v;
    files.VisitAll(&v);
    delete xlog;
    if (xfile != NULL) {
      if (sync_on_close) {
        xfile->Sync();
      }
      delete xfile;  // This will close the file
    }
  }

  Status TryCreateObject(const std::string& underlying_obj) {
    if (xlog == NULL) {
      return Status::ReadOnly(Slice());
    } else {
      assert(!read_only);
      Status s = xlog->AddRecord(LogRecord(kTryCreateObj, underlying_obj));
      if (s.ok() && sync) {
        s = xfile->Sync();
      }
      return s;
    }
  }

  Status Link(const Slice& lname, const std::string& underlying_obj) {
    if (xlog == NULL) {
      return Status::ReadOnly(Slice());
    } else {
      assert(!read_only);
      Status s = xlog->AddRecord(LogRecord(kLink, lname, underlying_obj));
      if (s.ok() && sync) {
        s = xfile->Sync();
      }
      if (s.ok()) {
        char* c = strdup(underlying_obj.c_str());
        c = files.Insert(lname, c);
        if (c) {
          free(c);
        }
      }
      return s;
    }
  }

  Status UnlinkAndDelete(  ///
      const Slice& lname, const std::string& underlying_obj) {
    if (xlog == NULL) {
      return Status::ReadOnly(Slice());
    } else {
      assert(!read_only);
      Status s =
          xlog->AddRecord(LogRecord(kUnlinkAndDel, lname, underlying_obj));
      if (s.ok() && sync) {
        s = xfile->Sync();
      }
      if (s.ok()) {
        char* const c = files.Erase(lname);
        if (c) {
          free(c);
        }
      }
      return s;
    }
  }

  Status Unlink(const Slice& lname) {
    if (xlog == NULL) {
      return Status::ReadOnly(Slice());
    } else {
      assert(!read_only);
      Status s = xlog->AddRecord(LogRecord(kUnlink, lname));
      if (s.ok() && sync) {
        s = xfile->Sync();
      }
      if (s.ok()) {
        char* const c = files.Erase(lname);
        if (c) {
          free(c);
        }
      }
      return s;
    }
  }

  Status DeletedObject(const std::string& underlying_obj) {
    if (xlog == NULL) {
      return Status::ReadOnly(Slice());
    } else {
      assert(!read_only);
      Status s = xlog->AddRecord(LogRecord(kObjDeleted, underlying_obj));
      if (s.ok() && sync) {
        s = xfile->Sync();
      }
      return s;
    }
  }

  // File set options
  // Constant after construction
  bool paranoid_checks;
  bool read_only;
  bool create_if_missing;
  bool error_if_exists;
  bool sync_on_close;
  bool sync;

  typedef HashMap<char>::Visitor Visitor;
  std::string name;     // Internal name of the file set
  HashMap<char> files;  // Children files

  // Atomically write a log record
  static std::string LogRecord(  ///
      RecordType type, const Slice& name1, const Slice& name2 = Slice());
  WritableFile* xfile;  // The file backing the write-ahead log
  typedef log::Writer Log;
  Log* xlog;  // Write-ahead logger

 private:
  // No copying allowed
  void operator=(const FileSet& other);
  FileSet(const FileSet&);
};

class Ofs::Impl {
 public:
  typedef ResolvedPath OfsPath;
  Impl(const OfsOptions& options, Osd* osd) : options_(options), osd_(osd) {
    if (options_.info_log == NULL) {
      options_.info_log = Logger::Default();
    }
  }

  ~Impl() {
    // All file sets should have be unmounted
    // at this point
    assert(mtable_.Empty());
  }

  bool HasFileSet(const Slice& mntptr);
  Status LinkFileSet(const Slice& mntptr, FileSet* fset);
  Status UnlinkFileSet(const Slice& mntptr, bool deletion);
  Status ListFileSet(const Slice& mntptr, std::vector<std::string>* names);
  Status SynFileSet(const Slice& mntptr);

  bool HasFile(const OfsPath& fp);
  Status GetFile(const OfsPath& fp, std::string* data);
  Status PutFile(const OfsPath& fp, const Slice& data);
  Status FileSize(const OfsPath& fp, uint64_t* size);
  Status DeleteFile(const OfsPath& fp);
  Status NewSequentialFile(const OfsPath& fp, SequentialFile** result);
  Status NewRandomAccessFile(const OfsPath& fp, RandomAccessFile** result);
  Status NewWritableFile(const OfsPath& fp, WritableFile** result);
  std::string TEST_GetObjectName(const OfsPath& fp);
  Status CopyFile(const OfsPath& sp, const OfsPath& dp);
  Status Rename(const OfsPath& sp, const OfsPath& dp);

 private:
  port::Mutex mutex_;
  HashMap<FileSet> mtable_;
  // No copying allowed
  void operator=(const Impl&);
  Impl(const Impl&);

  friend class Ofs;
  OfsOptions options_;
  Osd* osd_;
};

inline void PutOp(  ///
    std::string* dst, FileSet::RecordType type, const Slice& name1,
    const Slice& name2 = Slice()) {
  dst->push_back(static_cast<unsigned char>(type));
  PutLengthPrefixedSlice(dst, name1);
  PutLengthPrefixedSlice(dst, name2);
}

// Each record is formatted as defined below:
//   timestamp: uint64_t
//   num_ops: uint32_t
//  For each op:
//   op_type: uint8_t
//   name1_len: varint32_t
//   name1: char[n]
//   name2_len: varint32_t
//   name2: char[n]
//  Note: both name1 and name2 may be empty
inline std::string FileSet::LogRecord(  ///
    FileSet::RecordType type, const Slice& name1, const Slice& name2) {
  std::string rec;
  size_t max_record_size = 8 + 4 + 1 + 5 + 5;
  max_record_size += name1.size();
  max_record_size += name2.size();
  rec.reserve(max_record_size);
  PutFixed64(&rec, CurrentMicros());
  PutFixed32(&rec, 1);
  PutOp(&rec, type, name1, name2);
  return rec;
}

}  // namespace pdlfs
