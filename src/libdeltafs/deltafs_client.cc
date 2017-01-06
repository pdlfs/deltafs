/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "deltafs_client.h"
#include "deltafs_conf_loader.h"
#include "deltafs_plfsio.h"
#include "pdlfs-common/blkdb.h"
#include "pdlfs-common/lazy_env.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/rpc.h"
#include "pdlfs-common/strutil.h"

namespace pdlfs {

// Helper for getting status strings.
#define OP_VERBOSE_LEVEL 8
#define OP_VERY_VERBOSE_LEVEL 10
#define STATUS_STR(status) status.ToString().c_str()
#define OP_STATUS(op, path, status) \
  "%s '%s': %s", op, path.c_str(), STATUS_STR(status)
#define OP_VERBOSE(path, status) \
  Verbose(__LOG_ARGS__, OP_VERBOSE_LEVEL, OP_STATUS(__func__, path, status))

static inline Status FileAccessModeNotMatched() {
  return Status::InvalidFileDescriptor(Slice());
}

static inline Status BadDescriptor() {
  return Status::InvalidFileDescriptor(Slice());
}

class WritablePlfsDir : public Fio::Handle {
  ~WritablePlfsDir() {
    if (writer != NULL) {
      delete writer;
    }
  }

  // No copying allowed
  WritablePlfsDir& operator=(const WritablePlfsDir&);
  WritablePlfsDir(const WritablePlfsDir&);

  int refs;

 public:
  explicit WritablePlfsDir() : refs(0) {}
  plfsio::Writer* writer;
  void Ref() { refs++; }

  void Unref() {
    assert(refs > 0);
    refs--;
    if (refs == 0) {
      delete this;
    }
  }
};

static inline WritablePlfsDir* ToWritablePlfsDir(Fio::Handle* fh) {
  WritablePlfsDir* dir = static_cast<WritablePlfsDir*>(fh);
  assert(dir != NULL);
  return dir;
}

Client::Client(size_t max_open_files) {
  mask_.Release_Store(reinterpret_cast<void*>(S_IWGRP | S_IWOTH));
  has_curroot_set_.Release_Store(NULL);
  has_curdir_set_.Release_Store(NULL);
  dummy_.prev = &dummy_;
  dummy_.next = &dummy_;
  max_open_fds_ = max_open_files;
  fds_ = new File*[max_open_fds_]();
  num_open_fds_ = 0;
  fd_slot_ = 0;
}

Client::~Client() {
  delete[] fds_;
  delete mdscli_;
  delete mdsfty_;
  delete fio_;
}

// Consume a descriptor slot to point to the specified file entry.
// REQUIRES: less than "max_open_files_" files have been opened.
// REQUIRES: mutex_ has been locked.
size_t Client::Alloc(File* f) {
  mutex_.AssertHeld();
  assert(num_open_fds_ < max_open_fds_);
  while (fds_[fd_slot_] != NULL) {
    fd_slot_ = (1 + fd_slot_) % max_open_fds_;
  }
  fds_[fd_slot_] = f;
  num_open_fds_++;
  return fd_slot_;
}

// Deallocate a given file descriptor slot.
// The associated file entry is not un-referenced.
// REQUIRES: mutex_ has been locked.
Client::File* Client::Free(size_t index) {
  mutex_.AssertHeld();
  File* f = fds_[index];
  assert(f != NULL);
  fds_[index] = NULL;
  assert(num_open_fds_ > 0);
  num_open_fds_--;
  return f;
}

// REQUIRES: less than "max_open_files_" files have been opened.
// REQUIRES: mutex_ has been locked.
size_t Client::Open(const Slice& encoding, int flags, Fio::Handle* fh) {
  assert(encoding.size() != 0);
  File* file = static_cast<File*>(malloc(sizeof(File) + encoding.size() - 1));
  memcpy(file->encoding_data, encoding.data(), encoding.size());
  file->encoding_length = encoding.size();
  file->next = &dummy_;
  file->prev = dummy_.prev;
  file->prev->next = file;
  file->next->prev = file;
  file->seq_write = 0;
  file->seq_flush = 0;
  file->flags = flags;
  file->refs = 1;
  file->fh = fh;
  return Alloc(file);
}

// REQUIRES: mutex_ has been locked.
void Client::Unref(File* f, const Fentry& fentry) {
  mutex_.AssertHeld();
  assert(f->refs > 0);
  f->refs--;
  if (f->refs == 0) {
    f->next->prev = f->prev;
    f->prev->next = f->next;
    if (!DELTAFS_DIR_IS_PLFS_STYLE(fentry.file_mode())) {
      if (S_ISREG(fentry.file_mode())) {
        fio_->Close(fentry, f->fh);
      } else {
        assert(f->fh == NULL);
      }
    } else if (IsWriteOk(f)) {
      ToWritablePlfsDir(f->fh)->Unref();
    } else {
      assert(false);
      // TODO
    }
    free(f);
  } else {
    // Do nothing
  }
}

// Sanitize path by removing all tailing slashes.
// Return OK if the resulting path is non-empty, or a non-OK status otherwise.
// NOTE: *path is used both as an input and output parameter.
static Status SanitizePath(Slice* path, std::string* scratch) {
  if (path->size() > 1 && (*path)[path->size() - 1] == '/') {
    scratch->assign(path->c_str(), path->size() - 1);
    while (scratch->size() > 1 && (*scratch)[scratch->size() - 1] == '/') {
      scratch->resize(scratch->size() - 1);
    }
    *path = *scratch;
  }
  if (path->empty()) {
    return Status::InvalidArgument("path is empty");
  } else {
    return Status::OK();
  }
}

// Sanitize the given path. After that:
// If path is relative, the current working directory is used to obtain the
// final path. If path is absolute, the current root directory is used.
// NOTE: *path is used both as an input and output parameter.
Status Client::ExpandPath(Slice* path, std::string* scratch) {
  Status s = SanitizePath(path, scratch);
  if (!s.ok()) {
    // Path is not valid
  } else if ((*path)[0] == '/') {  // Absolute path
    if (has_curroot_set_.Acquire_Load()) {
      MutexLock l(&mutex_);
      *scratch = curroot_ + path->ToString();
      *path = *scratch;
    }
  } else {  // Relative path
    if (has_curdir_set_.Acquire_Load()) {
      MutexLock l(&mutex_);
      *scratch = curdir_ + "/" + path->ToString();
      *path = *scratch;
    } else {
      std::string r = "/";
      *scratch = r + path->ToString();
      *path = *scratch;
    }
  }

  return s;
}

// Apply the current mask set by umask.
mode_t Client::MaskMode(mode_t mode) {
  mode_t mask = reinterpret_cast<intptr_t>(mask_.Acquire_Load());
  return (~mask) & mode;
}

// Open a file or a directory below a specific directory.
// The input path is always considered relative.
// XXX: BUG - plfs files currently can only be opened by Fopenat.
Status Client::Fopenat(int fd, const char* path, int flags, mode_t mode,
                       FileInfo* info) {
  Status s;
  Fentry fentry;
  MutexLock ml(&mutex_);
  File* file = FetchFile(fd, &fentry);
  if (file == NULL) {
    s = BadDescriptor();
  } else if (num_open_fds_ < max_open_fds_) {
    FileAndEntry at;
    at.ent = &fentry;
    at.file = file;
    std::string p = "/";
    p += path;
    s = InternalOpen(p, flags, mode, &at, info);
  } else {
    s = Status::TooManyOpens(Slice());
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  char tmp[20];
  snprintf(tmp, sizeof(tmp), "#%d + ", fd);
  std::string p = tmp;
  p += path;
  OP_VERBOSE(p, s);
#endif

  return s;
}

// Open a specific file or directory, including plfs directories.
Status Client::Fopen(const char* path, int flags, mode_t mode, FileInfo* info) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);

  if (s.ok()) {
    MutexLock ml(&mutex_);
    if (num_open_fds_ < max_open_fds_) {
      s = InternalOpen(p, flags, mode, NULL, info);
    } else {
      s = Status::TooManyOpens(Slice());
    }
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

// Open a file for I/O operations. Return OK on success.
// If O_CREAT is specified and the file does not exist, it will be created. If
// both O_CREAT and O_EXCL are specified and the file exists, error is returned.
// If O_TRUNC is specified and the file already exists and is a regular
// file and the open mode allows for writing, it will be truncated to length 0.
// In addition to opening the file, a file descriptor is allocated to
// hold the file. The current length of the file is also returned along
// with the file descriptor.
// Only a fixed amount of file can be opened simultaneously.
// A process may open a same file multiple times and obtain multiple file
// descriptors that point to distinct open file entries.
// If O_DIRECTORY is specified and the open file is not a directory,
// error is returned. If O_DIRECTORY is not specified and the open file is a
// directory, the directory shall be opened as if O_DIRECTORY is specified.
// If the opened file is a regular directory, O_RDONLY must be specified.
// If the opened file is a pdlfs directory, either O_RDONLY or O_WDONLY must
// be specified.
// REQUIRES: mutex_ has been locked.
Status Client::InternalOpen(const Slice& path, int flags, mode_t mode,
                            FileAndEntry* at, FileInfo* info) {
  Status s;
  mutex_.Unlock();
  Fentry fentry;
  mode_t my_file_mode = 0;
  bool is_new = false;  // True when file is newly created
  if ((flags & O_CREAT) != O_CREAT) {
    s = mdscli_->Fstat(path, &fentry, at != NULL ? at->ent : NULL);
  } else {
    s = mdscli_->Fcreat(path, MaskMode(mode), &fentry,
                        (flags & O_EXCL) == O_EXCL, &is_new,
                        at != NULL ? at->ent : NULL);
  }

  // Verify file modes
  if (s.ok()) {
    my_file_mode = fentry.file_mode();
    if (S_ISDIR(my_file_mode)) {
      if (!DELTAFS_DIR_IS_PLFS_STYLE(my_file_mode) &&
          (flags & O_ACCMODE) != O_RDONLY) {
        s = Status::FileExpected(Slice());
      } else if (DELTAFS_DIR_IS_PLFS_STYLE(my_file_mode) &&
                 (flags & O_ACCMODE) == O_RDWR) {
        s = Status::InvalidArgument(Slice());
      }
    } else {
      if ((flags & O_DIRECTORY) == O_DIRECTORY) {
        s = Status::DirExpected(Slice());
      }
    }
  }

  // Verify I/O permissions
  if (s.ok()) {
    if (S_ISREG(my_file_mode) || DELTAFS_DIR_IS_PLFS_STYLE(my_file_mode)) {
      if ((flags & O_ACCMODE) != O_RDONLY) {
        if (!mdscli_->IsWriteOk(&fentry.stat)) {
          s = Status::AccessDenied(Slice());
        }
      }

      if (s.ok() && (flags & O_ACCMODE) != O_WRONLY) {
        if (!mdscli_->IsReadOk(&fentry.stat)) {
          s = Status::AccessDenied(Slice());
        }
      }
    }
  }

#if VERBOSE >= OP_VERY_VERBOSE_LEVEL
  if (s.ok()) {
    Verbose(__LOG_ARGS__, OP_VERY_VERBOSE_LEVEL, "%s (at pid=%s) %s -> %s",
            __func__, at != NULL ? at->ent->DebugString().c_str()
                                 : DirId(0, 0, 0).DebugString().c_str(),
            path.c_str(), fentry.DebugString().c_str());
  } else {
    Verbose(__LOG_ARGS__, OP_VERY_VERBOSE_LEVEL, "%s (at pid=%s) %s: %s",
            __func__, at != NULL ? at->ent->DebugString().c_str()
                                 : DirId(0, 0, 0).DebugString().c_str(),
            path.c_str(), STATUS_STR(s));
  }
#endif

  char tmp[DELTAFS_FENTRY_BUFSIZE];
  Slice encoding;
  uint64_t mtime = 0;
  uint64_t size = 0;

  if (s.ok()) {
    encoding = fentry.EncodeTo(tmp);  // Use a compact representation

    mtime = fentry.stat.ModifyTime();
    size = fentry.stat.FileSize();
  }

  // Link file objects
  Fio::Handle* fh = NULL;

  if (s.ok()) {
    if (!DELTAFS_DIR_IS_PLFS_STYLE(my_file_mode)) {
      if (S_ISREG(my_file_mode)) {
        const bool append_only = (flags & O_APPEND) == O_APPEND;
        if (!is_new) {
          const bool create_if_missing = true;  // Allow lazy object creation
          const bool truncate_if_exists =
              (flags & O_ACCMODE) != O_RDONLY && (flags & O_TRUNC) == O_TRUNC;
          s = fio_->Open(fentry, create_if_missing, truncate_if_exists,
                         append_only, &mtime, &size, &fh);
        } else {
          // File doesn't exist before so we explicit create it
          s = fio_->Creat(fentry, append_only, &fh);
        }
      } else {
        // Do nothing
      }
    } else {
      if (S_ISDIR(my_file_mode)) {
        WritablePlfsDir* d = new WritablePlfsDir;
        d->writer = NULL;

        fh = d;
      } else if (at != NULL) {
        if (DELTAFS_DIR_IS_PLFS_STYLE(at->ent->file_mode())) {
          assert(at->file->fh != NULL);
          fh = at->file->fh;
        }
      }

      if (fh == NULL) {
        s = Status::InvalidArgument(Slice());
      }
    }
  }

  mutex_.Lock();

  if (s.ok()) {
    if (DELTAFS_DIR_IS_PLFS_STYLE(my_file_mode)) {
      ToWritablePlfsDir(fh)->Ref();
    }
  }

  if (s.ok()) {
    if (num_open_fds_ >= max_open_fds_) {
      s = Status::TooManyOpens(Slice());
      if (fh != NULL) {
        if (!DELTAFS_DIR_IS_PLFS_STYLE(my_file_mode)) {
          fio_->Close(fentry, fh);
        } else if ((flags & O_ACCMODE) == O_WRONLY) {
          ToWritablePlfsDir(fh)->Unref();
        } else {
          // TODO
        }
      }
    } else {
      fentry.stat.SetFileSize(size);
      fentry.stat.SetModifyTime(mtime);

      info->fd = Open(encoding, flags, fh);  // fh could be NULL
      info->stat = fentry.stat;
    }
  }

  return s;
}

bool Client::IsReadOk(const File* f) {
  if ((f->flags & O_ACCMODE) == O_WRONLY) {
    return false;
  } else {
    return true;
  }
}

bool Client::IsWriteOk(const File* f) {
  if ((f->flags & O_ACCMODE) == O_RDONLY) {
    return false;
  } else {
    return true;
  }
}

// REQUIRES: mutex_ has been locked.
Client::File* Client::FetchFile(int fd, Fentry* result) {
  size_t index = fd;
  if (index < max_open_fds_) {
    File* f = fds_[index];
    if (f != NULL) {
      Slice input = f->fentry_encoding();
#ifndef NDEBUG
      bool r = result->DecodeFrom(&input);
      assert(r);
#else
      result->DecodeFrom(&input);
#endif
    }
    return f;
  } else {
    return NULL;
  }
}

Status Client::Fstat(int fd, Stat* statbuf) {
  MutexLock ml(&mutex_);
  Fentry fentry;
  File* file = FetchFile(fd, &fentry);
  if (file == NULL) {
    return BadDescriptor();
  } else {
    Status s;
    file->refs++;  // Ref
    if (!DELTAFS_DIR_IS_PLFS_STYLE(fentry.file_mode())) {
      if (S_ISREG(fentry.file_mode())) {
        mutex_.Unlock();
        uint64_t mtime = 0;
        uint64_t size = 0;
        s = fio_->Fstat(fentry, file->fh, &mtime, &size);
        if (s.ok()) {
          fentry.stat.SetModifyTime(mtime);
          fentry.stat.SetFileSize(size);
        }
        mutex_.Lock();
      }
    }
    if (s.ok()) *statbuf = fentry.stat;
    Unref(file, fentry);
    return s;
  }
}

Status Client::Pwrite(int fd, const Slice& data, uint64_t off) {
  MutexLock ml(&mutex_);
  Fentry fentry;
  File* file = FetchFile(fd, &fentry);
  if (file == NULL) {
    return BadDescriptor();
  } else if (!S_ISREG(fentry.file_mode())) {
    return FileAccessModeNotMatched();
  } else if (!IsWriteOk(file)) {
    return FileAccessModeNotMatched();
  } else {
    Status s;
    file->refs++;  // Ref
    mutex_.Unlock();
    if (DELTAFS_DIR_IS_PLFS_STYLE(fentry.file_mode())) {
      // XXX: All writes are considered an append operation
      plfsio::Writer* writer = ToWritablePlfsDir(file->fh)->writer;
      assert(writer != NULL);
      s = writer->Append(fentry.nhash, data);
    } else {
      s = fio_->Pwrite(fentry, file->fh, data, off);
    }
    mutex_.Lock();
    if (s.ok()) {
      file->seq_write++;
    }
    Unref(file, fentry);
    return s;
  }
}

Status Client::Write(int fd, const Slice& data) {
  MutexLock ml(&mutex_);
  Fentry fentry;
  File* file = FetchFile(fd, &fentry);
  if (file == NULL) {
    return BadDescriptor();
  } else if (!S_ISREG(fentry.file_mode())) {
    return FileAccessModeNotMatched();
  } else if (!IsWriteOk(file)) {
    return FileAccessModeNotMatched();
  } else {
    Status s;
    file->refs++;  // Ref
    mutex_.Unlock();
    if (DELTAFS_DIR_IS_PLFS_STYLE(fentry.file_mode())) {
      // XXX: All writes are considered an append operation
      plfsio::Writer* writer = ToWritablePlfsDir(file->fh)->writer;
      assert(writer != NULL);
      s = writer->Append(fentry.nhash, data);
    } else {
      s = fio_->Write(fentry, file->fh, data);
    }
    mutex_.Lock();
    if (s.ok()) {
      file->seq_write++;
    }
    Unref(file, fentry);
    return s;
  }
}

Status Client::Ftruncate(int fd, uint64_t len) {
  MutexLock ml(&mutex_);
  Fentry fentry;
  File* file = FetchFile(fd, &fentry);
  if (file == NULL) {
    return BadDescriptor();
  } else if (DELTAFS_DIR_IS_PLFS_STYLE(fentry.file_mode())) {
    return FileAccessModeNotMatched();
  } else if (!S_ISREG(fentry.file_mode())) {
    return FileAccessModeNotMatched();
  } else if (!IsWriteOk(file)) {
    return FileAccessModeNotMatched();
  } else {
    Status s;
    file->refs++;  // Ref
    mutex_.Unlock();
    s = fio_->Ftrunc(fentry, file->fh, len);
    mutex_.Lock();
    if (s.ok()) {
      file->seq_write++;
    }
    Unref(file, fentry);
    return s;
  }
}

// If fd refers to a plfs directory, we do a forced sync.
// If fd refers to a plfs file under a plfs directory, we ignore the request.
// If fd refers to a normal file, we sync its data and update its metadata.
// If fd refers to a normal directory, we don't yet have that logic.
Status Client::Fdatasync(int fd) {
  MutexLock ml(&mutex_);
  Fentry fentry;
  File* file = FetchFile(fd, &fentry);
  if (file == NULL) {
    return BadDescriptor();
  } else if (DELTAFS_DIR_IS_PLFS_STYLE(fentry.file_mode())) {
    Status s;
    if (S_ISDIR(fentry.file_mode())) {
      plfsio::Writer* writer = ToWritablePlfsDir(file->fh)->writer;
      assert(writer != NULL);
      s = writer->Sync();
    }
    return s;
  } else if (!S_ISREG(fentry.file_mode())) {
    return Status::NotSupported(Slice());
  } else if (IsWriteOk(file)) {
    return InternalFdatasync(file, fentry);
  } else {
    return Status::OK();
  }
}

Status Client::InternalFdatasync(File* file, const Fentry& fentry) {
  Status s;
  uint64_t mtime;
  uint64_t size;
  uint32_t seq_write = file->seq_write;
  uint32_t seq_flush = file->seq_flush;
  file->refs++;  // Ref
  mutex_.Unlock();
  s = fio_->Flush(fentry, file->fh, true /*force*/);
  if (s.ok()) {
    if (seq_flush < seq_write) {
      s = fio_->Fstat(fentry, file->fh, &mtime, &size, true /*skip_cache*/);
      if (s.ok()) {
        s = mdscli_->Ftruncate(fentry, mtime, size);
      }
    }
  }
  mutex_.Lock();
  if (s.ok()) {
    if (seq_write > file->seq_flush) {
      file->seq_flush = seq_write;
    }
  }
  Unref(file, fentry);
  return s;
}

Status Client::Pread(int fd, Slice* result, uint64_t off, uint64_t size,
                     char* scratch) {
  MutexLock ml(&mutex_);
  Fentry fentry;
  File* file = FetchFile(fd, &fentry);
  if (file == NULL) {
    return BadDescriptor();
  } else if (!S_ISREG(fentry.file_mode())) {
    return FileAccessModeNotMatched();
  } else if (!IsReadOk(file)) {
    return FileAccessModeNotMatched();
  } else {
    Status s;
    file->refs++;  // Ref
    mutex_.Unlock();
    if (!DELTAFS_DIR_IS_PLFS_STYLE(fentry.file_mode())) {
      s = fio_->Pread(fentry, file->fh, result, off, size, scratch);
    } else {
      // TODO
    }
    mutex_.Lock();
    Unref(file, fentry);
    return s;
  }
}

Status Client::Read(int fd, Slice* result, uint64_t size, char* scratch) {
  MutexLock ml(&mutex_);
  Fentry fentry;
  File* file = FetchFile(fd, &fentry);
  if (file == NULL) {
    return BadDescriptor();
  } else if (!S_ISREG(fentry.file_mode())) {
    return FileAccessModeNotMatched();
  } else if (!IsReadOk(file)) {
    return FileAccessModeNotMatched();
  } else {
    Status s;
    file->refs++;  // Ref
    mutex_.Unlock();
    if (!DELTAFS_DIR_IS_PLFS_STYLE(fentry.file_mode())) {
      s = fio_->Read(fentry, file->fh, result, size, scratch);
    } else {
      // TODO
    }
    mutex_.Lock();
    Unref(file, fentry);
    return s;
  }
}

// If fd refers to a plfs directory, we do flush epoch.
// If fd refers to a plfs file under a plfs directory, we ignore the request.
// If fd refers to a normal file, we flush its data and update its metadata.
// If fd refers to a normal directory, we don't yet have that logic.
Status Client::Flush(int fd) {
  MutexLock ml(&mutex_);
  Fentry fentry;
  File* file = FetchFile(fd, &fentry);
  if (file == NULL) {
    return BadDescriptor();
  } else if (DELTAFS_DIR_IS_PLFS_STYLE(fentry.file_mode())) {
    Status s;
    if (S_ISDIR(fentry.file_mode())) {
      plfsio::Writer* writer = ToWritablePlfsDir(file->fh)->writer;
      assert(writer != NULL);
      s = writer->MakeEpoch();
    }
    return s;
  } else if (!S_ISREG(fentry.file_mode())) {
    return Status::NotSupported(Slice());
  } else if (IsWriteOk(file)) {
    return InternalFlush(file, fentry);
  } else {
    return Status::OK();
  }
}

Status Client::InternalFlush(File* file, const Fentry& fentry) {
  Status s;
  uint64_t mtime;
  uint64_t size;
  uint32_t seq_write = file->seq_write;
  uint32_t seq_flush = file->seq_flush;
  file->refs++;  // Ref
  mutex_.Unlock();
  s = fio_->Flush(fentry, file->fh);
  if (s.ok()) {
    if (seq_flush < seq_write) {
      s = fio_->Fstat(fentry, file->fh, &mtime, &size);
      if (s.ok()) {
        s = mdscli_->Ftruncate(fentry, mtime, size);
      }
    }
  }
  mutex_.Lock();
  if (s.ok()) {
    if (seq_write > file->seq_flush) {
      file->seq_flush = seq_write;
    }
  }
  Unref(file, fentry);
  return s;
}

Status Client::Close(int fd) {
  MutexLock ml(&mutex_);
  Fentry fentry;
  File* file = FetchFile(fd, &fentry);
  if (file == NULL) {
    return BadDescriptor();
  } else {
    if (DELTAFS_DIR_IS_PLFS_STYLE(fentry.file_mode())) {
      // Ignore write and flush sequences
      plfsio::Writer* writer = ToWritablePlfsDir(file->fh)->writer;
      assert(writer != NULL);
      if (S_ISDIR(fentry.file_mode())) {
        writer->Finish();  // Ignore errors
      }
    } else {
      if (S_ISREG(fentry.file_mode())) {
        while (file->seq_flush < file->seq_write) {
          mutex_.Unlock();
          Flush(fd);  // Ignore errors
          mutex_.Lock();
        }
      }
    }

    Free(fd);  // Release fd slot

    Unref(file, fentry);
    return Status::OK();
  }
}

Status Client::Access(const char* path, int mode) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);
  if (s.ok()) {
    s = mdscli_->Access(p, mode);
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

Status Client::Accessdir(const char* path, int mode) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);
  if (s.ok()) {
    s = mdscli_->Accessdir(p, mode);
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

Status Client::Listdir(const char* path, std::vector<std::string>* names) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);
  if (s.ok()) {
    s = mdscli_->Listdir(p, names);
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

Status Client::Lstat(const char* path, Stat* statbuf) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);

  Fentry fentry;
  if (s.ok()) {
    s = mdscli_->Fstat(p, &fentry);
    if (s.ok()) {
      *statbuf = fentry.stat;
    }
  }

  if (s.ok()) {
    if (S_ISREG(statbuf->FileMode())) {
      uint64_t mtime;
      uint64_t size;
      s = fio_->Stat(fentry, &mtime, &size);
      if (s.ok()) {
        statbuf->SetModifyTime(mtime);
        statbuf->SetFileSize(size);
      }
    }
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

Status Client::Getattr(const char* path, Stat* statbuf) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);

  Fentry ent;
  if (s.ok()) {
    s = mdscli_->Fstat(p, &ent);
    if (s.ok()) {
      *statbuf = ent.stat;
    }
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

Status Client::Mkfile(const char* path, mode_t mode) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);
  if (s.ok()) {
    mode = MaskMode(mode);
    s = mdscli_->Fcreat(p, mode);
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

Status Client::Mkdirs(const char* path, mode_t mode) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);
  if (s.ok()) {
    mode = MaskMode(mode);
    s = mdscli_->Mkdir(p, mode, NULL, true,  // create_if_missing
                       false                 // error_if_exists
                       );
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

Status Client::Mkdir(const char* path, mode_t mode) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);
  if (s.ok()) {
    mode = MaskMode(mode);
    s = mdscli_->Mkdir(p, mode);
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

Status Client::Chmod(const char* path, mode_t mode) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);
  if (s.ok()) {
    s = mdscli_->Chmod(p, mode);
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

Status Client::Chown(const char* path, uid_t usr, gid_t grp) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);
  if (s.ok()) {
    s = mdscli_->Chown(p, usr, grp);
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

Status Client::Truncate(const char* path, uint64_t len) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);

  Fentry fentry;
  if (s.ok()) {
    s = mdscli_->Fstat(p, &fentry);
    if (s.ok()) {
      s = fio_->Trunc(fentry, len);
    }
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

Status Client::Unlink(const char* path) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);

  Fentry fentry;
  if (s.ok()) {
    s = mdscli_->Unlink(p, &fentry);
    if (s.ok()) {
      fio_->Drop(fentry);
    }
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

mode_t Client::Umask(mode_t mode) {
  mode &= ACCESSPERMS;  // Discard unrelated bits
  mode_t result = reinterpret_cast<intptr_t>(mask_.Acquire_Load());
  mask_.Release_Store(reinterpret_cast<void*>(mode));

  return result;
}

Status Client::Chroot(const char* path) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);
  if (s.ok()) {
    s = mdscli_->Accessdir(p, X_OK);
  }

  if (s.ok()) {
    MutexLock l(&mutex_);
    has_curroot_set_.NoBarrier_Store(this);  // any non-NULL value is ok
    curroot_ = p.ToString();

    if (curroot_ == "/") {
      has_curroot_set_.NoBarrier_Store(NULL);
      curroot_.clear();
    }
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

Status Client::Chdir(const char* path) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);
  if (s.ok()) {
    s = mdscli_->Accessdir(p, X_OK);
  }

  if (s.ok()) {
    MutexLock l(&mutex_);
    has_curdir_set_.NoBarrier_Store(this);  // any non-NULL value is ok
    curdir_ = p.ToString();

    if (curdir_ == "/") {
      has_curdir_set_.NoBarrier_Store(NULL);
      curdir_.clear();
    }
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

Status Client::Getcwd(char* buf, size_t size) {
  if (buf == NULL) {
    return Status::InvalidArgument(Slice());
  } else if (size == 0) {
    return Status::Range(Slice());
  }

  Status s;
  std::string curdir = "/";
  if (has_curdir_set_.Acquire_Load()) {
    MutexLock ml(&mutex_);
    curdir = curdir_;
  }

  if (size >= curdir.size() + 1) {
    strncpy(buf, curdir.c_str(), curdir.size() + 1);
  } else {
    s = Status::Range(Slice());
  }

  return s;
}

class Client::Builder {
 public:
  explicit Builder()
      : env_(NULL),
        mdsfty_(NULL),
        mdscli_(NULL),
        db_(NULL),
        blkdb_(NULL),
        fio_(NULL) {}
  ~Builder() {}

  Status status() const { return status_; }
  Client* BuildClient();

 private:
  static int FetchUid() {
#if defined(PDLFS_PLATFORM_POSIX)
    return getuid();
#else
    return 0;
#endif
  }

  static int FetchGid() {
#if 0
#if defined(PDLFS_PLATFORM_POSIX)
    return getgid();
#else
    return 0;
#endif
#else
    // XXX: mark everyone part of the root group so they can start
    // creating  stuff under the root directory.
    return 0;
#endif
  }

  void LoadIds();
  void LoadMDSTopology();
  void OpenSession();
  void OpenDB();
  void OpenMDSCli();

  Status status_;
  bool ok() const { return status_.ok(); }
  Env* env_;
  MDSTopology mdstopo_;
  MDSFactoryImpl* mdsfty_;
  MDSCliOptions mdscliopts_;
  MDSClient* mdscli_;
  DBOptions dbopts_;
  DB* db_;
  BlkDBOptions blkdbopts_;
  BlkDB* blkdb_;
  Fio* fio_;
  size_t max_open_files_;
  int cli_id_;
  int session_id_;
  int uid_;
  int gid_;
};

void Client::Builder::LoadIds() {
  uid_ = FetchUid();
  gid_ = FetchGid();

  uint64_t cli_id;
  status_ = config::LoadInstanceId(&cli_id);
  if (ok()) {
    cli_id_ = cli_id;
  }
}

// Try to obtain the uri for a specified server.
// Return the uri if success, or an empty string if not.
static std::string TryObtainSrvUri(int srv_id) {
  std::string run_dir = config::RunDir();
  if (!run_dir.empty()) {
    Env* const env = Env::Default();

    std::string result;
    std::string fname = run_dir;
    char tmp[30];
    snprintf(tmp, sizeof(tmp), "/srv-%08d.uri", srv_id);
    fname += tmp;
    Status s = ReadFileToString(env, fname, &result);
    if (s.ok()) {
#if VERBOSE >= 10
      Verbose(__LOG_ARGS__, 10, "TryObtainSrvUri #%d: %s", srv_id,
              result.c_str());
#endif

      return result;
    }
  }

  return "";
}

void Client::Builder::LoadMDSTopology() {
  uint64_t num_vir_srvs;
  uint64_t num_srvs;

  if (ok()) {
    status_ = config::LoadNumOfVirMetadataSrvs(&num_vir_srvs);
    if (ok()) {
      status_ = config::LoadNumOfMetadataSrvs(&num_srvs);
    }
  }

  if (ok()) {
    std::string addrs = config::MetadataSrvAddrs();
    size_t num_addrs = SplitString(&mdstopo_.srv_addrs, addrs, '&');
    if (num_addrs == 0) {
      for (size_t i = 0; i < num_srvs; i++) {
        std::string uri = TryObtainSrvUri(i);
        if (!uri.empty()) {
          mdstopo_.srv_addrs.push_back(uri);
        } else {
          mdstopo_.srv_addrs.clear();
          break;
        }
      }
    }
  }

  if (ok()) {
    if (mdstopo_.srv_addrs.size() < num_srvs) {
      status_ = Status::InvalidArgument("not enough srv addrs");
    } else if (mdstopo_.srv_addrs.size() > num_srvs) {
      status_ = Status::InvalidArgument("too many srv addrs");
    }
  }

  if (ok()) {
    status_ = config::LoadMDSTracing(&mdstopo_.mds_tracing);
  }

  if (ok()) {
    mdstopo_.rpc_proto = config::RPCProto();
    num_vir_srvs = std::max(num_vir_srvs, num_srvs);
    mdstopo_.num_vir_srvs = num_vir_srvs;
    mdstopo_.num_srvs = num_srvs;
  }

  if (ok()) {
    MDSFactoryImpl* fty = new MDSFactoryImpl;
    status_ = fty->Init(mdstopo_);
    if (ok()) {
      status_ = fty->Start();
    }
    if (ok()) {
      mdsfty_ = fty;
    } else {
      delete fty;
    }
  }
}

// REQUIRES: both LoadIds() and LoadMDSTopology() have been called.
void Client::Builder::OpenSession() {
  if (ok()) {
    assert(mdsfty_ != NULL);
    MDS* mds = mdsfty_->Get(cli_id_ % mdstopo_.num_srvs);
    assert(mds != NULL);
    MDS::OpensessionOptions options;
    options.dir_id = DirId(0, 0, 0);
    MDS::OpensessionRet ret;
    status_ = mds->Opensession(options, &ret);
    if (ok()) {
      session_id_ = ret.session_id;
      env_ = new LazyEnv(ret.env_name, ret.env_conf);
      fio_ = Fio::Open(ret.fio_name, ret.fio_conf);
      if (fio_ == NULL) {
        status_ = Status::IOError("cannot open fio");
      }
    }
  }
}

// REQUIRES: OpenSession() has been called.
void Client::Builder::OpenDB() {
  blkdb_ = NULL;
  db_ = NULL;
#if 0
  std::string output_root;

  if (ok()) {
    assert(mdsfty_ != NULL);
    MDS* mds = mdsfty_->Get(session_id_ % mdstopo_.num_srvs);
    assert(mds != NULL);
    MDS::GetoutputOptions options;
    options.dir_id = DirId(0, 0, 0);
    MDS::GetoutputRet ret;
    status_ = mds->Getoutput(options, &ret);
    if (ok()) {
      output_root = ret.info;
    }
  }

  if (ok()) {
    status_ = config::LoadVerifyChecksums(&blkdbopts_.verify_checksum);
  }

  if (ok()) {
    dbopts_.create_if_missing = true;
    dbopts_.compression = kNoCompression;
    dbopts_.disable_compaction = true;
    dbopts_.env = env_;
  }

  if (ok()) {
    std::string dbhome = output_root;
    char tmp[30];
    snprintf(tmp, sizeof(tmp), "/data_%d", session_id_);
    dbhome += tmp;
    status_ = DB::Open(dbopts_, dbhome, &db_);
    if (ok()) {
      blkdbopts_.db = db_;
      blkdbopts_.uniquefier = session_id_;
      blkdbopts_.owns_db = true;
      blkdb_ = new BlkDB(blkdbopts_);
      db_ = NULL;
    }
  }
#endif
}

// REQUIRES: OpenSession() has been called.
void Client::Builder::OpenMDSCli() {
  uint64_t idx_cache_sz;
  uint64_t lookup_cache_sz;
  uint64_t max_open_files;

  if (ok()) {
    status_ = config::LoadSizeOfCliIndexCache(&idx_cache_sz);
    if (ok()) {
      status_ = config::LoadSizeOfCliLookupCache(&lookup_cache_sz);
    }
  }

  if (ok()) {
    status_ = config::LoadMaxNumOfOpenFiles(&max_open_files);
    max_open_files_ = max_open_files;
  }

  if (ok()) {
    status_ = config::LoadAtomicPathRes(&mdscliopts_.atomic_path_resolution);
    if (ok()) {
      status_ = config::LoadParanoidChecks(&mdscliopts_.paranoid_checks);
    }
  }

  if (ok()) {
    mdscliopts_.env = env_;
    mdscliopts_.factory = mdsfty_;
    mdscliopts_.index_cache_size = idx_cache_sz;
    mdscliopts_.lookup_cache_size = lookup_cache_sz;
    mdscliopts_.num_virtual_servers = mdstopo_.num_vir_srvs;
    mdscliopts_.num_servers = mdstopo_.num_srvs;
    mdscliopts_.session_id = session_id_;
    mdscliopts_.cli_id = cli_id_;
    mdscliopts_.uid = uid_;
    mdscliopts_.gid = gid_;
  }

  if (ok()) {
    mdscli_ = MDSClient::Open(mdscliopts_);
  }
}

Client* Client::Builder::BuildClient() {
  LoadIds();
#if VERBOSE >= 10
  Verbose(__LOG_ARGS__, 10, "LoadIds: %s", STATUS_STR(status_));
#endif

  LoadMDSTopology();
#if VERBOSE >= 10
  Verbose(__LOG_ARGS__, 10, "LoadMDSTopology: %s", STATUS_STR(status_));
#endif

  OpenSession();
#if VERBOSE >= 10
  Verbose(__LOG_ARGS__, 10, "OpenSession: %s", STATUS_STR(status_));
#endif

  OpenDB();
  OpenMDSCli();
#if VERBOSE >= 10
  Verbose(__LOG_ARGS__, 10, "OpenMDSCli: %s", STATUS_STR(status_));
#endif

  if (ok()) {
    Client* cli = new Client(max_open_files_);
    cli->mdscli_ = mdscli_;
    cli->mdsfty_ = mdsfty_;
    cli->fio_ = fio_;
    return cli;
  } else {
    delete mdscli_;
    delete mdsfty_;
    delete fio_;
    delete blkdb_;
    delete db_;
    delete env_;
    return NULL;
  }
}

Status Client::Open(Client** cliptr) {
  Builder builder;
  *cliptr = builder.BuildClient();
  return builder.status();
}

}  // namespace pdlfs
