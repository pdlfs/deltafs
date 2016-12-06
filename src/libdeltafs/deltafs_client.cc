/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
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
#include "pdlfs-common/blkdb.h"
#include "pdlfs-common/lazy_env.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/rpc.h"
#include "pdlfs-common/strutil.h"

namespace pdlfs {

// Helper for getting status strings.
#define C_STR(status) status.ToString().c_str()
#define OP_VERBOSE_LEVEL 8
#define OP_STATUS(op, path, status) \
  "%s '%s': %s", op, path.c_str(), C_STR(status)
#define OP_VERBOSE(path, status) \
  Verbose(__LOG_ARGS__, OP_VERBOSE_LEVEL, OP_STATUS(__func__, path, status))

static inline Status FileAccessModeNotMatched() {
  return Status::InvalidFileDescriptor(Slice());
}

static inline Status BadDescriptor() {
  return Status::InvalidFileDescriptor(Slice());
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
  File* f = fds_[index];
  assert(f != NULL);
  fds_[index] = NULL;
  assert(num_open_fds_ > 0);
  num_open_fds_--;
  return f;
}

// REQUIRES: less than "max_open_files_" files have been opened.
// REQUIRES: mutex_ has been locked.
size_t Client::Open(const Slice& encoding, int flags, const Stat& stat,
                    Fio::Handle* fh) {
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
void Client::Unref(File* f) {
  assert(f->refs > 0);
  f->refs--;
  if (f->refs == 0) {
    f->next->prev = f->prev;
    f->prev->next = f->next;
    mutex_.Unlock();
    fio_->Close(f->fentry_encoding(), f->fh);
    free(f);
    mutex_.Lock();
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
  return ~mask & mode;
}

// Open a file for I/O operations. Return OK on success.
// If O_CREAT is specified and the file does not exist, it will be created.
// If both O_CREAT and O_EXCL are specified and the file exists,
// error is returned.
// If O_TRUNC is specified and the file already exists and is a regular
// file and the open mode allows writing, it will be truncated to length 0.
// In addition to opening the file, a file descriptor is allocated
// to represent the file. The current length of the file is also returned along
// with the file descriptor.
// Only a fixed amount of file can be opened simultaneously.
// A process may open a same file multiple times and obtain multiple file
// descriptors that point to distinct open file entries.
// If the opened file is a directory, O_RDONLY must and only that
// can be specified.
Status Client::Fopen(const Slice& p, int flags, mode_t mode, FileInfo* info) {
  Status s;
  Slice path = p;
  std::string tmp;
  s = ExpandPath(&path, &tmp);
  if (!s.ok()) {
    return s;
  }

  MutexLock ml(&mutex_);
  if (num_open_fds_ >= max_open_fds_) {
    s = Status::TooManyOpens(Slice());
  } else {
    mutex_.Unlock();
    Fentry fentry;
    mode = MaskMode(mode);
    uint64_t my_time = Env::Default()->NowMicros();
    if ((flags & O_CREAT) == O_CREAT) {
      const bool error_if_exists = (flags & O_EXCL) == O_EXCL;
      s = mdscli_->Fcreat(path, mode, &fentry, error_if_exists);
    } else {
      s = mdscli_->Fstat(path, &fentry);
      if (s.ok()) {
        if (!S_ISDIR(fentry.stat.FileMode())) {
          if ((flags & O_DIRECTORY) == O_DIRECTORY) {
            s = Status::DirExpected(Slice());
          }
        } else {
          if ((flags & O_ACCMODE) != O_RDONLY) {
            s = Status::FileExpected(Slice());
          }
        }
      }
    }

#if VERBOSE >= 10
    if (s.ok()) {
      Verbose(__LOG_ARGS__, 10, "Fopen: %s -> inode=[%llu:%llu:%llu]",
              path.c_str(), (unsigned long long)fentry.stat.RegId(),
              (unsigned long long)fentry.stat.SnapId(),
              (unsigned long long)fentry.stat.InodeNo());
    }
#endif

    // Verify I/O permissions
    if (s.ok() && S_ISREG(fentry.stat.FileMode())) {
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

    char tmp[DELTAFS_FENTRY_BUFSIZE];
    Slice encoding;
    uint64_t mtime = 0;
    uint64_t size = 0;

    if (s.ok()) {
      encoding = fentry.EncodeTo(tmp);

      mtime = fentry.stat.ModifyTime();
      size = fentry.stat.FileSize();
    }

    // Link file objects
    Fio::Handle* fh = NULL;

    if (s.ok() && S_ISREG(fentry.stat.FileMode())) {
      if (fentry.stat.ChangeTime() < my_time) {
        const bool create_if_missing = true;  // Allow lazy object creation
        const bool truncate_if_exists =
            (flags & O_ACCMODE) != O_RDONLY && (flags & O_TRUNC) == O_TRUNC;
        s = fio_->Open(encoding, create_if_missing, truncate_if_exists, &mtime,
                       &size, &fh);
      } else {
        // File doesn't exist before so we explicit create it
        s = fio_->Creat(encoding, &fh);
      }
    }

    mutex_.Lock();
    if (s.ok()) {
      if (num_open_fds_ >= max_open_fds_) {
        s = Status::TooManyOpens(Slice());
        if (fh != NULL) {
          fio_->Close(encoding, fh);
        }
      } else {
        fentry.stat.SetFileSize(size);
        fentry.stat.SetModifyTime(mtime);

        info->fd = Open(encoding, flags, fentry.stat, fh);
        info->stat = fentry.stat;
      }
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
Client::File* Client::FetchFile(int fd, Fentry* ent) {
  size_t index = fd;
  if (index < max_open_fds_) {
    File* f = fds_[index];
    if (f != NULL) {
      Slice input = f->fentry_encoding();
      ent->DecodeFrom(&input);
    }
    return f;
  } else {
    return NULL;
  }
}

Status Client::Fstat(int fd, Stat* statbuf) {
  Status s;
  MutexLock ml(&mutex_);
  Fentry ent;
  File* file = FetchFile(fd, &ent);
  if (file == NULL) {
    return BadDescriptor();
  } else {
    assert(file->fh != NULL);
    file->refs++;  // Ref
    uint64_t mtime = 0;
    uint64_t size = 0;
    if (S_ISREG(ent.file_mode())) {
      mutex_.Unlock();
      s = fio_->Fstat(file->fentry_encoding(), file->fh, &mtime, &size);
      mutex_.Lock();
    } else {
      // FIXME
    }
    if (s.ok()) {
      ent.stat.SetModifyTime(mtime);
      ent.stat.SetFileSize(size);
      *statbuf = ent.stat;
    }
    Unref(file);
    return s;
  }
}

Status Client::Pwrite(int fd, const Slice& data, uint64_t off) {
  MutexLock ml(&mutex_);
  Fentry ent;
  File* file = FetchFile(fd, &ent);
  if (file == NULL) {
    return BadDescriptor();
  } else if (DELTAFS_DIR_IS_PLFS_STYLE(ent.file_mode())) {
    if (!S_ISDIR(ent->file_mode())) {
      // TODO: write into a file under a pdlfs dir
    }

    return FileAccessModeNotMatched();
  } else if (S_ISDIR(ent.file_mode())) {
    return FileAccessModeNotMatched();
  } else if (!IsWriteOk(file)) {
    return FileAccessModeNotMatched();
  } else {
    Status s;
    assert(file->fh != NULL);
    file->refs++;  // Ref
    mutex_.Unlock();
    s = fio_->Pwrite(file->fentry_encoding(), file->fh, data, off);
    mutex_.Lock();
    if (s.ok()) {
      file->seq_write++;
    }
    Unref(file);
    return s;
  }
}

Status Client::Write(int fd, const Slice& data) {
  MutexLock ml(&mutex_);
  Fentry ent;
  File* file = FetchFile(fd, &ent);
  if (file == NULL) {
    return BadDescriptor();
  } else if (DELTAFS_DIR_IS_PLFS_STYLE(ent.file_mode())) {
    if (!S_ISDIR(ent.file_mode())) {
      // TODO: write into a file under a pdlfs dir
    }

    return FileAccessModeNotMatched();
  } else if (S_ISDIR(ent.file_mode())) {
    return FileAccessModeNotMatched();
  } else if (!IsWriteOk(file)) {
    return FileAccessModeNotMatched();
  } else {
    Status s;
    assert(file->fh != NULL);
    file->refs++;  // Ref
    mutex_.Unlock();
    s = fio_->Write(file->fentry_encoding(), file->fh, data);
    mutex_.Lock();
    if (s.ok()) {
      file->seq_write++;
    }
    Unref(file);
    return s;
  }
}

Status Client::Ftruncate(int fd, uint64_t len) {
  MutexLock ml(&mutex_);
  Fentry ent;
  File* file = FetchFile(fd, &ent);
  if (file == NULL) {
    return BadDescriptor();
  } else if (DELTAFS_DIR_IS_PLFS_STYLE(ent.file_mode())) {
    return FileAccessModeNotMatched();
  } else if (S_ISDIR(ent.file_mode())) {
    return FileAccessModeNotMatched();
  } else if (!IsWriteOk(file)) {
    return FileAccessModeNotMatched();
  } else {
    Status s;
    assert(file->fh != NULL);
    file->refs++;  // Ref
    mutex_.Unlock();
    s = fio_->Ftruncate(file->fentry_encoding(), file->fh, len);
    mutex_.Lock();
    if (s.ok()) {
      file->seq_write++;
    }
    Unref(file);
    return s;
  }
}

Status Client::Fdatasync(int fd) {
  Status s;
  MutexLock ml(&mutex_);
  Fentry ent;
  File* file = FetchFile(fd.& ent);
  if (file == NULL) {
    s = BadDescriptor();
  } else if (DELTAFS_DIR_IS_PLFS_STYLE(ent.file_mode())) {
    s = Status::NotSupported(Slice());  // FIXME

  } else if (S_ISDIR(ent.file_mode())) {
    s = Status::NotSupported(Slice());  // FIXME

  } else if ((file->flags & O_ACCMODE) != O_RDONLY) {
    assert(file->fh != NULL);
    uint32_t seq_write = file->seq_write;
    uint32_t seq_flush = file->seq_flush;
    file->refs++;  // Ref
    mutex_.Unlock();
    s = fio_->Flush(file->fentry_encoding(), file->fh, true /*force*/);
    if (s.ok() && seq_flush < seq_write) {
      uint64_t mtime;
      uint64_t size;
      s = fio_->Fstat(file->fentry_encoding(), file->fh, &mtime, &size,
                      true /*skip_cache*/);
      if (s.ok()) {
        Fentry fentry;
        Slice encoding = file->fentry_encoding();
        if (fentry.DecodeFrom(&encoding)) {
          s = mdscli_->Ftruncate(fentry, mtime, size);
        } else {
          s = Status::Corruption(Slice());
        }
      }
    }
    mutex_.Lock();
    if (s.ok()) {
      if (seq_write > file->seq_flush) {
        file->seq_flush = seq_write;
      }
    }
    Unref(file);
  } else {
    // Nothing to sync
  }

  return s;
}

Status Client::Pread(int fd, Slice* result, uint64_t off, uint64_t size,
                     char* scratch) {
  MutexLock ml(&mutex_);
  Fentry ent;
  File* file = FetchFile(fd, &ent);
  if (file == NULL) {
    return BadDescriptor();
  } else if (DELTAFS_DIR_IS_PLFS_STYLE(ent.file_mode())) {
    if (!S_ISDIR(ent.file_mode())) {
      // TODO: read a file from a plfs dir
    }

    return FileAccessModeNotMatched();
  } else if (S_ISDIR(ent.file_mode())) {
    return FileAccessModeNotMatched();
  } else if (!IsReadOk(file)) {
    return FileAccessModeNotMatched();
  } else {
    Status s;
    assert(file->fh != NULL);
    file->refs++;  // Ref
    mutex_.Unlock();
    Slice e = file->fentry_encoding();
    s = fio_->Pread(e, file->fh, result, off, size, scratch);
    mutex_.Lock();
    Unref(file);
    return s;
  }
}

Status Client::Read(int fd, Slice* result, uint64_t size, char* scratch) {
  MutexLock ml(&mutex_);
  Fentry ent;
  File* file = FetchFile(fd, &ent);
  if (file == NULL) {
    return BadDescriptor();
  } else if (DELTAFS_DIR_IS_PLFS_STYLE(ent.file_mode())) {
    if (!S_ISDIR(ent.file_mode())) {
      // TODO: read a file from a plfs dir
    }

    return FileAccessModeNotMatched();
  } else if (S_ISDIR(ent.file_mode())) {
    return FileAccessModeNotMatched();
  } else if (!IsReadOk(file)) {
    return FileAccessModeNotMatched();
  } else {
    Status s;
    assert(file->fh != NULL);
    file->refs++;  // Ref
    mutex_.Unlock();
    Slice e = file->fentry_encoding();
    s = fio_->Read(e, file->fh, result, size, scratch);
    mutex_.Lock();
    Unref(file);
    return s;
  }
}

Status Client::Flush(int fd) {
  Status s;
  MutexLock ml(&mutex_);
  Fentry ent;
  File* file = FetchFile(fd, &ent);
  if (file == NULL) {
    s = BadDescriptor();
  } else if (DELTAFS_DIR_IS_PLFS_STYLE(ent.file_mode())) {
    if (S_ISDIR(ent.file_mode())) {
      // TODO: force the generation of a new epoch
    } else {
      // Ignore it
    }

  } else if (S_ISDIR(ent.file_mode())) {
    // Nothing to flush

  } else if ((file->flags & O_ACCMODE) != O_RDONLY) {
    assert(file->fh != NULL);
    uint32_t seq_write = file->seq_write;
    uint32_t seq_flush = file->seq_flush;
    file->refs++;  // Ref
    mutex_.Unlock();
    s = fio_->Flush(file->fentry_encoding(), file->fh);
    if (s.ok() && seq_flush < seq_write) {
      uint64_t mtime;
      uint64_t size;
      s = fio_->Fstat(file->fentry_encoding(), file->fh, &mtime, &size);
      if (s.ok()) {
        Fentry fentry;
        Slice encoding = file->fentry_encoding();
        if (fentry.DecodeFrom(&encoding)) {
          s = mdscli_->Ftruncate(fentry, mtime, size);
        } else {
          s = Status::Corruption(Slice());
        }
      }
    }
    mutex_.Lock();
    if (s.ok()) {
      if (seq_write > file->seq_flush) {
        file->seq_flush = seq_write;
      }
    }
    Unref(file);
  } else {
    // Nothing to flush
  }

  return s;
}

Status Client::Close(int fd) {
  Status s;
  MutexLock ml(&mutex_);
  Fentry ent;
  File* file = FetchFile(fd, &ent);
  if (file == NULL) {
    return BadDescriptor();
  } else {
    if (DELTAFS_DIR_IS_PLFS_STYLE(ent.file_mode())) {
      if (S_ISDIR(ent.file_mode())) {
        // XXX: TODO
      } else {
        // Do nothing
      }

    } else if (S_ISREG(ent.file_mode())) {
      while (file->seq_flush < file->seq_write) {
        mutex_.Unlock();
        Flush(fd);  // Ignore errors
        mutex_.Lock();
      }
    } else {
      // Do nothing
    }

    Free(fd);  // Release fd slot

    Unref(file);
  }

  return s;
}

Status Client::Access(const Slice& path, int mode) {
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

Status Client::Accessdir(const Slice& path, int mode) {
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

Status Client::Listdir(const Slice& path, std::vector<std::string>* names) {
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

Status Client::Lstat(const Slice& path, Stat* statbuf) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);

  Fentry ent;
  char encoding_buf[DELTAFS_FENTRY_BUFSIZE];
  Slice encoding;
  if (s.ok()) {
    s = mdscli_->Fstat(p, &ent);
    if (s.ok()) {
      encoding = ent.EncodeTo(encoding_buf);
      *statbuf = ent.stat;
    }
  }

  if (s.ok()) {
    if (S_ISREG(statbuf->FileMode())) {
      uint64_t mtime;
      uint64_t size;
      s = fio_->Stat(encoding, &mtime, &size);
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

Status Client::Getattr(const Slice& path, Stat* statbuf) {
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

Status Client::Mkfile(const Slice& path, mode_t mode) {
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

Status Client::Mkdirs(const Slice& path, mode_t mode) {
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

Status Client::Mkdir(const Slice& path, mode_t mode) {
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

Status Client::Chmod(const Slice& path, mode_t mode) {
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

Status Client::Chown(const Slice& path, uid_t usr, gid_t grp) {
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

Status Client::Truncate(const Slice& path, uint64_t len) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);

  Fentry ent;
  char encoding_buf[DELTAFS_FENTRY_BUFSIZE];
  Slice encoding;
  if (s.ok()) {
    s = mdscli_->Fstat(p, &ent);
    if (s.ok()) {
      encoding = ent.EncodeTo(encoding_buf);
    }
  }

  if (s.ok()) {
    s = fio_->Truncate(encoding, len);
  }

#if VERBOSE >= OP_VERBOSE_LEVEL
  OP_VERBOSE(p, s);
#endif

  return s;
}

Status Client::Unlink(const Slice& path) {
  Status s;
  Slice p = path;
  std::string tmp;
  s = ExpandPath(&p, &tmp);

  Fentry ent;
  char encoding_buf[DELTAFS_FENTRY_BUFSIZE];
  Slice encoding;
  if (s.ok()) {
    s = mdscli_->Unlink(p, &ent);
    if (s.ok()) {
      encoding = ent.EncodeTo(encoding_buf);
    }
  }

  if (s.ok()) {
    fio_->Drop(encoding);
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

Status Client::Chroot(const Slice& path) {
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

Status Client::Chdir(const Slice& path) {
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
  Verbose(__LOG_ARGS__, 10, "LoadIds: %s", C_STR(status_));
#endif

  LoadMDSTopology();
#if VERBOSE >= 10
  Verbose(__LOG_ARGS__, 10, "LoadMDSTopology: %s", C_STR(status_));
#endif

  OpenSession();
#if VERBOSE >= 10
  Verbose(__LOG_ARGS__, 10, "OpenSession: %s", C_STR(status_));
#endif

  OpenDB();
  OpenMDSCli();
#if VERBOSE >= 10
  Verbose(__LOG_ARGS__, 10, "OpenMDSCli: %s", C_STR(status_));
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
