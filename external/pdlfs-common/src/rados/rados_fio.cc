/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/pdlfs_config.h"

#include "rados_fio.h"

namespace pdlfs {
namespace rados {

static std::string ToOid(const Slice& encoding) {
  Slice key_prefix = Fentry::ExtractUntypedKeyPrefix(encoding);
  char tmp[200];
  sprintf(tmp, "o_");
  char* p = tmp + 2;
  for (size_t i = 0; i < key_prefix.size(); i++) {
    sprintf(p, "%02x", (unsigned char)key_prefix[i]);
    p += 2;
  }
  return tmp;
}

RadosFio::~RadosFio() {
  // Wait until all async IO operations to finish
  rados_aio_flush(ioctx_);
  rados_ioctx_destroy(ioctx_);
}

void RadosFio::IO_safe(rados_completion_t comp, void* arg) {
  if (arg != NULL) {
    RadosFobj* fobj = static_cast<RadosFobj*>(arg);
    fobj->fio->UpdateAndUnref(fobj, rados_aio_get_return_value(comp));
  }
}

void RadosFio::UpdateAndUnref(RadosFobj* fobj, int err) {
  MutexLock ml(mutex_);
  if (fobj->bg_err == 0 && err != 0) {
    fobj->bg_err = err;
  }
  Unref(fobj);
}

// REQUIRES: mutex_ has been locked.
void RadosFio::Unref(RadosFobj* fobj) {
  assert(fobj->refs > 0);
  fobj->refs--;
  if (fobj->refs == 0) {
    if (fobj->fctx != NULL) {
      rados_ioctx_destroy(fobj->fctx);
    }
    delete fobj;
  }
}

Status RadosFio::Creat(const Slice& fentry_encoding, Handle** fh) {
  Status s;
  std::string oid = ToOid(fentry_encoding);
  rados_ioctx_t fctx;
  int r = rados_ioctx_create(cluster_, pool_name_.c_str(), &fctx);
  if (r != 0) {
    s = RadosError("rados_ioctx_create", r);
  };

  if (s.ok()) {
    RadosFobj* fobj = new RadosFobj(this);
    fobj->fctx = fctx;
    fobj->refs = 2;  // One for the handle, one for the next async op
    fobj->mtime = Env::Default()->NowMicros();
    fobj->size = 0;
    fobj->bg_err = 0;
    fobj->off = 0;

    rados_completion_t comp;
    rados_aio_create_completion(fobj, NULL, IO_safe, &comp);
    rados_aio_write_full(fctx, oid.c_str(), comp, "", 0);
    rados_aio_release(comp);

    *fh = reinterpret_cast<Handle*>(fobj);
  }

  return s;
}

Status RadosFio::Open(const Slice& fentry_encoding, bool create_if_missing,
                      bool truncate_if_exists, uint64_t* mtime, uint64_t* size,
                      Handle** fh) {
  Status s;
  std::string oid = ToOid(fentry_encoding);
  uint64_t obj_size;
  time_t obj_mtime;
  int r = rados_stat(ioctx_, oid.c_str(), &obj_size, &obj_mtime);
  if (r != 0) {
    s = RadosError("rados_stat", r);
  }

  bool need_trunc = false;  // If an explicit truncate operation is needed
  if (s.ok()) {
    if (obj_size != 0 && truncate_if_exists) {
      obj_mtime = time(NULL);
      obj_size = 0;
      need_trunc = true;
    }
  } else if (s.IsNotFound()) {
    if (create_if_missing) {
      s = Status::OK();
      obj_mtime = time(NULL);
      obj_size = 0;
      need_trunc = true;
    }
  }

  rados_ioctx_t fctx;
  if (s.ok()) {
    r = rados_ioctx_create(cluster_, pool_name_.c_str(), &fctx);
    if (r != 0) {
      s = RadosError("rados_ioctx_create", r);
    }
  }

  if (s.ok()) {
    RadosFobj* fobj = new RadosFobj(this);
    fobj->fctx = fctx;
    fobj->refs = need_trunc ? 2 : 1;
    fobj->mtime = 1000ULL * 1000ULL * obj_mtime;
    fobj->size = obj_size;
    fobj->bg_err = 0;
    fobj->off = 0;

    if (need_trunc) {
      rados_completion_t comp;
      rados_aio_create_completion(fobj, NULL, IO_safe, &comp);
      rados_aio_write_full(fctx, oid.c_str(), comp, "", 0);
      rados_aio_release(comp);
    }

    *fh = reinterpret_cast<Handle*>(fobj);
    *mtime = fobj->mtime;
    *size = fobj->size;
  }

  return s;
}

Status RadosFio::Drop(const Slice& fentry_encoding) {
  Status s;
  std::string oid = ToOid(fentry_encoding);
  int r = rados_remove(ioctx_, oid.c_str());
  if (r != 0) {
    return RadosError("rados_remove", r);
  } else {
    return s;
  }
}

Status RadosFio::Stat(const Slice& fentry_encoding, Handle* fh, uint64_t* mtime,
                      uint64_t* size, bool skip_cache) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = reinterpret_cast<RadosFobj*>(fh);
  MutexLock ml(mutex_);
  if (fobj->bg_err != 0) {
    s = RadosError("rados_bg_io", fobj->bg_err);
    fobj->bg_err = 0;
  } else {
    if (skip_cache) {
      ExclusiveLock el(&rwl_);
      rados_ioctx_t fctx = fobj->fctx;
      if (fctx == NULL) {
        fctx = ioctx_;
      }
      mutex_->Unlock();
      uint64_t obj_size;
      time_t obj_mtime;
      std::string oid = ToOid(fentry_encoding);
      int r = rados_stat(fctx, oid.c_str(), &obj_size, &obj_mtime);
      if (r != 0) {
        s = RadosError("rados_stat", r);
      }
      mutex_->Lock();
      if (s.ok()) {
        fobj->mtime = 1000ULL * 1000ULL * obj_mtime;
        fobj->size = obj_size;
      }
    }
    if (s.ok()) {
      *mtime = fobj->mtime;
      *size = fobj->size;
    }
  }
  if (!s.ok()) {
    Error(__LOG_ARGS__, s);
  }
  return s;
}

Status RadosFio::Close(const Slice& fentry_encoding, Handle* fh) {
  assert(fh != NULL);
  RadosFobj* fobj = reinterpret_cast<RadosFobj*>(fh);
  mutex_->Lock();
  Unref(fobj);
  mutex_->Unlock();
  return Status::OK();
}

Status RadosFio::Flush(const Slice& fentry_encoding, Handle* fh,
                       bool force_sync) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = reinterpret_cast<RadosFobj*>(fh);
  mutex_->Lock();
  if (fobj->bg_err != 0) {
    s = RadosError("rados_bg_io", fobj->bg_err);
    fobj->bg_err = 0;
  }
  rados_ioctx_t fctx = fobj->fctx;
  if (fctx == NULL) {
    fctx = ioctx_;
  }
  mutex_->Unlock();
  if (s.ok()) {
    // No data is buffered locally so there is no need for an explicit flush,
    // so only sync will be handled
    if (force_sync) {
      rados_aio_flush(fctx);
      MutexLock ml(mutex_);
      if (fobj->bg_err != 0) {
        s = RadosError("rados_bg_io", fobj->bg_err);
        fobj->bg_err = 0;
      }
    }
  }
  if (!s.ok()) {
    Error(__LOG_ARGS__, s);
  }
  return s;
}

Status RadosFio::Truncate(const Slice& fentry_encoding, Handle* fh,
                          uint64_t size) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = reinterpret_cast<RadosFobj*>(fh);
  MutexLock ml(mutex_);
  if (fobj->bg_err != 0) {
    s = RadosError("rados_bg_io", fobj->bg_err);
    fobj->bg_err = 0;
  } else {
    ExclusiveLock el(&rwl_);
    rados_ioctx_t fctx = fobj->fctx;
    if (fctx == NULL) {
      fctx = ioctx_;
    }
    mutex_->Unlock();
    std::string oid = ToOid(fentry_encoding);
    int r = rados_trunc(fctx, oid.c_str(), size);
    if (r != 0) {
      s = RadosError("rados_trunc", r);
    }
    mutex_->Lock();
    if (s.ok()) {
      uint64_t mtime = Env::Default()->NowMicros();
      if (mtime > fobj->mtime) {
        fobj->mtime = mtime;
      }
      fobj->size = size;
    }
  }
  if (!s.ok()) {
    Error(__LOG_ARGS__, s);
  }
  return s;
}

Status RadosFio::Write(const Slice& fentry_encoding, Handle* fh,
                       const Slice& buf) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = reinterpret_cast<RadosFobj*>(fh);
  MutexLock ml(mutex_);
  if (fobj->bg_err != 0) {
    s = RadosError("rados_bg_io", fobj->bg_err);
    fobj->bg_err = 0;
  } else {
    SharedLock sl(&rwl_);
    uint64_t off = fobj->off;
    uint64_t end = off + buf.size();
    rados_ioctx_t fctx = fobj->fctx;
    if (fctx == NULL) {
      fctx = ioctx_;
    }
    if (!force_sync_) {
      fobj->refs++;
    }
    mutex_->Unlock();
    std::string oid = ToOid(fentry_encoding);
    if (!force_sync_) {
      rados_completion_t comp;
      rados_aio_create_completion(fobj, NULL, IO_safe, &comp);
      rados_aio_write(fctx, oid.c_str(), comp, buf.data(), buf.size(), off);
      rados_aio_release(comp);
    } else {
      int r = rados_write(fctx, oid.c_str(), buf.data(), buf.size(), off);
      if (r != 0) {
        s = RadosError("rados_write", r);
      }
    }
    mutex_->Lock();
    if (s.ok()) {
      uint64_t mtime = Env::Default()->NowMicros();
      fobj->off = end;
      if (mtime > fobj->mtime) {
        fobj->mtime = mtime;
      }
      if (end > fobj->size) {
        fobj->size = end;
      }
    }
  }
  if (!s.ok()) {
    Error(__LOG_ARGS__, s);
  }
  return s;
}

Status RadosFio::Pwrite(const Slice& fentry_encoding, Handle* fh,
                        const Slice& buf, uint64_t off) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = reinterpret_cast<RadosFobj*>(fh);
  MutexLock ml(mutex_);
  if (fobj->bg_err != 0) {
    s = RadosError("rados_bg_io", fobj->bg_err);
    fobj->bg_err = 0;
  } else {
    SharedLock sl(&rwl_);
    uint64_t end = off + buf.size();
    rados_ioctx_t fctx = fobj->fctx;
    if (fctx == NULL) {
      fctx = ioctx_;
    }
    if (!force_sync_) {
      fobj->refs++;
    }
    mutex_->Unlock();
    std::string oid = ToOid(fentry_encoding);
    if (!force_sync_) {
      rados_completion_t comp;
      rados_aio_create_completion(fobj, NULL, IO_safe, &comp);
      rados_aio_write(fctx, oid.c_str(), comp, buf.data(), buf.size(), off);
      rados_aio_release(comp);
    } else {
      int r = rados_write(fctx, oid.c_str(), buf.data(), buf.size(), off);
      if (r != 0) {
        s = RadosError("rados_write", r);
      }
    }
    mutex_->Lock();
    if (s.ok()) {
      uint64_t mtime = Env::Default()->NowMicros();
      if (mtime > fobj->mtime) {
        fobj->mtime = mtime;
      }
      if (end > fobj->size) {
        fobj->size = end;
      }
    }
  }
  if (!s.ok()) {
    Error(__LOG_ARGS__, s);
  }
  return s;
}

Status RadosFio::Read(const Slice& fentry_encoding, Handle* fh, Slice* result,
                      uint64_t size, char* scratch) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = reinterpret_cast<RadosFobj*>(fh);
  MutexLock ml(mutex_);
  if (fobj->bg_err != 0) {
    s = RadosError("rados_bg_io", fobj->bg_err);
    fobj->bg_err = 0;
  } else {
    SharedLock sl(&rwl_);
    uint64_t off = fobj->off;
    rados_ioctx_t fctx = fobj->fctx;
    if (fctx == NULL) {
      fctx = ioctx_;
    }
    mutex_->Unlock();
    std::string oid = ToOid(fentry_encoding);
    int n = rados_read(fctx, oid.c_str(), scratch, size, off);
    if (n < 0) {
      s = RadosError("rados_read", n);
    }
    mutex_->Lock();
    if (s.ok()) {
      *result = Slice(scratch, n);
      if (n > 0) {
        uint64_t end = off + n;
        fobj->off = end;
        if (end > fobj->size) {
          fobj->size = end;
        }
      }
    }
  }
  if (!s.ok()) {
    Error(__LOG_ARGS__, s);
  }
  return s;
}

Status RadosFio::Pread(const Slice& fentry_encoding, Handle* fh, Slice* result,
                       uint64_t off, uint64_t size, char* scratch) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = reinterpret_cast<RadosFobj*>(fh);
  MutexLock ml(mutex_);
  if (fobj->bg_err != 0) {
    s = RadosError("rados_bg_io", fobj->bg_err);
    fobj->bg_err = 0;
  } else {
    SharedLock sl(&rwl_);
    rados_ioctx_t fctx = fobj->fctx;
    if (fctx == NULL) {
      fctx = ioctx_;
    }
    mutex_->Unlock();
    std::string oid = ToOid(fentry_encoding);
    int n = rados_read(fctx, oid.c_str(), scratch, size, off);
    if (n < 0) {
      s = RadosError("rados_read", n);
    }
    mutex_->Lock();
    if (s.ok()) {
      *result = Slice(scratch, n);
      if (n > 0) {
        uint64_t end = off + n;
        if (end > fobj->size) {
          fobj->size = end;
        }
      }
    }
  }
  if (!s.ok()) {
    Error(__LOG_ARGS__, s);
  }
  return s;
}

}  // namespace rados
}  // namespace pdlfs
