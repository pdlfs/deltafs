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

#include "rados_fio.h"

#include "pdlfs-common/logging.h"

namespace pdlfs {
namespace rados {

static inline void Error(Logger* L, const char* F, int N, const Status& s) {
  Error(L, F, N, "RADOS: %s", s.ToString().c_str());
}

static inline RadosFobj* ToFobj(Fio::Handle* fh) {
#ifndef NDEBUG
  return dynamic_cast<RadosFobj*>(fh);
#else
  return (RadosFobj*)fh;
#endif
}

static std::string ToOid(const Fentry& fentry) {
  char tmp[200];
  char* p = tmp;
  std::string key_prefix = fentry.UntypedKeyPrefix();
  for (size_t i = 0; i < key_prefix.size(); i++) {
    sprintf(p, "%02x", static_cast<unsigned char>(key_prefix[i]));
    p += 2;
  }
  sprintf(p, ".fobj");
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
    fobj->fio->MaybeSetError(fobj, rados_aio_get_return_value(comp));
  }
}

void RadosFio::MaybeSetError(RadosFobj* fobj, int err) {
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

Status RadosFio::Creat(const Fentry& fentry, bool append_only, Handle** fh) {
  Status s;
  RadosFobj* fobj = NULL;
  *fh = NULL;
  std::string oid = ToOid(fentry);
  rados_ioctx_t fctx;
  int r = rados_ioctx_create(cluster_, pool_name_.c_str(), &fctx);
  if (r != 0) {
    s = RadosError("rados_ioctx_create", r);
  };

  if (s.ok()) {
    fobj = new RadosFobj(this);
    fobj->fctx = fctx;
    fobj->refs = 2;  // One for the returned handle, one for the op
    fobj->mtime = Env::Default()->NowMicros();
    fobj->size = 0;
    fobj->append_only = append_only;
    fobj->bg_err = 0;
    fobj->off = 0;

    rados_completion_t comp;
    rados_aio_create_completion(fobj, NULL, IO_safe, &comp);
    rados_aio_write_full(fctx, oid.c_str(), comp, "", 0);
    rados_aio_release(comp);

    *fh = (Handle*)fobj;
  }

  return s;
}

Status RadosFio::Open(const Fentry& fentry, bool create_if_missing,
                      bool truncate_if_exists, bool append_only,
                      uint64_t* mtime, uint64_t* size, Handle** fh) {
  Status s;
  RadosFobj* fobj = NULL;
  *fh = NULL;
  std::string oid = ToOid(fentry);
  uint64_t obj_size;
  time_t obj_mtime;
  int r = rados_stat(ioctx_, oid.c_str(), &obj_size, &obj_mtime);
  if (r != 0) {
    s = RadosError("rados_stat", r);
  }

  //  If an explicit truncate operation is needed
  bool neee_trunc = false;  // Also used to create missing objects

  if (s.ok()) {
    if (obj_size != 0 && truncate_if_exists) {
      neee_trunc = true;
    }
  } else if (s.IsNotFound()) {
    if (create_if_missing) {
      neee_trunc = true;
    }
  }

  if (neee_trunc) {
    obj_mtime = time(NULL);
    obj_size = 0;
  }

  rados_ioctx_t fctx;
  if (s.ok()) {
    r = rados_ioctx_create(cluster_, pool_name_.c_str(), &fctx);
    if (r != 0) {
      s = RadosError("rados_ioctx_create", r);
    }
  }

  if (s.ok()) {
    fobj = new RadosFobj(this);
    fobj->fctx = fctx;
    fobj->refs = neee_trunc ? 2 : 1;  // One for the handle, one for the op
    fobj->mtime = 1000LLU * 1000LLU * obj_mtime;
    fobj->size = obj_size;
    fobj->append_only = append_only;
    fobj->bg_err = 0;
    fobj->off = 0;

    if (neee_trunc) {
      rados_completion_t comp;
      rados_aio_create_completion(fobj, NULL, IO_safe, &comp);
      rados_aio_write_full(fctx, oid.c_str(), comp, "", 0);
      rados_aio_release(comp);
    }

    *fh = (Handle*)fobj;
    *mtime = fobj->mtime;
    *size = fobj->size;
  }

  return s;
}

Status RadosFio::Stat(const Fentry& fentry, uint64_t* mtime, uint64_t* size) {
  Status s;
  std::string oid = ToOid(fentry);
  time_t obj_mtime;
  size_t obj_size;
  int r = rados_stat(ioctx_, oid.c_str(), &obj_size, &obj_mtime);
  if (r != 0) {
    return RadosError("rados_stat", r);
  } else {
    *mtime = 1000LLU * 1000LLU * obj_mtime;
    *size = obj_size;
    return s;
  }
}

Status RadosFio::Trunc(const Fentry& fentry, uint64_t size) {
  Status s;
  std::string oid = ToOid(fentry);
  int r = rados_trunc(ioctx_, oid.c_str(), size);
  if (r != 0) {
    return RadosError("rados_trunc", r);
  } else {
    return s;
  }
}

Status RadosFio::Drop(const Fentry& fentry) {
  Status s;
  std::string oid = ToOid(fentry);
  int r = rados_remove(ioctx_, oid.c_str());
  if (r != 0) {
    return RadosError("rados_remove", r);
  } else {
    return s;
  }
}

Status RadosFio::Fstat(const Fentry& fentry, Handle* fh, uint64_t* mtime,
                       uint64_t* size, bool skip_cache) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = ToFobj(fh);
  MutexLock ml(mutex_);
  if (fobj->bg_err != 0) {
    s = RadosError("rados_bg_io", fobj->bg_err);
    fobj->bg_err = 0;
  } else {
    if (skip_cache) {
      ExclusiveLock el(&rw_lock_);
      rados_ioctx_t fctx = fobj->fctx;
      if (fctx == NULL) {
        fctx = ioctx_;
      }
      mutex_->Unlock();
      uint64_t obj_size;
      time_t obj_mtime;
      std::string oid = ToOid(fentry);
      int r = rados_stat(fctx, oid.c_str(), &obj_size, &obj_mtime);
      if (r != 0) {
        s = RadosError("rados_stat", r);
      }
      mutex_->Lock();
      if (s.ok()) {
        fobj->mtime = 1000LLU * 1000LLU * obj_mtime;
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

Status RadosFio::Close(const Fentry& fentry, Handle* fh) {
  assert(fh != NULL);
  RadosFobj* fobj = ToFobj(fh);
  mutex_->Lock();
  Unref(fobj);
  mutex_->Unlock();
  return Status::OK();
}

Status RadosFio::Flush(const Fentry& fentry, Handle* fh, bool force_sync) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = ToFobj(fh);
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

Status RadosFio::Ftrunc(const Fentry& fentry, Handle* fh, uint64_t size) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = ToFobj(fh);
  MutexLock ml(mutex_);
  if (fobj->bg_err != 0) {
    s = RadosError("rados_bg_io", fobj->bg_err);
    fobj->bg_err = 0;
  } else {
    ExclusiveLock el(&rw_lock_);
    rados_ioctx_t fctx = fobj->fctx;
    if (fctx == NULL) {
      fctx = ioctx_;
    }
    mutex_->Unlock();
    std::string oid = ToOid(fentry);
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

Status RadosFio::Write(const Fentry& fentry, Handle* fh, const Slice& buf) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = ToFobj(fh);
  MutexLock ml(mutex_);
  if (fobj->bg_err != 0) {
    s = RadosError("rados_bg_io", fobj->bg_err);
    fobj->bg_err = 0;
  } else {
    SharedLock sl(&rw_lock_);
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
    std::string oid = ToOid(fentry);
    if (!force_sync_) {
      rados_completion_t comp;
      rados_aio_create_completion(fobj, NULL, IO_safe, &comp);
      if (!fobj->append_only) {
        rados_aio_write(fctx, oid.c_str(), comp, buf.data(), buf.size(), off);
      } else {
        rados_aio_append(fctx, oid.c_str(), comp, buf.data(), buf.size());
      }
      rados_aio_release(comp);
    } else {
      if (!fobj->append_only) {
        int r = rados_write(fctx, oid.c_str(), buf.data(), buf.size(), off);
        if (r != 0) {
          s = RadosError("rados_write", r);
        }
      } else {
        int r = rados_append(fctx, oid.c_str(), buf.data(), buf.size());
        if (r != 0) {
          s = RadosError("rados_append", r);
        }
      }
    }
    mutex_->Lock();
    if (s.ok()) {
      uint64_t mtime = Env::Default()->NowMicros();
      if (!fobj->append_only) {
        fobj->off = end;
        if (end > fobj->size) {
          fobj->size = end;
        }
      } else {
        fobj->size += buf.size();
        fobj->off = 0;
      }
      if (mtime > fobj->mtime) {
        fobj->mtime = mtime;
      }
    }
  }
  if (!s.ok()) {
    Error(__LOG_ARGS__, s);
  }
  return s;
}

Status RadosFio::Pwrite(const Fentry& fentry, Handle* fh, const Slice& buf,
                        uint64_t off) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = ToFobj(fh);
  MutexLock ml(mutex_);
  if (fobj->bg_err != 0) {
    s = RadosError("rados_bg_io", fobj->bg_err);
    fobj->bg_err = 0;
  } else {
    SharedLock sl(&rw_lock_);
    uint64_t end = off + buf.size();
    rados_ioctx_t fctx = fobj->fctx;
    if (fctx == NULL) {
      fctx = ioctx_;
    }
    if (!force_sync_) {
      fobj->refs++;
    }
    mutex_->Unlock();
    std::string oid = ToOid(fentry);
    if (!force_sync_) {
      rados_completion_t comp;
      rados_aio_create_completion(fobj, NULL, IO_safe, &comp);
      if (!fobj->append_only) {
        rados_aio_write(fctx, oid.c_str(), comp, buf.data(), buf.size(), off);
      } else {
        rados_aio_append(fctx, oid.c_str(), comp, buf.data(), buf.size());
      }
      rados_aio_release(comp);
    } else {
      if (!fobj->append_only) {
        int r = rados_write(fctx, oid.c_str(), buf.data(), buf.size(), off);
        if (r != 0) {
          s = RadosError("rados_write", r);
        }
      } else {
        int r = rados_append(fctx, oid.c_str(), buf.data(), buf.size());
        if (r != 0) {
          s = RadosError("rados_append", r);
        }
      }
    }
    mutex_->Lock();
    if (s.ok()) {
      uint64_t mtime = Env::Default()->NowMicros();
      if (!fobj->append_only) {
        if (end > fobj->size) {
          fobj->size = end;
        }
      } else {
        fobj->size += buf.size();
      }
      if (mtime > fobj->mtime) {
        fobj->mtime = mtime;
      }
    }
  }
  if (!s.ok()) {
    Error(__LOG_ARGS__, s);
  }
  return s;
}

Status RadosFio::Read(const Fentry& fentry, Handle* fh, Slice* result,
                      uint64_t size, char* scratch) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = ToFobj(fh);
  MutexLock ml(mutex_);
  if (fobj->bg_err != 0) {
    s = RadosError("rados_bg_io", fobj->bg_err);
    fobj->bg_err = 0;
  } else {
    SharedLock sl(&rw_lock_);
    uint64_t off = fobj->off;
    rados_ioctx_t fctx = fobj->fctx;
    if (fctx == NULL) {
      fctx = ioctx_;
    }
    mutex_->Unlock();
    std::string oid = ToOid(fentry);
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

Status RadosFio::Pread(const Fentry& fentry, Handle* fh, Slice* result,
                       uint64_t off, uint64_t size, char* scratch) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = ToFobj(fh);
  MutexLock ml(mutex_);
  if (fobj->bg_err != 0) {
    s = RadosError("rados_bg_io", fobj->bg_err);
    fobj->bg_err = 0;
  } else {
    SharedLock sl(&rw_lock_);
    rados_ioctx_t fctx = fobj->fctx;
    if (fctx == NULL) {
      fctx = ioctx_;
    }
    mutex_->Unlock();
    std::string oid = ToOid(fentry);
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
