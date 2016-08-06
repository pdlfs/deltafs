/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#if defined(RADOS)
#include "rados_fio.h"

namespace pdlfs {
namespace rados {

static std::string ToOid(const Slice& encoding) {
  Slice key_prefix = Fentry::ExtractUntypedKeyPrefix(encoding);
  char tmp[200];
  int n = snprintf(tmp, sizeof(tmp), "f-");
  char* p = tmp + n;
  for (size_t i = 0; i < key_prefix.size(); i++) {
    snprintf(p, sizeof(tmp) - (p - tmp), "%02X", (unsigned)key_prefix[i]);
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
  if (fobj->err == 0 && err != 0) {
    fobj->err = err;
  }
  Unref(fobj);
}

// REQUIRES: mutex_ has been locked.
void RadosFio::Unref(RadosFobj* fobj) {
  assert(fobj->nrefs > 0);
  fobj->nrefs--;
  if (fobj->nrefs == 0) {
    delete fobj;
  }
}

Status RadosFio::Creat(const Slice& fentry_encoding, Handle** fh) {
  Status s;
  std::string oid = ToOid(fentry_encoding);

  RadosFobj* fobj = new RadosFobj(this);
  fobj->nrefs = 2;  // One for the handle, one for the next async op
  fobj->mtime = Env::Default()->NowMicros();
  fobj->size = 0;
  fobj->err = 0;
  fobj->off = 0;

  rados_completion_t comp;
  rados_aio_create_completion(fobj, NULL, IO_safe, &comp);
  rados_aio_write_full(ioctx_, oid.c_str(), comp, "", 0);
  rados_aio_release(comp);

  *fh = reinterpret_cast<Handle*>(fobj);

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

  if (s.ok()) {
    RadosFobj* fobj = new RadosFobj(this);
    fobj->nrefs = need_trunc ? 2 : 1;
    fobj->mtime = 1000LLU * 1000LLU * obj_mtime;
    fobj->size = obj_size;
    fobj->err = 0;
    fobj->off = 0;

    if (need_trunc) {
      rados_completion_t comp;
      rados_aio_create_completion(fobj, NULL, IO_safe, &comp);
      rados_aio_write_full(ioctx_, oid.c_str(), comp, "", 0);
      rados_aio_release(comp);
    }

    *fh = reinterpret_cast<Handle*>(fobj);
    *mtime = fobj->mtime;
    *size = fobj->size;
  }

  return s;
}

Status RadosFio::GetInfo(const Slice& fentry_encoding, Handle* fh, bool* dirty,
                         uint64_t* mtime, uint64_t* size) {
  Status s;
  assert(fh != NULL);
  const RadosFobj* fobj = reinterpret_cast<RadosFobj*>(fh);
  MutexLock ml(mutex_);
  if (fobj->err != 0) {
    s = RadosError("rados_bg_io", fobj->err);
    Error(__LOG_ARGS__, s);
  } else {
    *mtime = fobj->mtime;
    *size = fobj->size;
    // We don't buffer/cache data locally
    *dirty = false;
  }
  return s;
}

Status RadosFio::Close(const Slice& fentry_encoding, Handle* fh) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = reinterpret_cast<RadosFobj*>(fh);
  MutexLock ml(mutex_);
  if (fobj->err != 0) {
    s = RadosError("rados_bg_io", fobj->err);
    Error(__LOG_ARGS__, s);
  }
  Unref(fobj);
  return s;
}

Status RadosFio::Flush(const Slice& fentry_encoding, Handle* fh,
                       bool force_sync) {
  Status s;
  assert(fh != NULL);
  const RadosFobj* fobj = reinterpret_cast<RadosFobj*>(fh);
  mutex_->Lock();
  if (fobj->err != 0) {
    s = RadosError("rados_bg_io", fobj->err);
  }
  mutex_->Unlock();
  if (s.ok()) {
    // No data is buffered locally so there is no need for an explicit flush,
    // so only sync will be handled
    if (force_sync) {
      // We are actually wait for all async write operations on
      // every open file to finish
      rados_aio_flush(ioctx_);
      MutexLock ml(mutex_);
      if (fobj->err != 0) {
        s = RadosError("rados_bg_io", fobj->err);
        Error(__LOG_ARGS__, s);
      }
    }
  }
  return s;
}

Status RadosFio::Write(const Slice& fentry_encoding, Handle* fh,
                       const Slice& buf) {
  Status s;
  assert(fh != NULL);
  RadosFobj* fobj = reinterpret_cast<RadosFobj*>(fh);
  MutexLock ml(mutex_);
  if (fobj->err != 0) {
    s = RadosError("rados_bg_io", fobj->err);
  } else {
    uint64_t off = fobj->off;
    uint64_t end = off + buf.size();
    if (!force_sync_) {
      fobj->nrefs++;  // IO callback
    }
    mutex_->Unlock();
    std::string oid = ToOid(fentry_encoding);
    if (!force_sync_) {
      rados_completion_t comp;
      rados_aio_create_completion(fobj, NULL, IO_safe, &comp);
      rados_aio_write(ioctx_, oid.c_str(), comp, buf.data(), buf.size(), off);
      rados_aio_release(comp);
    } else {
      int r = rados_write(ioctx_, oid.c_str(), buf.data(), buf.size(), off);
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
  if (fobj->err != 0) {
    s = RadosError("rados_bg_io", fobj->err);
  } else {
    uint64_t end = off + buf.size();
    if (!force_sync_) {
      fobj->nrefs++;  // IO callback
    }
    mutex_->Unlock();
    std::string oid = ToOid(fentry_encoding);
    if (!force_sync_) {
      rados_completion_t comp;
      rados_aio_create_completion(fobj, NULL, IO_safe, &comp);
      rados_aio_write(ioctx_, oid.c_str(), comp, buf.data(), buf.size(), off);
      rados_aio_release(comp);
    } else {
      int r = rados_write(ioctx_, oid.c_str(), buf.data(), buf.size(), off);
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
  if (fobj->err != 0) {
    s = RadosError("rados_bg_io", fobj->err);
  } else {
    uint64_t off = fobj->off;
    mutex_->Unlock();
    std::string oid = ToOid(fentry_encoding);
    int n = rados_read(ioctx_, oid.c_str(), scratch, size, off);
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
  if (fobj->err != 0) {
    s = RadosError("rados_bg_io", fobj->err);
  } else {
    mutex_->Unlock();
    std::string oid = ToOid(fentry_encoding);
    int n = rados_read(ioctx_, oid.c_str(), scratch, size, off);
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

#endif  // RADOS
