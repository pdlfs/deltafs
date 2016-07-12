/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "blkdb.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

bool StreamInfo::DecodeFrom(Slice* input) {
  if (!GetVarint64(input, &mtime) || !GetVarint64(input, &size)) {
    return false;
  } else {
    return true;
  }
}

Slice StreamInfo::EncodeTo(char* scratch) const {
  char* p = scratch;
  p = EncodeVarint64(p, mtime);
  p = EncodeVarint64(p, size);
  return Slice(scratch, p - scratch);
}

BlkDBOptions::BlkDBOptions()
    : cli_id(0), verify_checksum(false), sync(false), db(NULL) {}

BlkDB::~BlkDB() {
  // empty
}

// REQUIRES: mutex_ has been locked.
void BlkDB::Remove(Stream* s) {
  s->next->prev = s->prev;
  s->prev->next = s->next;
}

// REQUIRES: mutex_ has been locked.
void BlkDB::Append(Stream* s) {
  // Make "e" newest entry by inserting just before head_
  s->next = &head_;
  s->prev = head_.prev;
  s->prev->next = s;
  s->next->prev = s;
}

static bool FetchOffset(const Slice& encoding, uint64_t* result) {
  if (encoding.size() < 8) {
    return false;
  } else {
    memcpy(result, encoding.data(), 8);
    *result = be64toh(*result);
    return true;
  }
}

struct BlkInfo {
  bool ParseFrom(const Slice& k, const Slice& v);
  BlkInfo() {}
  uint64_t size;
  uint64_t off;
};

bool BlkInfo::ParseFrom(const Slice& k, const Slice& v) {
  uint64_t end;
  if (!FetchOffset(k, &end)) {
    return false;
  } else {
    size = v.size();
    off = end - size + 1;
    return true;
  }
}

Status BlkDB::Open(const DirId& pid, const Slice& nhash, const Stat& stat,
                   bool create_if_missing, bool error_if_exists,
                   Stream** result) {
  Status s;
  Key key(stat, kDataDesType);
  Slice prefix = key.prefix();
  ReadOptions options;
  options.verify_checksums = verify_checksum_;
  Iterator* iter = db_->NewIterator(options);
  iter->Seek(prefix);
  *result = NULL;

  StreamInfo info;
  bool found = false;
  uint64_t mtime = 0;
  uint64_t size = 0;
  for (; s.ok() && iter->Valid(); iter->Next()) {
    Slice k = iter->key();
    if (k.starts_with(prefix)) {
      Slice v = iter->value();
      if (info.DecodeFrom(&v)) {
        mtime = std::max(mtime, info.mtime);
        size = std::max(size, info.size);
      } else {
        s = Status::Corruption(Slice());
      }
      found = true;
    } else {
      break;
    }
  }

  if (s.ok()) {
    if (found) {
      if (error_if_exists) {
        s = Status::AlreadyExists(Slice());
      }
    } else {
      if (!create_if_missing) {
        s = Status::NotFound(Slice());
      }
    }
  }

  if (s.ok()) {
    info.mtime = mtime;
    info.size = size;
  }

  if (s.ok()) {
    if (!found) {
      char tmp[20];
      Slice encoding = info.EncodeTo(tmp);
      WriteOptions options;
      options.sync = sync_;
      key.SetOffset(cli_id_);
      s = db_->Put(options, key.Encode(), encoding);
    }
  }

  if (s.ok()) {
    Key k(prefix);
    k.SetType(kDataBlockType);
    Slice p = k.prefix();
    Stream* stream = static_cast<Stream*>(malloc(sizeof(Stream) + p.size()));
    stream->prefix_rep[0] = static_cast<unsigned char>(p.size());
    memcpy(stream->prefix_rep + 1, p.data(), p.size());
    assert(stream->prefix() == p);
    stream->pid = pid;
    assert(nhash.size() == 8);
    memcpy(stream->nhash, nhash.data(), nhash.size());
    if (found) {
      stream->iter = iter;
      iter = NULL;
    } else {
      stream->iter = NULL;
    }
    stream->mtime = info.mtime;
    stream->size = info.size;
    MutexLock ml(&mutex_);
    Append(stream);
    *result = stream;
  }

  delete iter;
  return s;
}

Status BlkDB::Sync(Stream* stream) {
  Status s;
  assert(stream != NULL);
  StreamInfo info;
  info.mtime = stream->mtime;
  info.size = stream->size;
  char tmp[20];
  Slice encoding = info.EncodeTo(tmp);
  Key key(stream->prefix());
  key.SetType(kDataDesType);
  key.SetOffset(cli_id_);
  WriteOptions options;
  options.sync = true;
  s = db_->Put(options, key.Encode(), encoding);
  return s;
}

// REQUIRES: Sync(...) has been called against the stream.
Status BlkDB::Close(Stream* stream) {
  Status s;
  MutexLock ml(&mutex_);
  Remove(stream);
  if (stream->iter != NULL) {
    delete stream->iter;
  }
  free(stream);
  return s;
}

Status BlkDB::Pwrite(Stream* stream, const Slice& data, uint64_t off) {
  Status s;
  assert(stream != NULL);
  WriteOptions options;
  options.sync = sync_;
  Key key(stream->prefix());
  assert(key.type() == kDataBlockType);
  uint64_t end = off + data.size();
  key.SetOffset(end - 1);
  s = db_->Put(options, key.Encode(), data);
  if (s.ok()) {
    MutexLock ml(&mutex_);
    if (stream->iter != NULL) {
      delete stream->iter;
      stream->iter = NULL;
    }
    uint64_t mtime = Env::Default()->NowMicros();
    if (mtime > stream->mtime) {
      stream->mtime = mtime;
    }
    if (end > stream->size) {
      stream->size = end;
    }
  }
  return s;
}

Status BlkDB::Pread(Stream* stream, Slice* result, uint64_t off, uint64_t size,
                    char* scratch) {
  Status s;
  assert(stream != NULL);
  MutexLock ml(&mutex_);
  Iterator* iter = stream->iter;
  stream->iter = NULL;
  mutex_.Unlock();
  if (iter == NULL) {
    ReadOptions options;
    options.verify_checksums = verify_checksum_;
    iter = db_->NewIterator(options);
  }
  *result = Slice();
  Slice prefix = stream->prefix();
  if (off < stream->size) {
    if (off + size > stream->size) {
      size = stream->size - off;
    }

    bool need_reseek = true;
    if (iter->Valid()) {
      Slice k = iter->key();
      if (k.starts_with(prefix)) {
        k.remove_prefix(prefix.size());
        BlkInfo blk;
        if (blk.ParseFrom(k, iter->value())) {
          if (off >= blk.off) {
            if (off < blk.off + blk.size) {
              need_reseek = false;
            } else if (off == blk.off + blk.size) {
              need_reseek = false;
              iter->Next();
            }
          }
        }
      }
    }
    if (need_reseek) {
      Key key(prefix);
      assert(key.type() == kDataBlockType);
      key.SetOffset(off);
      iter->Seek(key.Encode());
    }

    char* p = scratch;
    while (size != 0 && iter->Valid()) {
      BlkInfo blk;
      Slice k = iter->key();
      Slice data = iter->value();
      if (!k.starts_with(prefix)) break;  // End-of-file
      k.remove_prefix(prefix.size());
      if (!blk.ParseFrom(k, data)) {
        iter->Next();
        continue;  // Skip non-known blocks
      }

      if (off < blk.off) {
        uint64_t n = std::min(size, blk.off - off);
        memset(p, 0, n);
        size -= n;
        off += n;
        p += n;
      }

      if (size != 0) {
        if (off < blk.off + blk.size) {
          uint64_t n = std::min(size, blk.off + blk.size - off);
          memcpy(p, data.data() + off - blk.off, n);
          size -= n;
          off += n;
          p += n;
        }
      }

      if (size != 0) {
        iter->Next();
      }
    }
    *result = Slice(scratch, p - scratch);
  }
  mutex_.Lock();
  if (stream->iter == NULL) {
    stream->iter = iter;
  } else {
    delete iter;
  }
  return s;
}

}  // namespce pdlfs
