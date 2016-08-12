/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/blkdb.h"
#include "pdlfs-common/coding.h"
#include "pdlfs-common/logging.h"
#include "pdlfs-common/mutexlock.h"

namespace pdlfs {

static Slice UntypedKeyPrefix(const Slice& fentry_encoding) {
  return Fentry::ExtractUntypedKeyPrefix(fentry_encoding);
}

bool StreamHeader::DecodeFrom(Slice* input) {
  if (!GetVarint64(input, &mtime) || !GetVarint64(input, &size)) {
    return false;
  } else {
    return true;
  }
}

Slice StreamHeader::EncodeTo(char* scratch) const {
  char* p = scratch;
  p = EncodeVarint64(p, mtime);
  p = EncodeVarint64(p, size);
  return Slice(scratch, p - scratch);
}

BlkDBOptions::BlkDBOptions()
    : uniquefier(0),
      sync(false),
      verify_checksum(false),
      owns_db(false),
      db(NULL) {}

BlkDB::BlkDB(const BlkDBOptions& options)
    : uniquefier_(options.uniquefier),
      sync_(options.sync),
      verify_checksum_(options.verify_checksum),
      owns_db_(options.owns_db),
      db_(options.db) {
  assert(db_ != NULL);
}

BlkDB::~BlkDB() {
  if (owns_db_) {
    delete db_;
  }
}

Status BlkDB::GetInfo(const Slice& fentry, Handle* fh, bool* dirty,
                      uint64_t* mtime, uint64_t* size) {
  Status s;
  assert(fh != NULL);
  const Stream* stream = reinterpret_cast<Stream*>(fh);
  MutexLock ml(&mutex_);
  if (stream->nflus < 0 || stream->nwrites < 0) {
    s = Status::IOError(Slice());
  } else {
    *mtime = stream->mtime;
    *size = stream->size;
    if (stream->nflus >= stream->nwrites) {
      *dirty = false;
    } else {
      *dirty = true;
    }
  }
  return s;
}

Status BlkDB::Creat(const Slice& fentry, Handle** fh) {
  Status s;
  *fh = NULL;

  Key key(UntypedKeyPrefix(fentry));
  key.SetType(kHeaderType);
  char tmp[20];
  StreamHeader header;
  header.mtime = 0;
  header.size = 0;
  Slice header_encoding = header.EncodeTo(tmp);

  WriteOptions options;
  options.sync = sync_;
  key.SetOffset(uniquefier_);
  s = db_->Put(options, key.Encode(), header_encoding);

  if (s.ok()) {
    Stream* stream = new Stream;
    stream->iter = NULL;
    stream->mtime = header.mtime;
    stream->size = header.size;
    stream->nwrites = 0;
    stream->nflus = 0;
    stream->off = 0;

    *fh = reinterpret_cast<Handle*>(stream);
  }

  return s;
}

Status BlkDB::Open(const Slice& fentry, bool create_if_missing,
                   bool truncate_if_exists, uint64_t* mtime, uint64_t* size,
                   Handle** fh) {
  Status s;
  *fh = NULL;
  *mtime = 0;
  *size = 0;

  Key key(UntypedKeyPrefix(fentry));
  key.SetType(kHeaderType);
  Slice key_prefix = key.prefix();
  ReadOptions options;
  options.verify_checksums = verify_checksum_;
  Iterator* iter = db_->NewIterator(options);
  iter->Seek(key_prefix);  // Search header records to see if the stream exists
  StreamHeader header;
  bool found = false;

  for (; s.ok() && iter->Valid(); iter->Next()) {
    Slice k = iter->key();
    if (k.starts_with(key_prefix)) {
      Slice v = iter->value();
      if (header.DecodeFrom(&v)) {
        *mtime = std::max(*mtime, header.mtime);
        *size = std::max(*size, header.size);
      } else {
        s = Status::Corruption("Bad stream info block");
      }
      found = true;
    } else {
      break;
    }
  }

  if (s.ok()) {
    if (found) {
      if (truncate_if_exists) {
        // Truncating a stream involving deleting a whole range of DB
        // records and we currently don't support it.
        s = Status::NotSupported(Slice());
      }
    } else {
      if (!create_if_missing) {
        s = Status::NotFound(Slice());
      }
    }
  }

  if (s.ok()) {
    header.mtime = *mtime;
    header.size = *size;
  }

  if (s.ok()) {
    if (!found) {
      char tmp[20];
      Slice header_encoding = header.EncodeTo(tmp);
      WriteOptions options;
      options.sync = sync_;
      key.SetOffset(uniquefier_);
      s = db_->Put(options, key.Encode(), header_encoding);
    }
  }

  if (s.ok()) {
    Stream* stream = new Stream;
    stream->iter = NULL;
    stream->mtime = header.mtime;
    stream->size = header.size;
    stream->nwrites = 0;
    stream->nflus = 0;
    stream->off = 0;

    if (found) {
      // Reuse cursor position
      stream->iter = iter;
      iter = NULL;
    }

    *fh = reinterpret_cast<Handle*>(stream);
  }

  delete iter;
  return s;
}

Status BlkDB::Drop(const Slice& fentry) {
  // Dropping a stream involving deleting a whole range of DB
  // records and we currently don't support it.
  return Status::NotSupported(Slice());
}

// Commit the latest modification time and size of the stream to the DB,
// but not necessarily to the underlying storage, unless "force_sync" is true.
// Return OK on success. If the stream has not changed since its last
// flush, no action is taken, unless "force_sync" is true.
Status BlkDB::Flush(const Slice& fentry, Handle* fh, bool force_sync) {
  Status s;
  assert(fh != NULL);
  Stream* stream = reinterpret_cast<Stream*>(fh);
  MutexLock ml(&mutex_);
  if (stream->nflus < 0 || stream->nwrites < 0) {
    s = Status::IOError(Slice());
  } else if (force_sync || stream->nflus < stream->nwrites) {
    int32_t nwrites = stream->nwrites;
    int32_t nflus = stream->nflus;
    StreamHeader header;
    header.mtime = stream->mtime;
    header.size = stream->size;
    mutex_.Unlock();
    if (nflus < nwrites) {
      char tmp[20];
      Slice header_encoding = header.EncodeTo(tmp);
      Key key(UntypedKeyPrefix(fentry));
      key.SetType(kHeaderType);
      WriteOptions options;
      options.sync = (force_sync || sync_);
      key.SetOffset(uniquefier_);
      s = db_->Put(options, key.Encode(), header_encoding);
    } else {
      s = db_->SyncWAL();
    }
    mutex_.Lock();
    if (s.ok()) {
      if (nwrites > stream->nflus) {
        stream->nflus = nwrites;
      }
    } else {
      Error(__LOG_ARGS__, s);
      stream->nwrites = -1;
      stream->nflus = -1;
    }
  }
  return s;
}

Status BlkDB::Close(const Slice& fentry, Handle* fh) {
  Status s;
  assert(fh != NULL);
  Stream* stream = reinterpret_cast<Stream*>(fh);
  MutexLock ml(&mutex_);
  while (stream->nflus < stream->nwrites) {
    mutex_.Unlock();
    s = Flush(fentry, fh);
    mutex_.Lock();
    if (!s.ok()) {
      break;
    }
  }
  if (stream->iter != NULL) {
    delete stream->iter;
  }
  delete stream;
  return s;
}

// REQUIRES: mutex_ has been locked.
Status BlkDB::WriteTo(Stream* stream, const Slice& fentry, const Slice& data,
                      uint64_t off) {
  uint64_t end = off + data.size();
  mutex_.Unlock();
  Key key(UntypedKeyPrefix(fentry));
  key.SetType(kDataBlockType);
  key.SetOffset(end - 1);
  WriteOptions options;
  options.sync = sync_;
  Status s = db_->Put(options, key.Encode(), data);
  mutex_.Lock();
  if (s.ok()) {
    stream->nwrites++;
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
  } else {
    Error(__LOG_ARGS__, s);
    stream->nwrites = -1;
    stream->nflus = -1;
  }
  return s;
}

Status BlkDB::Write(const Slice& fentry, Handle* fh, const Slice& data) {
  Status s;
  assert(fh != NULL);
  Stream* stream = reinterpret_cast<Stream*>(fh);
  MutexLock ml(&mutex_);
  if (stream->nflus < 0 || stream->nwrites < 0) {
    s = Status::IOError(Slice());
  } else {
    uint64_t off = stream->off;
    s = WriteTo(stream, fentry, data, off);
    if (s.ok()) {
      stream->off = off + data.size();
    }
  }
  return s;
}

Status BlkDB::Pwrite(const Slice& fentry, Handle* fh, const Slice& data,
                     uint64_t off) {
  Status s;
  assert(fh != NULL);
  Stream* stream = reinterpret_cast<Stream*>(fh);
  MutexLock ml(&mutex_);
  if (stream->nflus < 0 || stream->nwrites < 0) {
    s = Status::IOError(Slice());
  } else {
    s = WriteTo(stream, fentry, data, off);
  }
  return s;
}

namespace {
struct BlkInfo {
  BlkInfo() {}
  bool ParseFrom(const Slice& k, const Slice& v);
  uint64_t size;
  uint64_t off;
};

static bool FetchOffset(const Slice& encoding, uint64_t* result) {
  if (encoding.size() < 8) {
    return false;
  } else {
    memcpy(result, encoding.data(), 8);
    *result = be64toh(*result);
    return true;
  }
}

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
}

// REQUIRES: mutex_ has been locked.
Status BlkDB::ReadFrom(Stream* stream, const Slice& fentry, Slice* result,
                       uint64_t off, uint64_t size, char* scratch) {
  Status s;
  uint64_t flen = stream->size;
  int32_t nwrites = stream->nwrites;
  Iterator* iter = stream->iter;
  stream->iter = NULL;
  mutex_.Unlock();

  if (iter == NULL) {
    ReadOptions options;
    options.verify_checksums = verify_checksum_;
    iter = db_->NewIterator(options);
  }

  if (off < flen) {
    if (off + size > flen) {
      size = flen - off;
    }

    Key key(UntypedKeyPrefix(fentry));
    key.SetType(kDataBlockType);
    Slice key_prefix = key.prefix();
    bool do_seek = true;  // Try reusing existing cursor position if possible
    if (iter->Valid()) {
      Slice k = iter->key();
      if (k.starts_with(key_prefix)) {
        BlkInfo blk;
        k.remove_prefix(key_prefix.size());
        if (blk.ParseFrom(k, iter->value())) {
          if (off >= blk.off) {
            if (off < blk.off + blk.size) {
              do_seek = false;
            } else if (off == blk.off + blk.size) {
              do_seek = false;
              iter->Next();
            }
          }
        }
      }
    }

    if (do_seek) {
      key.SetOffset(off);
      iter->Seek(key.Encode());
    }

    char* p = scratch;
    while (size != 0 && iter->Valid()) {
      BlkInfo blk;
      Slice k = iter->key();
      Slice data = iter->value();
      if (k.starts_with(key_prefix)) {
        k.remove_prefix(key_prefix.size());
        if (!blk.ParseFrom(k, data)) {
          iter->Next();
          continue;  // Skip bad blocks
        } else {
          // OK
        }
      } else {
        break;  // End-of-file
      }

      // Handle file holes
      if (off < blk.off) {
        uint64_t n = std::min(size, blk.off - off);
        memset(p, 0, n);
        size -= n;
        off += n;
        p += n;
      }

      // Read data
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
    if (!iter->Valid()) {
      s = iter->status();
    }
  }
  mutex_.Lock();
  if (s.ok()) {
    if (stream->iter == NULL) {
      stream->iter = iter;
    } else {
      delete iter;
    }
    if (stream->nwrites > nwrites) {
      if (stream->iter != NULL) {
        delete stream->iter;
        stream->iter = NULL;
      }
    }
  } else {
    Error(__LOG_ARGS__, s);
    delete iter;
  }
  return s;
}

Status BlkDB::Read(const Slice& fentry, Handle* fh, Slice* result,
                   uint64_t size, char* scratch) {
  Status s;
  assert(fh != NULL);
  Stream* stream = reinterpret_cast<Stream*>(fh);
  MutexLock ml(&mutex_);
  uint64_t off = stream->off;
  s = ReadFrom(stream, fentry, result, off, size, scratch);
  if (s.ok()) {
    stream->off = off + result->size();
  }
  return s;
}

Status BlkDB::Pread(const Slice& fentry, Handle* fh, Slice* result,
                    uint64_t off, uint64_t size, char* scratch) {
  Status s;
  assert(fh != NULL);
  Stream* stream = reinterpret_cast<Stream*>(fh);
  MutexLock ml(&mutex_);
  s = ReadFrom(stream, fentry, result, off, size, scratch);
  return s;
}

}  // namespace pdlfs
