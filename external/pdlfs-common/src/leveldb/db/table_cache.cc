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

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */
#include "table_cache.h"

#include "pdlfs-common/leveldb/filenames.h"
#include "pdlfs-common/leveldb/options.h"
#include "pdlfs-common/leveldb/table.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/env_files.h"

namespace pdlfs {
namespace {
struct TableAndFile {
  SequenceOff off;
  RandomAccessFile* file;
  Table* table;
};
}  // namespace

TableCache::TableCache(const std::string& dbname, const Options* options,
                       Cache* cache)
    : env_(options->env), dbname_(dbname), options_(options), cache_(cache) {
  id_ = cache_->NewId();
}

TableCache::~TableCache() {}

Status TableCache::OpenTable(uint64_t file_number, uint64_t file_size,
                             Table** table, RandomAccessFile** file,
                             bool prefetch) {
  Status s;
  std::string fname = TableFileName(dbname_, file_number);
  if (!prefetch) {
    s = env_->NewRandomAccessFile(fname.c_str(), file);
  } else {
    SequentialFile* base;
    s = env_->NewSequentialFile(fname.c_str(), &base);
    if (s.ok()) {
      WholeFileBufferedRandomAccessFile* f =
          new WholeFileBufferedRandomAccessFile(base, file_size,
                                                options_->table_bulk_read_size);
      s = f->Load();
      if (s.ok()) {
        *file = f;
      } else {
        delete f;
      }
    }
  }

  if (s.ok()) {
    s = Table::Open(*options_, *file, file_size, table);
    if (!s.ok()) {
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
      delete *file;
    }
  }

  if (!s.ok()) {
    Log(options_->info_log, 0, "Error opening table #%llu: %s",
        static_cast<unsigned long long>(file_number), s.ToString().c_str());
  } else if (prefetch) {
#if VERBOSE >= 2
    Log(options_->info_log, 2, "Read table #%llu from storage => %llu bytes",
        static_cast<unsigned long long>(file_number),
        static_cast<unsigned long long>(file_size));
#endif
  } else {
#if VERBOSE >= 5
    Log(options_->info_log, 5, "Opened table #%llu",
        static_cast<unsigned long long>(file_number));
#endif
  }
  return s;
}

namespace {
void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}
}  // namespace

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             SequenceOff seq_off, Cache::Handle** handle) {
  Status s;
  char buf[16];
  EncodeFixed64(buf, id_);
  EncodeFixed64(buf + 8, file_number);
  Slice key(buf, 16);

  *handle = cache_->Lookup(key);
  if (*handle == NULL) {
    // Load table from storage
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = OpenTable(file_number, file_size, &table, &file, false);
    if (s.ok()) {
      TableAndFile* tf = new TableAndFile;
      tf->off = seq_off;
      tf->file = file;
      tf->table = table;

      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  } else {
    // Fetch table from cache
    TableAndFile* const tf =
        reinterpret_cast<TableAndFile*>(cache_->Value(*handle));
    if (tf->off != seq_off) {
      if (tf->off == 0) {
        // Apply the given offset to this table.
        tf->off = seq_off;
      } else {
        s = Status::Corruption("Changing table sequence number offset");
        cache_->Release(*handle);
        *handle = NULL;
      }
    }
  }

  return s;
}

namespace {
// A helper class that applies an offset to the sequence numbers of all the
// internal keys that it sees.
class SequenceOffsetter : public Iterator {
 public:
  explicit SequenceOffsetter(SequenceOff offset, Iterator* iter)
      : offset_(offset), iter_(iter) {
    key_.reserve(64);
  }

  virtual ~SequenceOffsetter() { delete iter_; }

  virtual bool Valid() const { return status_.ok() && iter_->Valid(); }

  virtual void Seek(const Slice& target) {
    ParsedInternalKey parsed;
    if (!ParseInternalKey(target, &parsed)) {
      status_ = Status::Corruption("Malformed internal key");
    } else {
      // Don't apply offset when a seek wants to see all the versions
      if (parsed.sequence < kMaxSequenceNumber) {
        if (offset_ > 0 && parsed.sequence < offset_) {
          parsed.sequence = 0;
        } else {
          parsed.sequence -= offset_;
        }
        assert(parsed.sequence <= kMaxSequenceNumber);
      }
      key_.clear();
      AppendInternalKey(&key_, parsed);
      iter_->Seek(key_);
      Update();
    }
  }

  virtual void SeekToFirst() {
    iter_->SeekToFirst();
    Update();
  }

  virtual void SeekToLast() {
    iter_->SeekToLast();
    Update();
  }

  virtual void Next() {
    iter_->Next();
    Update();
  }

  virtual void Prev() {
    iter_->Prev();
    Update();
  }

  virtual Slice key() const {
    assert(Valid());
    return key_;
  }

  virtual Slice value() const {
    assert(Valid());
    return iter_->value();
  }

  virtual Status status() const {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

 private:
  void Update() {
    key_.clear();
    if (Valid()) {
      ParsedInternalKey parsed;
      if (!ParseInternalKey(iter_->key(), &parsed)) {
        status_ = Status::Corruption("Malformed internal key");
      } else {
        parsed.sequence += offset_;
        assert(parsed.sequence <= kMaxSequenceNumber);
        AppendInternalKey(&key_, parsed);
      }
    }
  }

  Status status_;
  std::string key_;
  const SequenceOff offset_;
  Iterator* const iter_;
};

void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}
}  // namespace

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  SequenceOff seq_off, Table** tableptr) {
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, seq_off, &handle);
  if (!s.ok()) {
    if (tableptr != NULL) {
      *tableptr = NULL;
    }
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (seq_off != 0) {
    result = new SequenceOffsetter(seq_off, result);
  }
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

namespace {
// Delete the table and the file underlying an iterator.
void DeleteTableAndFile(void* arg1, void* arg2) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(arg1);
  delete tf->table;
  delete tf->file;
  delete tf;
}
}  // namespace

Iterator* TableCache::NewDirectIterator(const ReadOptions& options,
                                        bool prefetch_table,
                                        uint64_t file_number,
                                        uint64_t file_size, SequenceOff seq_off,
                                        Table** tableptr) {
  RandomAccessFile* file = NULL;
  Table* table = NULL;
  Status s = OpenTable(file_number, file_size, &table, &file, prefetch_table);
  if (!s.ok()) {
    if (tableptr != NULL) {
      *tableptr = NULL;
    }
    return NewErrorIterator(s);
  }

  TableAndFile* tf = new TableAndFile;
  tf->off = seq_off;
  tf->table = table;
  tf->file = file;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&DeleteTableAndFile, tf, NULL);
  if (seq_off != 0) {
    result = new SequenceOffsetter(seq_off, result);
  }
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

namespace {

typedef void (*Saver)(void*, const Slice& K, const Slice& V);

struct Wrapper {
  SequenceOff off;
  Saver saver;
  void* arg;
};

void ApplyOffset(void* arg, const Slice& key, const Slice& value) {
  Wrapper* wp = reinterpret_cast<Wrapper*>(arg);
  char sp[64];
  std::string buf;
  Slice k;

  ParsedInternalKey parsed;
  if (ParseInternalKey(key, &parsed)) {
    // Applies the offset.
    parsed.sequence += wp->off;
    assert(parsed.sequence <= kMaxSequenceNumber);

    // Prefer to use the static buffer if possible.
    // Use heap space for large keys which we don't expect to see frequently.
    if (key.size() <= sizeof(sp)) {
      k = AppendInternalKeyPtr(sp, parsed);
    } else {
      AppendInternalKey(&buf, parsed);
      k = buf;
    }
  }

  // k maybe empty, which indicates an error.
  (*wp->saver)(wp->arg, k, value);
}
}  // namespace

Status TableCache::Get(const ReadOptions& options, uint64_t fnum,
                       uint64_t fsize, SequenceOff off, const Slice& key,
                       void* arg, Saver saver) {
  Cache::Handle* handle;
  Status s = FindTable(fnum, fsize, off, &handle);
  if (!s.ok()) {
    return s;
  }

  Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  if (off == 0) {
    s = t->InternalGet(options, key, arg, saver);
    cache_->Release(handle);
    return s;
  }

  Slice _key = key;
  void* _arg = arg;
  Saver _saver = saver;
  char sp[64];
  std::string buf;
  Wrapper wp;
  ParsedInternalKey parsed;
  if (!ParseInternalKey(key, &parsed)) {
    s = Status::Corruption("Malformed internal key");
  } else {
    wp.arg = arg;
    wp.saver = saver;
    wp.off = off;

    // Don't apply offset when the get wants to see all the versions
    if (parsed.sequence != kMaxSequenceNumber) {
      if (off > 0 && parsed.sequence < off) {
        parsed.sequence = 0;
      } else {
        parsed.sequence -= off;
      }

      assert(parsed.sequence <= kMaxSequenceNumber);
      // Prefer to use the static buffer if possible. Use heap space for large
      // keys which we don't expect to see frequently.
      if (key.size() <= sizeof(sp)) {
        _key = AppendInternalKeyPtr(sp, parsed);
      } else {
        AppendInternalKey(&buf, parsed);
        _key = buf;
      }
    }

    _arg = &wp;
    _saver = ApplyOffset;
  }

  if (s.ok()) {
    s = t->InternalGet(options, _key, _arg, _saver);
  }
  cache_->Release(handle);
  return s;
}

void TableCache::Evict(uint64_t fnum) {
  char buf[16];
  EncodeFixed64(buf, id_);
  EncodeFixed64(buf + 8, fnum);
  Slice key(buf, 16);
  cache_->Erase(key);
}

}  // namespace pdlfs
