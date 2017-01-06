#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "db_impl.h"

#include "pdlfs-common/leveldb/db/columnar_db.h"

namespace pdlfs {

class Column {
 protected:
  typedef DBOptions Options;

 public:
  enum RecoverMethod { kCommit, kRollback };
  static Status Open(ColumnStyle style, RecoverMethod method,
                     const Options& options, const std::string& name,
                     Column** result);

  void Ref() { refs_++; }

  void Unref() {
    assert(refs_ > 0);
    refs_--;
    if (refs_ == 0) {
      delete this;
    }
  }

  virtual Status WriteTableStart() = 0;
  virtual Status WriteTable(Iterator* contents) = 0;
  virtual Status WriteTableEnd() = 0;

  // The following calls ignore snapshots specified in the read options
  virtual Iterator* NewInternalIterator(const ReadOptions& options) = 0;
  virtual Status Get(const ReadOptions& options, const LookupKey& lkey,
                     Buffer* result) = 0;

  // Handle db version transactions
  virtual Status PreRecover(RecoverMethod method) = 0;
  virtual Status Recover() = 0;

 protected:
  Column() : refs_(0) {}
  virtual ~Column();

  int32_t refs_;
};

class ColumnImpl : public Column {
 public:
  ColumnImpl(DBImpl* db) : db_(db) {}
  virtual ~ColumnImpl();

  virtual Status WriteTableStart();
  virtual Status WriteTable(Iterator* contents);
  virtual Status WriteTableEnd();

  virtual Iterator* NewInternalIterator(const ReadOptions& options);
  virtual Status Get(const ReadOptions& options, const LookupKey& lkey,
                     Buffer* result);

  virtual Status PreRecover(RecoverMethod method);
  virtual Status Recover();

 private:
  DBImpl* db_;
};

class ColumnarDBImpl : public DBImpl {
 public:
  ColumnarDBImpl(const Options& options, const std::string& dbname);
  virtual ~ColumnarDBImpl();

  Status ColumnarGet(const ReadOptions& options, const Slice& key,
                     Buffer* result);
  Iterator* NewColumnarIterator(const ReadOptions& options, const Slice& pivot);
  Iterator* NewColumnarInternalIterator(const ReadOptions& options,
                                        const Slice& pivot,
                                        SequenceNumber* latest_snapshot,
                                        uint32_t* seed);

 protected:
  friend class ColumnarDB;

  Status PreRecover(Column::RecoverMethod*);

  virtual void CompactMemTable();
  virtual Status RecoverLogFile(uint64_t log_number, VersionEdit* edit,
                                SequenceNumber* max_sequence);

  Status BeginMemTableCompaction();
  Status WriteToColumn(MemTable* mem, size_t column_index);
  Status FinishMemTableCompaction();

  Column* PickColumn(const Slice& key);

  const ColumnSelector* selector_;
  std::vector<Column*> columns_;
};

class ColumnarDBWrapper : public ColumnarDB {
 public:
  ColumnarDBWrapper(ColumnarDBImpl* impl) : impl_(impl) {}
  virtual ~ColumnarDBWrapper();

  // Implementations of the DB interface
  virtual Status SyncWAL() { return impl_->SyncWAL(); }
  virtual Status FlushMemTable(const FlushOptions&);
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions&, WriteBatch* updates);
  virtual Status Get(const ReadOptions&, const Slice& key, std::string* value);
  virtual Status Get(const ReadOptions&, const Slice& key, Slice* value,
                     char* scratch, size_t scratch_size);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);

  // Extra methods that are not in the public DB interface

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

 private:
  ColumnarDBImpl* impl_;
};

}  // namespace pdlfs
