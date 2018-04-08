/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "deltafs_plfsio.h"
#include "deltafs_plfsio_format.h"
#include "deltafs_plfsio_idx.h"
#include "deltafs_plfsio_nio.h"
#include "deltafs_plfsio_recov.h"

#include "pdlfs-common/env_files.h"
#include "pdlfs-common/port.h"

#include <set>
#include <string>
#include <vector>

namespace pdlfs {
namespace plfsio {

// Non-thread-safe append-only in-memory table.
class WriteBuffer {
 public:
  explicit WriteBuffer(const DirOptions& options);
  ~WriteBuffer() {}

  size_t memory_usage() const;  // Report real memory usage
  // Report estimated memory usage per entry
  size_t bytes_per_entry() const;

  void Reserve(size_t bytes_to_reserve);
  size_t CurrentBufferSize() const { return buffer_.size(); }
  uint32_t NumEntries() const { return num_entries_; }
  bool Add(const Slice& key, const Slice& value);
  Iterator* NewIterator() const;
  void Finish(bool skip_sort = false);
  void Reset();

 private:
  struct STLLessThan;
  // Estimated memory usage per entry (including overhead due to varint
  // encoding)
  size_t bytes_per_entry_;

  // Starting offsets of inserted entries
  std::vector<uint32_t> offsets_;
  std::string buffer_;
  uint32_t num_entries_;
  bool finished_;

  // No copying allowed
  void operator=(const WriteBuffer&);
  WriteBuffer(const WriteBuffer&);

  class Iter;
};

// Write sorted table contents into a pair of log files.
class TableLogger {
 public:
  TableLogger(const DirOptions& options, LogSink* data, LogSink* indx);
  ~TableLogger();

  bool ok() const { return status_.ok(); }
  Status status() const { return status_; }

  void Add(const Slice& key, const Slice& value);

  // Force the start of a new table.
  // Caller may optionally specify a corresponding filter block.
  // REQUIRES: Finish() has not been called.
  template <typename T>
  void EndTable(T* filter, ChunkType filter_type);

  // Force the start of a new epoch.
  // REQUIRES: Finish() has not been called.
  void MakeEpoch();

  // Finalize table contents.
  // No further writes.
  Status Finish();

 private:
  // End the current block and force the start of a new data block.
  // REQUIRES: Finish() has not been called.
  void EndBlock();

  // Flush buffered data blocks and finalize their indexes.
  // REQUIRES: Finish() has not been called.
  void Commit();

  const DirOptions& options_;
  DirOutputStats output_stats_;
#ifndef NDEBUG
  // Used to verify the uniqueness of all input keys
  std::set<std::string> keys_;
#endif
  template <typename T>
  friend class DirBuilder;

  // No copying allowed
  void operator=(const TableLogger&);
  TableLogger(const TableLogger&);

  Status status_;
  std::string smallest_key_;
  std::string largest_key_;
  std::string last_key_;
  uint32_t num_uncommitted_indx_;  // Number of uncommitted index entries
  uint32_t num_uncommitted_data_;  // Number of uncommitted data blocks
  bool pending_restart_;           // Request to restart the data block buffer
  bool pending_commit_;  // Request to commit buffered data and indexes
  BlockBuilder data_block_;
  BlockBuilder indx_block_;  // Locate the data blocks within a table
  BlockBuilder meta_block_;  // Locate the tables within an epoch
  BlockBuilder root_block_;  // Locate each epoch
  bool pending_indx_entry_;
  BlockHandle pending_indx_handle_;
  bool pending_meta_entry_;
  TableHandle pending_meta_handle_;
  bool pending_root_entry_;
  BlockHandle pending_root_handle_;
  uint32_t total_num_keys_;
  uint32_t total_num_dropped_keys_;
  uint32_t total_num_blocks_;
  uint32_t total_num_tables_;
  uint32_t num_tables_;  // Number of tables generated within the current epoch
  uint32_t num_epochs_;  // Number of epochs generated
  std::string uncommitted_indexes_;
  uint64_t pending_data_flush_;  // Offset of the data pending flush
  uint64_t pending_indx_flush_;  // Offset of the index pending flush
  LogSink* data_sink_;
  uint64_t data_offset_;  // Latest data offset
  LogWriter indx_logger_;
  LogSink* indx_sink_;
  bool finished_;
};

class BloomBlock;

// Sequentially format and write data as multiple sorted runs
// of indexed tables. Implementation is thread-safe and
// uses background threads.
template <typename T = BloomBlock>
class DirBuilder {
 public:
  DirBuilder(const DirOptions& options, size_t part, port::Mutex* mu,
             port::CondVar* cv);

  Status Open(LogSink* data, LogSink* indx);

  // Report compaction stats
  const DirOutputStats* output_stats() const { return &tb_->output_stats_; }

  uint32_t num_keys() const { return tb_->total_num_keys_; }
  uint32_t num_dropped_keys() const { return tb_->total_num_dropped_keys_; }

  uint32_t num_data_blocks() const { return tb_->total_num_blocks_; }
  uint32_t num_tables() const { return tb_->total_num_tables_; }
  uint32_t num_epochs() const { return tb_->num_epochs_; }

  // Report memory configurations and usage
  size_t estimated_sstable_size() const { return tb_bytes_; }
  size_t planned_filter_size() const { return ft_bytes_; }
  size_t memory_usage() const;  // Report actual memory usage

  // REQUIRES: mutex_ has been locked
  bool has_bg_compaction();
  Status bg_status();  // Return latest compaction status
  // May trigger a new compaction
  Status Add(const Slice& key, const Slice& value);

  // Force a compaction and maybe wait for it
  struct FlushOptions {
    explicit FlushOptions(bool ef = false, bool fi = false)
        : no_wait(true), dry_run(false), epoch_flush(ef), finalize(fi) {}

    // Do not wait for compaction to finish
    // Default: true
    bool no_wait;
    // Status checks only
    // Default: false
    bool dry_run;
    // Force a new epoch
    // Default: false
    bool epoch_flush;
    // Finalize the directory
    // Default: false
    bool finalize;
  };
  Status Flush(const FlushOptions& options);

  // Sync and close all associated log sinks.
  // DOES NOT REQUIRE MUTEX TO BE LOCKED (CALLED IN I/O CONTEXT ONLY)
  Status SyncAndClose();

  void Ref() { refs_++; }

  void Unref() {
    assert(refs_ > 0);
    refs_--;
    if (refs_ == 0) {
      delete this;
    }
  }

 private:
  WritableFileStats io_stats_;
  template <typename TT>
  friend class DirWriterImpl;
  friend class DirWriter;
  ~DirBuilder();

  Status Prepare(bool force = false, bool epoch_flush = false,
                 bool finalize = false);

  // No copying allowed
  void operator=(const DirBuilder&);
  DirBuilder(const DirBuilder&);

  static void BGWork(void*);
  void MaybeScheduleCompaction();
  void CompactMemtable();
  void DoCompaction();

  // Constant after construction
  const DirOptions& options_;
  port::CondVar* const bg_cv_;
  port::Mutex* const mu_;
  size_t ft_bits_;
  size_t ft_bytes_;          // Target bloom filter size
  uint32_t entries_per_tb_;  // Number of entries packed per table
  size_t tb_bytes_;          // Target table size
  size_t part_;              // Partition index

  // State below is protected by mutex_
  uint32_t num_flush_requested_;
  uint32_t num_flush_completed_;
  bool has_bg_compaction_;
  Status bg_status_;
  T* filter_;  // T* to allow distinct types of filters to be used
  WriteBuffer* mem_buf_;
  WriteBuffer* imm_buf_;
  bool imm_buf_is_epoch_flush_;
  bool imm_buf_is_final_;
  WriteBuffer buf0_;
  WriteBuffer buf1_;
  TableLogger* tb_;
  LogSink* data_;
  LogSink* indx_;
  bool opened_;
  int refs_;
};

// Retrieve indexed data from log files.
class Dir {
 public:
  Dir(const DirOptions& options, port::Mutex*, port::CondVar*);

  // Open a directory reader on top of a given directory index partition.
  // Return OK on success, or a non-OK status on errors.
  Status Open(LogSource* indx);

  // Obtain the value to a key within a given epoch range. All value found will
  // be appended to "dst". A caller may optionally provide a temporary buffer
  // for storing fetched block contents. Read stats will be accumulated to
  // "*stats". Return OK on success, or a non-OK status on errors.
  struct ReadOptions {
    ReadOptions();
    bool force_serial_reads;  // Do not fetch data in parallel
    uint32_t epoch_start;
    uint32_t epoch_end;
    // Temporary storage for data blocks
    size_t tmp_length;
    char* tmp;
  };

  struct ReadStats {
    size_t total_table_seeks;  // Total tables touched
    // Total data blocks fetched
    size_t total_seeks;
  };

  Status Read(const ReadOptions& opts, const Slice& key, std::string* dst,
              ReadStats* stats);

  // Iterate through all keys within a given epoch range. A caller may
  // optionally provide a temporary buffer for storing fetched block contents.
  // Read stats will be accumulated to "*stats". Return OK on success, or a
  // non-OK status on errors.
  struct ScanOptions {
    ScanOptions();
    bool force_serial_reads;  // Do not fetch data in parallel
    uint32_t epoch_start;
    uint32_t epoch_end;
    // User callback to handle fetched data
    void* usr_cb;
    void* arg_cb;
    // Temporary storage for data blocks
    size_t tmp_length;
    char* tmp;
  };

  struct ScanStats {
    size_t total_table_seeks;  // Total tables touched
    // Total data blocks fetched
    size_t total_seeks;
    // Total data entries processed
    size_t n;
  };

  Status Scan(const ScanOptions& opts, ScanStats* stats);

  void InstallDataSource(LogSource* data);

  void Ref() { refs_++; }

  void Unref() {
    assert(refs_ > 0);
    refs_--;
    if (refs_ == 0) {
      delete this;
    }
  }

 private:
  SequentialFileStats io_stats_;
  friend class DirReaderImpl;
  friend class DirReader;
  ~Dir();

  typedef void (*Saver)(void* arg, const Slice& key, const Slice& value);

  struct GetStats;
  struct FetchOptions {
    GetStats* stats;
    // Log rotation #
    uint32_t file_index;  // For data log only
    // Scratch space for temporary data block storage
    char* tmp;
    // Scratch size
    size_t tmp_length;
    // Callback for handling fetched data
    Saver saver;
    // Callback argument
    void* arg;
  };

  // Obtain the value to a specific key from a given table data block.
  // If key is found, "opts.saver" will be called. Set *exhausted to true if
  // keys larger than the given one have been seen.
  // NOTE: "opts.saver" may be called multiple times.
  // Return OK on success, or a non-OK status on errors.
  Status Fetch(const FetchOptions& opts, const Slice& key, Slice* input,
               bool* exhausted);

  // Return true if the given key matches a specific filter block.
  bool KeyMayMatch(const Slice& key, const BlockHandle& h);

  // Obtain the value to a specific key from a given table.
  // If key is found, "opts.saver" will be called.
  // NOTE: "opts.saver" may be called multiple times.
  // Return OK on success, or a non-OK status on errors.
  Status Fetch(const FetchOptions& opts, const Slice& key,
               const TableHandle& h);

  // Obtain the value to a specific key within a given directory epoch.
  // GetContext may be shared among multiple concurrent getters.
  // If key is found, value is appended to *ctx->dst.
  // NOTE: a key may appear multiple times within a single epoch.
  // Store an OK status in *ctx->status on success, or a non-OK status on
  // errors.
  struct GetContext {
    Iterator* rt_iter;  // Only used in serial reads
    std::string* dst;
    int num_open_reads;
    std::vector<uint32_t>* offsets;  // Only used during parallel reads
    std::string* buffer;
    Status* status;
    char* tmp;  // Temporary storage for block contents
    size_t tmp_length;
    size_t num_table_seeks;  // Total number of tables touched
    // Total number of data blocks fetched
    size_t num_seeks;
  };
  void Get(const Slice& key, uint32_t epoch, GetContext* ctx);

  struct GetStats {
    size_t table_seeks;  // Total tables touched for a certain epoch
    // Total data blocks fetched for a certain epoch
    size_t seeks;
  };
  Status DoGet(const Slice& key, const BlockHandle& h, uint32_t epoch,
               GetContext* ctx, GetStats* stats);

  // Merge results from concurrent getters.
  static void Merge(GetContext* ctx);

  struct BGGetItem {
    GetContext* ctx;
    uint32_t epoch;
    Slice key;
    Dir* dir;
  };
  static void BGGet(void*);

  struct ListStats;
  struct IterOptions {
    ListStats* stats;
    // Log rotation #
    uint32_t file_index;  // For data log only
    // Scratch space for temporary data block storage
    char* tmp;
    // Scratch size
    size_t tmp_length;
    // Callback for handling fetched data
    Saver saver;
    // Callback argument
    void* arg;
  };

  // Iterate through all keys within a given table data block whose block handle
  // is encoded as *input. Return OK on success, or a non-OK status on errors.
  Status Iter(const IterOptions& opts, Slice* input);

  // Iterate through all keys within a given table.
  // For each key obtained, "opts.saver" will be called to save the results.
  // Return OK on success, or a non-OK status on errors.
  Status Iter(const IterOptions& opts, const TableHandle& h);

  struct ListContext {
    Iterator* rt_iter;  // Only used in serial reads
    void* usr_cb;
    void* arg_cb;
    int num_open_lists;
    Status* status;
    char* tmp;  // Temporary storage for block contents
    size_t tmp_length;
    size_t num_table_seeks;  // Total number of tables touched
    // Total number of data blocks fetched
    size_t num_seeks;
    // Total number of records read
    size_t n;
  };
  void List(uint32_t epoch, ListContext* ctx);

  struct ListStats {
    size_t table_seeks;  // Total tables touched for a certain epoch
    // Total data blocks fetched for a certain epoch
    size_t seeks;
    // Total number of keys read
    size_t n;
  };
  Status DoList(const BlockHandle& h, uint32_t epoch, ListContext* ctx,
                ListStats* stats);

  struct BGListItem {
    ListContext* ctx;
    uint32_t epoch;
    Dir* dir;
  };
  static void BGList(void*);

  // No copying allowed
  void operator=(const Dir&);
  Dir(const Dir&);

  struct STLLessThan;
  // Constant after construction
  const DirOptions& options_;
  uint32_t num_epoches_;
  LogSource* data_;
  LogSource* indx_;

  port::Mutex* mu_;
  port::CondVar* bg_cv_;
  Block* rt_;
  int refs_;
};

}  // namespace plfsio
}  // namespace pdlfs
