/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/env.h"
#include "pdlfs-common/leveldb/db/options.h"
#include "pdlfs-common/port.h"

#include <stddef.h>
#include <stdint.h>

namespace pdlfs {
namespace plfsio {

class BatchCursor;
class EventListener;

struct IoStats {
  IoStats();

  // Total bytes accessed as indexes
  uint64_t index_bytes;
  // Total number of I/O operations for reading or writing indexes
  uint64_t index_ops;
  // Total bytes accessed as data
  uint64_t data_bytes;
  // Total number of I/O operations for reading or writing data
  uint64_t data_ops;
};

// Directory semantics
enum DirMode {
  // Each epoch is structured as a set of ordered multi-maps.
  // Duplicated keys inserted within an epoch
  // are considered separate records
  kDmMultiMap = 0x00,
  // Duplicated keys. Stored out-of-order.
  kDmMultiMapUnordered = 0x10,
  // Unique, un-ordered keys.
  kDmUniqueUnordered = 0x90,
  // Duplicated key insertions are silently discarded
  kDmUniqueDrop = 0x81,
  // No duplicated keys, ensured by users
  kDmUniqueKey = 0x80
};

// Directory filter types. Bitmap-based filters are optimized
// for workloads with compact key spaces.
enum FilterType {
  // Do not use any filters
  kFtNoFilter = 0x00,  // For debugging or benchmarking
  // Use bloom filters
  kFtBloomFilter = 0x01,
  // Use bitmap filters
  kFtBitmap = 0x02
};

// Bitmap compression format.
enum BitmapFormat {
  // Use the uncompressed bitmap format
  kFmtUncompressed = 0x00,
  // Use the roaring bitmap format
  kFmtRoaring = 0x01,
  // Use a modified varint format with a lookup table
  kFmtFastVarintPlus = 0x02,
  // Use a modified varint format
  kFmtVarintPlus = 0x03,
  // Use the original varint format
  kFmtVarint = 0x04,
  // Use p-for-delta with a lookup table
  kFmtFastPfDelta = 0x05,
  // Use p-for-delta
  kFmtPfDelta = 0x06
};

struct DirOptions {
  DirOptions();

  // Total memory reserved for write buffering.
  // This includes both the buffer space for memtables and the buffer space for
  // compaction. This does *NOT* include the buffer space for accumulating
  // small writes to ensure an optimized I/O size.
  // Default: 4MB
  size_t total_memtable_budget;

  // Flush memtable when its size >= memtable_size * memtable_util
  // Default: 0.97 (97%)
  double memtable_util;

  // Reserve memtable_size * memtable_reserv.
  // Default: 1.00 (100%)
  double memtable_reserv;

  // Always use LevelDb compatible block formats.
  // Default: true
  bool leveldb_compatible;

  // Skip sorting memtables.
  // This is useful when the input data is inserted in-order.
  // Default: false
  bool skip_sort;

  // If key value length is fixed.
  // This enables alternate block formats when "leveldb_compatible" is OFF.
  // Default: false
  bool fixed_kv_length;

  // Estimated key size.
  // If not known, keep the default.
  // Default: 8 bytes
  size_t key_size;

  // Estimated value size.
  // If not known, keep the default.
  // Default: 32 bytes
  size_t value_size;

  // Filter type to be applied to directory storage.
  // Default: kFtBloomFilter
  FilterType filter;

  // Number of bits to reserve per key for filter memory.
  // The actual amount of memory, and storage, used for each key in filters
  // may differ from the reserved memory.
  // Set to 0 to avoid pre-reserving memory for filters.
  // Default: 0 bit
  size_t filter_bits_per_key;

  // Bloom filter bits per key.
  // This option is only used when bloom filter is enabled.
  // Set to 0 to disable bloom filters.
  // Default: 8 bits
  size_t bf_bits_per_key;

  // Storage format used to encoding the bitmap filter.
  // This option is only used when bitmap filter is enabled.
  // Default: kFmtUncompressed
  BitmapFormat bm_fmt;

  // Total number of bits in each key.
  // Used to bound the domain size of the key space.
  // This option is only used when bitmap filter is enabled.
  // Default: 24 bits
  size_t bm_key_bits;

  // Approximate size of user data packed per data block.
  // Note that block is used both as the packaging format and as the logical I/O
  // unit for reading and writing the underlying data log objects.
  // The size of all index and filter blocks are *not* affected
  // by this option.
  // Default: 32K
  size_t block_size;

  // Start zero padding if current estimated block size reaches the
  // specified utilization target. Only applies to data blocks.
  // Default: 0.996 (99.6%)
  double block_util;

  // Set to false to disable the zero padding of data blocks.
  // Only relevant to data blocks.
  // Default: true
  bool block_padding;

  // Number of data blocks to accumulate before flush out to the data log in a
  // single atomic batch. Aggregating data block writes can reduce the I/O
  // contention among multiple concurrent compaction threads that compete
  // for access to a shared data log.
  // Default: 2MB
  size_t block_batch_size;

  // Write buffer size for each physical data log.
  // Set to zero to disable buffering and each data block flush
  // becomes an actual physical write to the underlying storage.
  // Default: 4MB
  size_t data_buffer;

  // Minimum write size for each physical data log.
  // Default: 4MB
  size_t min_data_buffer;

  // Write buffer size for each physical index log.
  // Set to zero to disable buffering and each index block flush
  // becomes an actual physical write to the underlying storage.
  // Default: 4MB
  size_t index_buffer;

  // Minimum write size for each physical index log.
  // Default: 4MB
  size_t min_index_buffer;

  // Auto rotate log files at the end of each epoch.
  // Only data logs are rotated.
  // Default: false
  bool epoch_log_rotation;

  // Add necessary padding to the end of each log object to ensure the
  // final object size is always some multiple of the write size.
  // Required by some underlying object stores.
  // Default: false
  bool tail_padding;

  // Thread pool used to run concurrent background compaction jobs.
  // If set to NULL, Env::Default() may be used to schedule jobs if permitted.
  // Otherwise, the caller's thread context will be used directly to serve
  // compactions.
  // Default: NULL
  ThreadPool* compaction_pool;

  // Thread pool used to run concurrent background reads.
  // If set to NULL, Env::Default() may be used to schedule reads if permitted.
  // Otherwise, the caller's thread context will be used directly.
  // Consider setting parallel_reads to true to take full advantage
  // of this thread pool.
  // Default: NULL
  ThreadPool* reader_pool;

  // Number of bytes to read when loading the indexes.
  // Default: 8MB
  size_t read_size;

  // Set to true to enable parallel reading across different epochs.
  // Otherwise, reads progress serially over all epochs.
  // Default: false
  bool parallel_reads;

  // True if write operations should be performed in a non-blocking manner,
  // in which case a special status is returned instead of blocking the
  // writer to wait for buffer space.
  // Default: false
  bool non_blocking;

  // Number of microseconds to slowdown if a writer cannot make progress
  // because the system has run out of its buffer space.
  // Default: 0
  uint64_t slowdown_micros;

  // If true, the implementation will do aggressive checking of the
  // data it is processing and will stop early if it detects any
  // errors.
  // Default: false
  bool paranoid_checks;

  // Ignore all filters during reads.
  // Default: false
  bool ignore_filters;

  // Compression type to be applied to index blocks.
  // Data blocks are never compressed.
  // Default: kNoCompression
  CompressionType compression;

  // True if compressed data is written out even if compression rate is low.
  // Default: false
  bool force_compression;

  // True if all data read from underlying storage will be verified
  // against the corresponding checksums stored.
  // Default: false
  bool verify_checksums;

  // True if checksum calculation and verification are completely skipped.
  // Default: false
  bool skip_checksums;

  // True if read I/O should be measured.
  // Default: true
  bool measure_reads;

  // True if write I/O should be measured.
  // Default: true
  bool measure_writes;

  // Number of epochs to read during the read phase.
  // If set to -1, will use the value obtained from the footer.
  // Ignored in the write phase.
  // Default: -1
  int num_epochs;

  // Number of partitions to divide the data during the write phase.
  // Specified in logarithmic number so each x will give 2**x partitions.
  // Number of partitions to read during the read phase.
  // If set to -1 during the read phase, will use
  // the value obtained from the footer.
  // Default: -1
  int lg_parts;  // between [0, 8]

  // User callback for handling background events.
  // Default: NULL
  EventListener* listener;

  // Dir mode
  // Default: kDmUniqueKey
  DirMode mode;

  // Env instance used to access objects or files stored in the underlying
  // storage system. If NULL, Env::Default() will be used.
  // Default: NULL
  Env* env;

  // If the env context may be used to run background jobs.
  // Default: false
  bool allow_env_threads;

  // True if the underlying storage is implemented as a parallel
  // file system rather than an object storage.
  // Default: true
  bool is_env_pfs;

  // Rank of the process in the directory.
  // Default: 0
  int rank;
};

// Parse a given configuration string to structured options.
extern DirOptions ParseDirOptions(const char* conf);

// Destroy the contents of the specified directory.
// Be very careful using this method.
extern Status DestroyDir(const std::string& dirname, const DirOptions& options);

// Deltafs Plfs Dir Writer
class DirWriter {
 public:
  ~DirWriter();

  // Report the I/O stats for logging the data and the indexes.
  IoStats GetIoStats() const;

  // Return the estimated size of each sst.
  // This is the amount of memory we reserve for each sst.
  // The actual memory, and storage, used by each sst may differ.
  uint64_t TEST_estimated_sstable_size() const;

  // Return the planned size of each filter.
  // This is the amount of memory we reserve for the filter associated with each
  // sst. The actual memory, and storage, used by each filter may differ.
  uint64_t TEST_planned_filter_size() const;

  // Return the total number of keys inserted so far.
  uint32_t TEST_num_keys() const;

  // Return the total number of keys rejected so far.
  uint32_t TEST_num_dropped_keys() const;

  // Return the total number of data blocks generated so far.
  uint32_t TEST_num_data_blocks() const;

  // Return the total number of SSTable generated so far.
  uint32_t TEST_num_sstables() const;

  // Return the aggregated size of all index blocks.
  // Before compression and excluding any padding or checksum bytes.
  uint64_t TEST_raw_index_contents() const;

  // Return the aggregated size of all filter blocks.
  // Before compression and excluding any padding or checksum bytes.
  uint64_t TEST_raw_filter_contents() const;

  // Return the aggregated size of all data blocks.
  // Excluding any padding or checksum bytes.
  uint64_t TEST_raw_data_contents() const;

  // Return the aggregated size of all inserted keys.
  uint64_t TEST_key_bytes() const;

  // Return the aggregated size of all inserted values.
  uint64_t TEST_value_bytes() const;

  // Return the total amount of memory reserved by this directory.
  uint64_t TEST_total_memory_usage() const;

  // Open an I/O writer against a specified plfs-style directory.
  // Return OK on success, or a non-OK status on errors.
  static Status Open(const DirOptions& options, const std::string& dirname,
                     DirWriter** result);

  // Perform a batch of file appends in a single operation.
  // REQUIRES: Finish() has not been called.
  Status Write(BatchCursor* cursor, int epoch = -1);

  // Append a piece of data to a specific file under the directory.
  // Set epoch to -1 to disable epoch validation.
  // REQUIRES: Finish() has not been called.
  Status Append(const Slice& fid, const Slice& data, int epoch = -1);

  // Force a memtable compaction.
  // Set epoch to -1 to disable epoch validation.
  // REQUIRES: Finish() has not been called.
  Status Flush(int epoch = -1);

  // Force a memtable compaction and start a new epoch.
  // Set epoch to -1 to disable epoch validation.
  // REQUIRES: Finish() has not been called.
  Status EpochFlush(int epoch = -1);

  // Wait for all on-going background compactions to finish.
  // Return OK on success, or a non-OK status on errors.
  Status Wait();

  // Force a compaction and finalize all log files.
  // No further write operation is allowed after this call.
  Status Finish();

 private:
  struct Rep;
  Rep* rep_;
  explicit DirWriter(Rep* rep);
  static Status TryOpen(Rep* rep);

  void operator=(const DirWriter& dw);  // No copying allowed
  DirWriter(const DirWriter&);
};

// Deltafs Plfs Dir Reader
class DirReader {
 public:
  DirReader() {}
  virtual ~DirReader();

  // Open an I/O reader against a specific plfs-style directory.
  // Return OK on success, or a non-OK status on errors.
  static Status Open(const DirOptions& options, const std::string& dirname,
                     DirReader** result);

  // Default: fetch all epochs and allow parallel reads
  struct ReadOp {
    ReadOp();
    void SetEpoch(int epoch);
    uint32_t epoch_start;
    uint32_t epoch_end;
    bool no_parallel_reads;
    size_t* table_seeks;
    size_t* seeks;
  };
  // Obtain the value to a specific key stored in a given epoch range.
  // Report operation stats in *table_seeks and *seeks.
  // Return OK on success, or a non-OK status on errors.
  virtual Status Read(const ReadOp& op, const Slice& fid, std::string* dst) = 0;

  // Default: scan all epochs and allow parallel reads
  struct ScanOp {
    ScanOp();
    void SetEpoch(int epoch);
    uint32_t epoch_start;
    uint32_t epoch_end;
    bool no_parallel_reads;
    size_t* table_seeks;
    size_t* seeks;
    size_t* n;
  };
  typedef void (*ScanSaver)(void* arg, const Slice& key, const Slice& value);
  // List all keys stored in a given epoch range.
  // Report operation stats in *table_seeks, *seeks, and *n.
  // Return OK on success, or a non-OK status on errors.
  virtual Status Scan(const ScanOp& op, ScanSaver, void*) = 0;

  // Return the aggregated I/O stats accumulated so far.
  virtual IoStats GetIoStats() const = 0;

 private:
  // No copying allowed
  void operator=(const DirReader&);
  DirReader(const DirReader&);
};

}  // namespace plfsio
}  // namespace pdlfs
