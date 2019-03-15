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

#include "deltafs_plfsio_types.h"

#include "pdlfs-common/logging.h"
#include "pdlfs-common/strutil.h"

#include <string>
#include <vector>

namespace pdlfs {
namespace plfsio {

IoStats::IoStats() : index_bytes(0), index_ops(0), data_bytes(0), data_ops(0) {}

DirOptions::DirOptions()
    : total_memtable_budget(4 << 20),
      memtable_util(0.97),
      memtable_reserv(1.00),
      leveldb_compatible(true),
      skip_sort(false),
      fixed_kv_length(false),
      key_size(8),
      value_size(32),
      filter(kFtBloomFilter),
      filter_bits_per_key(0),
      bf_bits_per_key(8),
      bm_fmt(kFmtUncompressed),
      bm_key_bits(24),
      cuckoo_seed(301),
      cuckoo_max_moves(500),
      cuckoo_frac(0.95),
      block_size(32 << 10),
      block_util(0.996),
      block_padding(true),
      block_batch_size(2 << 20),
      data_buffer(4 << 20),
      min_data_buffer(4 << 20),
      index_buffer(4 << 20),
      min_index_buffer(4 << 20),
      epoch_log_rotation(false),
      tail_padding(false),
      compaction_pool(NULL),
      reader_pool(NULL),
      read_size(8 << 20),
      parallel_reads(false),
      paranoid_checks(false),
      ignore_filters(false),
      compression(kNoCompression),
      index_compression(kNoCompression),
      force_compression(false),
      verify_checksums(false),
      skip_checksums(false),
      measure_reads(true),
      measure_writes(true),
      num_epochs(-1),
      lg_parts(-1),
      listener(NULL),
      mode(kDmUniqueKey),
      env(NULL),
      allow_env_threads(false),
      is_env_pfs(true),
      rank(0) {}

namespace {

bool ParseBool(const Slice& key, const Slice& value, bool* result) {
  if (!ParsePrettyBool(value, result)) {
    Warn(__LOG_ARGS__, "Unknown dir option: %s=%s, option ignored", key.c_str(),
         value.c_str());
    return false;
  } else {
    return true;
  }
}

bool ParseInteger(const Slice& key, const Slice& value, uint64_t* result) {
  if (!ParsePrettyNumber(value, result)) {
    Warn(__LOG_ARGS__, "Unknown dir option: %s=%s, option ignored", key.c_str(),
         value.c_str());
    return false;
  } else {
    return true;
  }
}

bool ParseBitmapFormat(const Slice& key, const Slice& value,
                       BitmapFormat* result) {
  if (value == "uncompressed") {
    *result = kFmtUncompressed;
    return true;
  } else if (value == "roar") {
    *result = kFmtRoaring;
    return true;
  } else if (value == "fast-vb+") {
    *result = kFmtFastVarintPlus;
    return true;
  } else if (value == "vb+") {
    *result = kFmtVarintPlus;
    return true;
  } else if (value == "vb") {
    *result = kFmtVarint;
    return true;
  } else if (value == "fast-p-f-delta") {
    *result = kFmtFastPfDelta;
    return true;
  } else if (value == "p-f-delta") {
    *result = kFmtPfDelta;
    return true;
  } else {
    Warn(__LOG_ARGS__, "Unknown bitmap format: %s=%s, option ignored",
         key.c_str(), value.c_str());
    return false;
  }
}

bool ParseFilterType(const Slice& key, const Slice& value, FilterType* result) {
  if (value.starts_with("bloom")) {
    *result = kFtBloomFilter;
    return true;
  } else if (value.starts_with("bitmap")) {
    *result = kFtBitmap;
    return true;
  } else {
    Warn(__LOG_ARGS__, "Unknown filter type: %s=%s, option ignored",
         key.c_str(), value.c_str());
    return false;
  }
}

bool ParseCompressionType(const Slice& key, const Slice& value,
                          CompressionType* result) {
  if (value.starts_with("snappy")) {
    *result = kSnappyCompression;
    return true;
  } else if (value.starts_with("no")) {
    *result = kNoCompression;
    return true;
  } else {
    Warn(__LOG_ARGS__, "Unknown compression type: %s=%s, option ignored",
         key.c_str(), value.c_str());
    return false;
  }
}

}  // namespace

DirOptions ParseDirOptions(const char* input) {
  DirOptions result;
  std::vector<std::string> conf_segments;
  size_t n = SplitString(&conf_segments, input, '&');  // k1=v1 & k2=v2 & k3=v3
  std::vector<std::string> conf_pair;
  for (size_t i = 0; i < n; i++) {
    conf_pair.resize(0);
    SplitString(&conf_pair, conf_segments[i].c_str(), '=', 1);
    if (conf_pair.size() != 2) {
      continue;
    }
    FilterType filter_type;
    BitmapFormat bm_fmt;
    CompressionType compression_type;
    Slice conf_key = conf_pair[0];
    Slice conf_value = conf_pair[1];
    uint64_t num;
    bool flag;
    if (conf_key == "lg_parts") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.lg_parts = int(num);
      }
    } else if (conf_key == "num_epochs") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.num_epochs = int(num);
      }
    } else if (conf_key == "rank") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.rank = int(num);
      }
    } else if (conf_key == "memtable_size") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.total_memtable_budget = num;
      }
    } else if (conf_key == "total_memtable_budget") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.total_memtable_budget = num;
      }
    } else if (conf_key == "compaction_buffer") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.block_batch_size = num;
      }
    } else if (conf_key == "data_buffer") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.data_buffer = num;
      }
    } else if (conf_key == "min_data_buffer") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.min_data_buffer = num;
      }
    } else if (conf_key == "index_buffer") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.index_buffer = num;
      }
    } else if (conf_key == "min_index_buffer") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.min_index_buffer = num;
      }
    } else if (conf_key == "block_size") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.block_size = num;
      }
    } else if (conf_key == "block_padding") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.block_padding = flag;
      }
    } else if (conf_key == "tail_padding") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.tail_padding = flag;
      }
    } else if (conf_key == "verify_checksums") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.verify_checksums = flag;
      }
    } else if (conf_key == "skip_checksums") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.skip_checksums = flag;
      }
    } else if (conf_key == "skip_sort") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.skip_sort = flag;
      }
    } else if (conf_key == "parallel_reads") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.parallel_reads = flag;
      }
    } else if (conf_key == "paranoid_checks") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.paranoid_checks = flag;
      }
    } else if (conf_key == "epoch_log_rotation") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.epoch_log_rotation = flag;
      }
    } else if (conf_key == "ignore_filters") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.ignore_filters = flag;
      }
    } else if (conf_key == "fixed_kv") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.fixed_kv_length = flag;
      }
    } else if (conf_key == "leveldb_compatible") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.leveldb_compatible = flag;
      }
    } else if (conf_key == "filter") {
      if (ParseFilterType(conf_key, conf_value, &filter_type)) {
        result.filter = filter_type;
      }
    } else if (conf_key == "filter_bits_per_key") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.filter_bits_per_key = num;
      }
    } else if (conf_key == "bf_bits_per_key") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.bf_bits_per_key = num;
      }
    } else if (conf_key == "bm_fmt") {
      if (ParseBitmapFormat(conf_key, conf_value, &bm_fmt)) {
        result.bm_fmt = bm_fmt;
      }
    } else if (conf_key == "bm_key_bits") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.bm_key_bits = num;
      }
    } else if (conf_key == "compression") {
      if (ParseCompressionType(conf_key, conf_value, &compression_type)) {
        result.compression = compression_type;
      }
    } else if (conf_key == "index_compression") {
      if (ParseCompressionType(conf_key, conf_value, &compression_type)) {
        result.index_compression = compression_type;
      }
    } else if (conf_key == "force_compression") {
      if (ParseBool(conf_key, conf_value, &flag)) {
        result.force_compression = flag;
      }
    } else if (conf_key == "value_size") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.value_size = num;
      }
    } else if (conf_key == "key_size") {
      if (ParseInteger(conf_key, conf_value, &num)) {
        result.key_size = num;
      }
    } else {
      Warn(__LOG_ARGS__, "Unknown option key: %s, option ignored",
           conf_key.c_str());
    }
  }

  return result;
}

}  // namespace plfsio
}  // namespace pdlfs
