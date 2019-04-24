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

#include "format.h"

#include <stdio.h>
#include <string.h>

namespace pdlfs {
namespace plfsio {

std::string DirInfoFileName(const std::string& dirname) {
  return dirname + "/DIR.info";
}

std::string DirModeName(DirMode mode) {
  switch (mode) {
    case kDmMultiMap:
      return "M/M";
    case kDmMultiMapUnordered:
      return "M/U";
    case kDmUniqueUnordered:
      return "U/U";
    case kDmUniqueDrop:
      return "U/D";
    case kDmUniqueKey:
      return "U/K";
    default:
      return "Unknown";
  }
}

Status ParseEpochKey(const Slice& input, uint32_t* epoch, uint32_t* table) {
  int parsed_epoch;
  int parsed_table;
  int r = sscanf(input.data(), "%04d-%04d", &parsed_epoch, &parsed_table);
  if (r != 2) {
    return Status::Corruption("Bad epoch key");
  } else {
    *epoch = static_cast<unsigned>(parsed_epoch);
    *table = static_cast<unsigned>(parsed_table);
    return Status::OK();
  }
}

std::string EpochTableKey(uint32_t epoch, uint32_t table) {
  assert(epoch <= kMaxEpochNo);
  assert(table <= kMaxTableNo);
  char tmp[10];
  snprintf(tmp, sizeof(tmp), "%04d-%04d", int(epoch), int(table));
  return tmp;
}

std::string EpochKey(uint32_t epoch) {
  assert(epoch <= kMaxEpochNo);
  char tmp[5];
  snprintf(tmp, sizeof(tmp), "%04d", int(epoch));
  return tmp;
}

void TableHandle::EncodeTo(std::string* dst) const {
  assert(filter_offset_ != ~static_cast<uint64_t>(0));
  assert(filter_size_ != ~static_cast<uint64_t>(0));
  assert(index_offset_ != ~static_cast<uint64_t>(0));
  assert(index_size_ != ~static_cast<uint64_t>(0));
  assert(!smallest_key_.empty());
  assert(!largest_key_.empty());

  PutLengthPrefixedSlice(dst, smallest_key_);
  PutLengthPrefixedSlice(dst, largest_key_);
  PutVarint64(dst, filter_offset_);
  PutVarint64(dst, filter_size_);
  PutVarint64(dst, index_offset_);
  PutVarint64(dst, index_size_);
}

Status TableHandle::DecodeFrom(Slice* input) {
  Slice smallest_key;
  Slice largest_key;
  if (!GetLengthPrefixedSlice(input, &smallest_key) ||
      !GetLengthPrefixedSlice(input, &largest_key) ||
      !GetVarint64(input, &filter_offset_) ||
      !GetVarint64(input, &filter_size_) ||
      !GetVarint64(input, &index_offset_) ||
      !GetVarint64(input, &index_size_)) {
    return Status::Corruption("Bad table handle");
  } else {
    smallest_key_ = smallest_key.ToString();
    largest_key_ = largest_key.ToString();
    return Status::OK();
  }
}

void EpochHandle::EncodeTo(std::string* dst) const {
  assert(index_offset_ != ~static_cast<uint64_t>(0));
  assert(index_size_ != ~static_cast<uint64_t>(0));
  assert(num_tables_ != ~static_cast<uint32_t>(0));
  assert(num_ents_ != ~static_cast<uint32_t>(0));

  PutVarint64(dst, index_offset_);
  PutVarint64(dst, index_size_);
  PutVarint32(dst, num_tables_);
  PutVarint32(dst, num_ents_);
}

Status EpochHandle::DecodeFrom(Slice* input) {
  if (!GetVarint64(input, &index_offset_) ||
      !GetVarint64(input, &index_size_) || !GetVarint32(input, &num_tables_) ||
      !GetVarint32(input, &num_ents_)) {
    return Status::Corruption("Bad epoch handle");
  } else {
    return Status::OK();
  }
}

void EpochStone::EncodeTo(std::string* dst) const {
  assert(id_ != ~static_cast<uint32_t>(0));
  handle_.EncodeTo(dst);
  PutVarint32(dst, id_);
}

Status EpochStone::DecodeFrom(Slice* input) {
  Status result = handle_.DecodeFrom(input);
  if (result.ok()) {
    if (!GetVarint32(input, &id_)) {
      return Status::Corruption("Bad epoch seal");
    } else {
      return Status::OK();
    }
  } else {
    return result;
  }
}

DirOptions ApplyFooter(const DirOptions& options, const Footer& footer) {
  DirOptions result = options;
  result.lg_parts = int(footer.lg_parts());
  result.num_epochs = int(footer.num_epochs());
  result.value_size = footer.value_size();
  result.key_size = footer.key_size();
  result.fixed_kv_length = footer.fixed_kv_length();
  result.leveldb_compatible = footer.leveldb_compatible();
  result.epoch_log_rotation = footer.epoch_log_rotation();
  result.skip_checksums = footer.skip_checksums();
  result.filter = static_cast<FilterType>(footer.filter_type());
  result.mode = static_cast<DirMode>(footer.mode());
  return result;
}

Footer Mkfoot(const DirOptions& options) {
  Footer result;
  result.set_lg_parts(static_cast<uint32_t>(options.lg_parts));
  result.set_value_size(static_cast<uint32_t>(options.value_size));
  result.set_key_size(static_cast<uint32_t>(options.key_size));
  result.set_fixed_kv_length(
      static_cast<unsigned char>(options.fixed_kv_length));
  result.set_leveldb_compatible(
      static_cast<unsigned char>(options.leveldb_compatible));
  result.set_epoch_log_rotation(
      static_cast<unsigned char>(options.epoch_log_rotation));
  result.set_skip_checksums(static_cast<unsigned char>(options.skip_checksums));
  result.set_filter_type(static_cast<unsigned char>(options.filter));
  result.set_mode(static_cast<unsigned char>(options.mode));
  return result;
}

void Footer::EncodeTo(std::string* dst) const {
  assert(lg_parts_ != ~static_cast<uint32_t>(0));
  assert(num_epochs_ != ~static_cast<uint32_t>(0));
  assert(value_size_ != ~static_cast<uint32_t>(0));
  assert(key_size_ != ~static_cast<uint32_t>(0));
  assert(fixed_kv_length_ != 0xFF);
  assert(leveldb_compatible_ != 0xFF);
  assert(epoch_log_rotation_ != 0xFF);
  assert(skip_checksums_ != 0xFF);
  assert(filter_type_ != 0xFF);
  assert(mode_ != 0xFF);

  epoch_index_handle_.EncodeTo(dst);
  dst->resize(BlockHandle::kMaxEncodedLength, 0);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xFFFFFFFFU));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  PutFixed32(dst, lg_parts_);
  PutFixed32(dst, num_epochs_);
  PutFixed32(dst, value_size_);
  PutFixed32(dst, key_size_);
  dst->push_back(static_cast<char>(fixed_kv_length_));
  dst->push_back(static_cast<char>(leveldb_compatible_));
  dst->push_back(static_cast<char>(epoch_log_rotation_));
  dst->push_back(static_cast<char>(skip_checksums_));
  dst->push_back(static_cast<char>(filter_type_));
  dst->push_back(static_cast<char>(mode_));
}

Status Footer::DecodeFrom(Slice* input) {
  const char* start = input->data();
  size_t size = input->size();
  uint64_t magic;

  if (size < kEncodedLength) {
    return Status::Corruption("Truncated dir footer");
  } else {
    const char* magic_ptr = start + BlockHandle::kMaxEncodedLength;
    const uint32_t magic_lo = DecodeFixed32(magic_ptr);
    const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
    magic = ((static_cast<uint64_t>(magic_hi) << 32) |
             (static_cast<uint64_t>(magic_lo)));
  }

  if (magic != kTableMagicNumber) {
    return Status::Corruption("Bad dir footer magic number");
  } else {
    lg_parts_ = DecodeFixed32(start + kEncodedLength - 22);
    num_epochs_ = DecodeFixed32(start + kEncodedLength - 18);
    value_size_ = DecodeFixed32(start + kEncodedLength - 14);
    key_size_ = DecodeFixed32(start + kEncodedLength - 10);
    fixed_kv_length_ = static_cast<unsigned char>(start[kEncodedLength - 6]);
    leveldb_compatible_ = static_cast<unsigned char>(start[kEncodedLength - 5]);
    epoch_log_rotation_ = static_cast<unsigned char>(start[kEncodedLength - 4]);
    skip_checksums_ = static_cast<unsigned char>(start[kEncodedLength - 3]);
    filter_type_ = static_cast<unsigned char>(start[kEncodedLength - 2]);
    mode_ = static_cast<unsigned char>(start[kEncodedLength - 1]);
    switch (mode_) {
      case kDmMultiMap:
      case kDmMultiMapUnordered:
      case kDmUniqueUnordered:
      case kDmUniqueDrop:
      case kDmUniqueKey:
        break;
      default:
        return Status::Corruption("Bad dir mode");
    }
  }

  Status result = epoch_index_handle_.DecodeFrom(input);
  if (result.ok()) {
    Slice source(start, size);
    source.remove_prefix(kEncodedLength);
    // This skips over any leftover data
    *input = source;
  }

  return result;
}

}  // namespace plfsio
}  // namespace pdlfs
