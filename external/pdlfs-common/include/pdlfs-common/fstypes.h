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
#pragma once

#include "pdlfs-common/coding.h"
#include "pdlfs-common/status.h"

#include <stdint.h>
#include <string.h>

namespace pdlfs {

class LookupStat;
class Stat;

enum KeyType {
  kDirEntType = 1,      // File or directory entry with embedded inode
  kDirIdxType = 2,      // GIGA+ directory index (deltafs, indexfs only)
  kDirMetaType = 3,     // Directory partition metadata (deltafs, indexfs only)
  kSuperBlockType = 4,  // File system superblock
  kDataDesType = 5,     // Header block of stored files
  kDataBlockType = 6,   // Data block of stored files
  kInoType = 7  // Dedicated inode entry that can be hard linked (tablefs only)
};

// Keys used for accessing metadata stored in KV-stores. In general, each key
// consists of a prefix and suffix. The prefix encodes a parent directory id and
// the type of the key as specified in KeyType. The suffix is typically either a
// filename or a hash of it.
class Key {
 public:
  Key() {}  // Intentionally not initialized for performance.
  // Initialize a key using a given prefix encoding.
  explicit Key(const Slice& prefix);
#if defined(DELTAFS)
  Key(uint64_t reg, uint64_t snap, uint64_t ino, KeyType type);
  Key(uint64_t snap, uint64_t ino, KeyType type);
#endif
  // Directly initialize a key using a given parent directory inode no.
  // along with the type of the key.
  Key(uint64_t ino, KeyType type);
  // Quickly initialize a key using information
  // provided by a LookupStat or Stat.
#if defined(DELTAFS) || defined(INDEXFS)  // Tablefs does not use LookupStat
  Key(const LookupStat& stat, KeyType type);
#endif
  Key(const Stat& stat, KeyType type);

  void SetType(KeyType type);  // Reset the type of a key.
  // The following functions reset the suffix of
  // a key in different ways.
  void SetSuffix(const Slice& suff);
  void SetIntegerUniquifier(uint64_t uni);

  // Other alternatives for setting suffixes.
  void SetName(const Slice& name);
  void SetOffset(uint64_t off);
#if defined(DELTAFS) || defined(INDEXFS)
  void SetHash(const Slice& hash);
#endif

  Slice prefix() const;  // Return the prefix encoding of a key in its entirety.
  // Return the type of a key.
  KeyType type() const;
  // Return the parent inode no. of a key.
  uint64_t inode() const;
#if defined(DELTAFS)  // Return reg id and snap id individually.
  uint64_t snap_id() const;
  uint64_t reg_id() const;
#endif

  // Return the suffix encoding of a key in its entirety.
  Slice suffix() const;
  // Return suffix as an integer.
  uint64_t offset() const;
#if defined(DELTAFS) || defined(INDEXFS)
  Slice hash() const;
#endif

  // Return the complete encoding (prefix + suffix) of key.
  Slice Encode() const { return Slice(data(), size()); }

  // Return the buf space. Deltafs and indexfs use immediate buf spaces.
#if defined(DELTAFS) || defined(INDEXFS)
  size_t size() const { return size_; }
  const char* data() const { return rep_; }
  char* data() { return rep_; }

  // Tablefs uses std::string.
#else
  size_t size() const { return rep_.size(); }
  const char* data() const { return &rep_[0]; }
  char* data() { return &rep_[0]; }
#endif

 private:
  // Deltafs and indexfs keys have fixed max length.
  // So we use an immediate buf space.
#if defined(DELTAFS) || defined(INDEXFS)
  // Intentionally copyable
  char rep_[50];
  size_t size_;

  // Tablefs stores filenames in keys, which can
  // be super lengthy. We use std::string.
#else
  std::string rep_;
#endif
};

// Common inode structure shared by deltafs, indexfs, and tablefs.
class Stat {
#if defined(DELTAFS)
  char reg_id_[8];
  char snap_id_[8];
#endif
  char file_ino_[8];
  char file_size_[8];
  char file_mode_[4];  // File type and access modes
#if defined(DELTAFS) || defined(INDEXFS)
  char zeroth_server_[4];
#endif
  char user_id_[4];
  char group_id_[4];
  // Absolute time in microseconds
  char modify_time_[8];
  char change_time_[8];

#ifndef NDEBUG
#if defined(DELTAFS)
  bool is_reg_id_set_;
  bool is_snap_id_set_;
#endif
  bool is_file_ino_set_;
  bool is_file_size_set_;
  bool is_file_mode_set_;
#if defined(DELTAFS) || defined(INDEXFS)
  bool is_zeroth_server_set_;
#endif
  bool is_user_id_set_;
  bool is_group_id_set_;
  bool is_modify_time_set_;
  bool is_change_time_set_;
#endif

 public:
  enum { kMaxEncodedLength = 80 };
  // scratch[...] should at least have kMaxEncodedLength of bytes, although
  // the real size used after encoding may be much smaller.
  Slice EncodeTo(char* scratch) const;
  // Return true if success, false otherwise.
  bool DecodeFrom(const Slice& encoding);
  bool DecodeFrom(Slice* input);
  // Intentionally not initialized for performance.
  Stat() {}

  void AssertAllSet() {
#ifndef NDEBUG
#if defined(DELTAFS)
    assert(is_reg_id_set_);
    assert(is_snap_id_set_);
#endif
    assert(is_file_ino_set_);
    assert(is_file_size_set_);
    assert(is_file_mode_set_);
#if defined(DELTAFS) || defined(INDEXFS)
    assert(is_zeroth_server_set_);
#endif
    assert(is_user_id_set_);
    assert(is_group_id_set_);
    assert(is_modify_time_set_);
    assert(is_change_time_set_);
#endif
  }

#if defined(DELTAFS)
  uint64_t RegId() const { return DecodeFixed64(reg_id_); }
  uint64_t SnapId() const { return DecodeFixed64(snap_id_); }
#endif
  uint64_t InodeNo() const { return DecodeFixed64(file_ino_); }
  uint64_t FileSize() const { return DecodeFixed64(file_size_); }
  uint32_t FileMode() const { return DecodeFixed32(file_mode_); }
#if defined(DELTAFS) || defined(INDEXFS)
  uint32_t ZerothServer() const { return DecodeFixed32(zeroth_server_); }
#endif
  uint32_t UserId() const { return DecodeFixed32(user_id_); }
  uint32_t GroupId() const { return DecodeFixed32(group_id_); }
  uint64_t ModifyTime() const { return DecodeFixed64(modify_time_); }
  uint64_t ChangeTime() const { return DecodeFixed64(change_time_); }

#if defined(DELTAFS)
  void SetRegId(uint64_t reg_id) {
    EncodeFixed64(reg_id_, reg_id);
#ifndef NDEBUG
    is_reg_id_set_ = true;
#endif
  }

  void SetSnapId(uint64_t snap_id) {
    EncodeFixed64(snap_id_, snap_id);
#ifndef NDEBUG
    is_snap_id_set_ = true;
#endif
  }
#endif

  void SetInodeNo(uint64_t inode_no) {
    EncodeFixed64(file_ino_, inode_no);
#ifndef NDEBUG
    is_file_ino_set_ = true;
#endif
  }

  void SetFileSize(uint64_t size) {
    EncodeFixed64(file_size_, size);
#ifndef NDEBUG
    is_file_size_set_ = true;
#endif
  }

  void SetFileMode(uint32_t mode) {
    EncodeFixed32(file_mode_, mode);
#ifndef NDEBUG
    is_file_mode_set_ = true;
#endif
  }

#if defined(DELTAFS) || defined(INDEXFS)
  void SetZerothServer(uint32_t server) {
    EncodeFixed32(zeroth_server_, server);
#ifndef NDEBUG
    is_zeroth_server_set_ = true;
#endif
  }
#endif

  void SetUserId(uint32_t usr) {
    EncodeFixed32(user_id_, usr);
#ifndef NDEBUG
    is_user_id_set_ = true;
#endif
  }

  void SetGroupId(uint32_t grp) {
    EncodeFixed32(group_id_, grp);
#ifndef NDEBUG
    is_group_id_set_ = true;
#endif
  }

  void SetModifyTime(uint64_t micro) {
    EncodeFixed64(modify_time_, micro);
#ifndef NDEBUG
    is_modify_time_set_ = true;
#endif
  }

  void SetChangeTime(uint64_t micro) {
    EncodeFixed64(change_time_, micro);
#ifndef NDEBUG
    is_change_time_set_ = true;
#endif
  }
};

#if defined(DELTAFS) || defined(INDEXFS)
// The result of lookup requests sent to clients during pathname resolution.
// If the lease due date is not zero, the client may cache
// and reuse the result until the specified due. Not used in tablefs.
class LookupStat {
#if defined(DELTAFS)
  char reg_id_[8];
  char snap_id_[8];
#endif
  char dir_ino_[8];
  char dir_mode_[4];
  char zeroth_server_[4];
  char user_id_[4];
  char group_id_[4];
  // Absolute time in microseconds
  char lease_due_[8];

#ifndef NDEBUG
#if defined(DELTAFS)
  bool is_reg_id_set_;
  bool is_snap_id_set_;
#endif
  bool is_dir_ino_set_;
  bool is_dir_mode_set_;
  bool is_zeroth_server_set_;
  bool is_user_id_set_;
  bool is_group_id_set_;
  bool is_lease_due_set_;
#endif

 public:
  enum { kMaxEncodedLength = 60 };
  // scratch[...] should at least have kMaxEncodedLength of bytes, although the
  // real size used after encoding may be much smaller.
  Slice EncodeTo(char* scratch) const;
  // Return true if success, false otherwise
  bool DecodeFrom(const Slice& encoding);
  bool DecodeFrom(Slice* input);
  // Intentionally not initialized for performance.
  LookupStat() {}

  void AssertAllSet() {
#ifndef NDEBUG
#if defined(DELTAFS)
    assert(is_reg_id_set_);
    assert(is_snap_id_set_);
#endif
    assert(is_dir_ino_set_);
    assert(is_dir_mode_set_);
    assert(is_zeroth_server_set_);
    assert(is_user_id_set_);
    assert(is_group_id_set_);
    assert(is_lease_due_set_);
#endif
  }

#if defined(DELTAFS)
  uint64_t RegId() const { return DecodeFixed64(reg_id_); }
  uint64_t SnapId() const { return DecodeFixed64(snap_id_); }
#endif
  uint64_t InodeNo() const { return DecodeFixed64(dir_ino_); }
  uint32_t DirMode() const { return DecodeFixed32(dir_mode_); }
  uint32_t ZerothServer() const { return DecodeFixed32(zeroth_server_); }
  uint32_t UserId() const { return DecodeFixed32(user_id_); }
  uint32_t GroupId() const { return DecodeFixed32(group_id_); }
  uint64_t LeaseDue() const { return DecodeFixed64(lease_due_); }

#if defined(DELTAFS)
  void SetRegId(uint64_t reg_id) {
    EncodeFixed64(reg_id_, reg_id);
#ifndef NDEBUG
    is_reg_id_set_ = true;
#endif
  }

  void SetSnapId(uint64_t snap_id) {
    EncodeFixed64(snap_id_, snap_id);
#ifndef NDEBUG
    is_snap_id_set_ = true;
#endif
  }
#endif

  void SetInodeNo(uint64_t inode_no) {
    EncodeFixed64(dir_ino_, inode_no);
#ifndef NDEBUG
    is_dir_ino_set_ = true;
#endif
  }

  void SetDirMode(uint32_t mode) {
    EncodeFixed32(dir_mode_, mode);
#ifndef NDEBUG
    is_dir_mode_set_ = true;
#endif
  }

  void SetZerothServer(uint32_t server) {
    EncodeFixed32(zeroth_server_, server);
#ifndef NDEBUG
    is_zeroth_server_set_ = true;
#endif
  }

  void SetUserId(uint32_t usr) {
    EncodeFixed32(user_id_, usr);
#ifndef NDEBUG
    is_user_id_set_ = true;
#endif
  }

  void SetGroupId(uint32_t grp) {
    EncodeFixed32(group_id_, grp);
#ifndef NDEBUG
    is_group_id_set_ = true;
#endif
  }

  void SetLeaseDue(uint64_t due) {
    EncodeFixed64(lease_due_, due);
#ifndef NDEBUG
    is_lease_due_set_ = true;
#endif
  }

  void CopyFrom(const Stat& stat);
};

#endif

}  // namespace pdlfs
