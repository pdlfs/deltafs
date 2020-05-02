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

// Keys for naming metadata stored in KV-stores. Each key has a type. In
// general, a key consists of a prefix and a suffix. The prefix encodes parent
// directory information and the type of the key. The suffix is typically either
// a filename or a hash of it.
class Key {
 public:
  Key() {}  // Intentionally not initialized for performance.
  // Initialize a key using a given prefix encoding.
  explicit Key(const Slice& prefix);
#if defined(DELTAFS_PROTO)
  Key(uint64_t dno, uint64_t ino, KeyType type);
#endif
#if defined(DELTAFS)
  Key(uint64_t reg, uint64_t snap, uint64_t ino, KeyType type);
  Key(uint64_t snap, uint64_t ino, KeyType type);
#endif
  // Directly initialize a key using a parent directory inode no.
  // and a specific key type.
  Key(uint64_t ino, KeyType type);
  // Quickly initialize a key using information
  // provided by a LookupStat or Stat.
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || \
    defined(INDEXFS)  // Tablefs does not use LookupStat
  Key(const LookupStat& stat, KeyType type);
#endif
  Key(const Stat& stat, KeyType type);

  void SetType(KeyType type);  // Reset the type of a key.
  // The following functions reset the suffix of
  // a key in different ways.
  void SetSuffix(const Slice& suff);
  void SetOffset(uint64_t off);

  // Other ways for setting suffixes.
  void SetName(const Slice& name);  // Names may be stored as hashes
#if defined(DELTAFS) || defined(INDEXFS)
  void SetHash(const Slice& hash);
#endif

  Slice prefix() const;  // Return the prefix encoding of a key in its entirety.
  // Return the type of a key.
  KeyType type() const;
  // Return the parent inode no. of a key.
  uint64_t inode() const;
#if defined(DELTAFS_PROTO)
  uint64_t dnode() const;
#endif
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

  // Return the final encoding of key.
  Slice Encode() const { return Slice(data(), size()); }

  // Return the raw bytes of a key. Indexfs uses immediate buf spaces.
#if defined(DELTAFS) || defined(INDEXFS)
  size_t size() const { return size_; }
  const char* data() const { return rep_; }
  char* data() { return rep_; }

  // Deltafs and tablefs use std::string.
#else
  size_t size() const { return rep_.size(); }
  const char* data() const { return &rep_[0]; }
  char* data() { return &rep_[0]; }
#endif

 private:
  // Indexfs keys have a small fixed length. So we use an immediate buf space.
#if defined(DELTAFS) || defined(INDEXFS)
  // Intentionally copyable
  char rep_[50];
  size_t size_;

  // Deltafs and tablefs store intact filenames in keys.
  // Filenames can be lengthy. So we use std::string.
#else
  std::string rep_;
#endif
};

// Common inode structure shared by deltafs, indexfs, and tablefs.
class Stat {
#if defined(DELTAFS_PROTO)
  uint64_t file_dno_;
#endif
#if defined(DELTAFS)
  uint64_t reg_id_;
  uint64_t snap_id_;
#endif
  uint64_t file_ino_;
  uint64_t file_size_;
  // Absolute time in microseconds
  uint64_t modify_time_;
  uint64_t change_time_;
  uint32_t file_mode_;  // File type and access modes
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
  uint32_t zeroth_server_;
#endif
  uint32_t user_id_;
  uint32_t group_id_;

#ifndef NDEBUG
#if defined(DELTAFS_PROTO)
  bool is_file_dno_set_;
#endif
#if defined(DELTAFS)
  bool is_reg_id_set_;
  bool is_snap_id_set_;
#endif
  bool is_file_ino_set_;
  bool is_file_size_set_;
  bool is_modify_time_set_;
  bool is_change_time_set_;
  bool is_file_mode_set_;
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
  bool is_zeroth_server_set_;
#endif
  bool is_user_id_set_;
  bool is_group_id_set_;
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
#if defined(DELTAFS_PROTO)
    assert(is_file_dno_set_);
#endif
#if defined(DELTAFS)
    assert(is_reg_id_set_);
    assert(is_snap_id_set_);
#endif
    assert(is_file_ino_set_);
    assert(is_file_size_set_);
    assert(is_modify_time_set_);
    assert(is_change_time_set_);
    assert(is_file_mode_set_);
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
    assert(is_zeroth_server_set_);
#endif
    assert(is_user_id_set_);
    assert(is_group_id_set_);
#endif
  }

#if defined(DELTAFS_PROTO)
  uint64_t DnodeNo() const { return (file_dno_); }
#endif
#if defined(DELTAFS)
  uint64_t RegId() const { return (reg_id_); }
  uint64_t SnapId() const { return (snap_id_); }
#endif
  uint64_t InodeNo() const { return (file_ino_); }
  uint64_t FileSize() const { return (file_size_); }
  uint64_t ModifyTime() const { return (modify_time_); }
  uint64_t ChangeTime() const { return (change_time_); }
  uint32_t FileMode() const { return (file_mode_); }
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
  uint32_t ZerothServer() const { return (zeroth_server_); }
#endif
  uint32_t UserId() const { return (user_id_); }
  uint32_t GroupId() const { return (group_id_); }

#if defined(DELTAFS_PROTO)
  void SetDnodeNo(uint64_t file_dno) {
    file_dno_ = file_dno;
#ifndef NDEBUG
    is_file_dno_set_ = true;
#endif
  }
#endif

#if defined(DELTAFS)
  void SetRegId(uint64_t reg_id) {
    reg_id_ = reg_id;
#ifndef NDEBUG
    is_reg_id_set_ = true;
#endif
  }

  void SetSnapId(uint64_t snap_id) {
    snap_id_ = snap_id;
#ifndef NDEBUG
    is_snap_id_set_ = true;
#endif
  }
#endif

  void SetInodeNo(uint64_t file_ino) {
    file_ino_ = file_ino;
#ifndef NDEBUG
    is_file_ino_set_ = true;
#endif
  }

  void SetFileSize(uint64_t size) {
    file_size_ = size;
#ifndef NDEBUG
    is_file_size_set_ = true;
#endif
  }

  void SetFileMode(uint32_t mode) {
    file_mode_ = mode;
#ifndef NDEBUG
    is_file_mode_set_ = true;
#endif
  }

#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
  void SetZerothServer(uint32_t server) {
    zeroth_server_ = server;
#ifndef NDEBUG
    is_zeroth_server_set_ = true;
#endif
  }
#endif

  void SetUserId(uint32_t usr) {
    user_id_ = usr;
#ifndef NDEBUG
    is_user_id_set_ = true;
#endif
  }

  void SetGroupId(uint32_t grp) {
    group_id_ = grp;
#ifndef NDEBUG
    is_group_id_set_ = true;
#endif
  }

  void SetModifyTime(uint64_t micro) {
    modify_time_ = micro;
#ifndef NDEBUG
    is_modify_time_set_ = true;
#endif
  }

  void SetChangeTime(uint64_t micro) {
    change_time_ = micro;
#ifndef NDEBUG
    is_change_time_set_ = true;
#endif
  }
};

#if defined(DELTAFS_PROTO) || defined(DELTAFS) || defined(INDEXFS)
// The result of lookup requests sent to clients during pathname resolution.
// If the lease due date is not zero, the client may cache
// and reuse the result until the specified due. Not used in tablefs.
class LookupStat {
#if defined(DELTAFS_PROTO)
  uint64_t dir_dno_;
#endif
#if defined(DELTAFS)
  uint64_t reg_id_;
  uint64_t snap_id_;
#endif
  uint64_t dir_ino_;
  // Absolute time in microseconds
  uint64_t lease_due_;
  uint32_t dir_mode_;
  uint32_t zeroth_server_;
  uint32_t user_id_;
  uint32_t group_id_;

#ifndef NDEBUG
#if defined(DELTAFS_PROTO)
  bool is_dir_dno_set_;
#endif
#if defined(DELTAFS)
  bool is_reg_id_set_;
  bool is_snap_id_set_;
#endif
  bool is_dir_ino_set_;
  bool is_lease_due_set_;
  bool is_dir_mode_set_;
  bool is_zeroth_server_set_;
  bool is_user_id_set_;
  bool is_group_id_set_;
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
#if defined(DELTAFS_PROTO)
    assert(is_dir_dno_set_);
#endif
#if defined(DELTAFS)
    assert(is_reg_id_set_);
    assert(is_snap_id_set_);
#endif
    assert(is_dir_ino_set_);
    assert(is_lease_due_set_);
    assert(is_dir_mode_set_);
    assert(is_zeroth_server_set_);
    assert(is_user_id_set_);
    assert(is_group_id_set_);
#endif
  }

#if defined(DELTAFS_PROTO)
  uint64_t DnodeNo() const { return (dir_dno_); }
#endif
#if defined(DELTAFS)
  uint64_t RegId() const { return (reg_id_); }
  uint64_t SnapId() const { return (snap_id_); }
#endif
  uint64_t InodeNo() const { return (dir_ino_); }
  uint32_t DirMode() const { return (dir_mode_); }
  uint32_t ZerothServer() const { return (zeroth_server_); }
  uint32_t UserId() const { return (user_id_); }
  uint32_t GroupId() const { return (group_id_); }
  uint64_t LeaseDue() const { return (lease_due_); }

#if defined(DELTAFS_PROTO)
  void SetDnodeNo(uint64_t dir_dno) {
    dir_dno_ = dir_dno;
#ifndef NDEBUG
    is_dir_dno_set_ = true;
#endif
  }
#endif

#if defined(DELTAFS)
  void SetRegId(uint64_t reg_id) {
    reg_id_ = reg_id;
#ifndef NDEBUG
    is_reg_id_set_ = true;
#endif
  }

  void SetSnapId(uint64_t snap_id) {
    snap_id_ = snap_id;
#ifndef NDEBUG
    is_snap_id_set_ = true;
#endif
  }
#endif

  void SetInodeNo(uint64_t dir_ino) {
    dir_ino_ = dir_ino;
#ifndef NDEBUG
    is_dir_ino_set_ = true;
#endif
  }

  void SetDirMode(uint32_t mode) {
    dir_mode_ = mode;
#ifndef NDEBUG
    is_dir_mode_set_ = true;
#endif
  }

  void SetZerothServer(uint32_t server) {
    zeroth_server_ = server;
#ifndef NDEBUG
    is_zeroth_server_set_ = true;
#endif
  }

  void SetUserId(uint32_t usr) {
    user_id_ = usr;
#ifndef NDEBUG
    is_user_id_set_ = true;
#endif
  }

  void SetGroupId(uint32_t grp) {
    group_id_ = grp;
#ifndef NDEBUG
    is_group_id_set_ = true;
#endif
  }

  void SetLeaseDue(uint64_t due) {
    lease_due_ = due;
#ifndef NDEBUG
    is_lease_due_set_ = true;
#endif
  }

  void CopyFrom(const Stat& stat);
};

#endif

}  // namespace pdlfs
