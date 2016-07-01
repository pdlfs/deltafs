#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdint.h>
#include <string.h>

#include "pdlfs-common/coding.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

enum KeyType {
  kDirEntType = 1,      // File or directory entry with embedded inode
  kDirIdxType = 2,      // GIGA+ directory index (deltafs, indexfs only)
  kDirMetaType = 3,     // Directory partition metadata (deltafs, indexfs only)
  kSuperBlockType = 4,  // File system superblock
  kDataBlockType = 5,   // Data block for small files (or all files)
  kInoType = 6  // Dedicated inode entry that can be hard linked (tablefs only)
};

class Key {
 public:
  Key() {}
  explicit Key(uint64_t dir, KeyType type);
  explicit Key(uint64_t reg, uint64_t snap, uint64_t dir, KeyType type);
  void SetName(const Slice& name);
  void SetHash(const Slice& hash);

  uint64_t reg_id() const;
  uint64_t snap_id() const;
  uint64_t dir_id() const;
  KeyType type() const;
  Slice hash() const;
  Slice prefix() const;

  Slice Encode() const { return Slice(data(), size()); }
  size_t size() const { return size_; }
  const char* data() const { return rep_; }
  char* data() { return rep_; }

 private:
  // Intentionally copyable
  char rep_[32];
  size_t size_;
};

// Common inode structure used in deltafs, indexfs, and tablefs.
class Stat {
  char file_ino_[8];
  char file_size_[8];
  char file_mode_[4];      // File type and access modes
  char zeroth_server_[4];  // Not used in tablefs
  char user_id_[4];
  char group_id_[4];
  char reserved_space0_[8];
  // We don't yet maintain access time
  char modify_time_[8];  // Absolute time in microseconds
  char change_time_[8];  // Absolute time in microseconds
  char reserved_space1_[4];
  char reserved_space2_[4];

#ifndef NDEBUG
  bool is_file_ino_set_;
  bool is_file_size_set_;
  bool is_file_mode_set_;
  bool is_zeroth_server_set_;
  bool is_user_id_set_;
  bool is_group_id_set_;
  bool is_modify_time_set_;
  bool is_change_time_set_;
#endif

 public:
  // scratch[...] should at least have sizeof(Stat) of bytes, although
  // the real size used after encoding may be much smaller.
  Slice EncodeTo(char* scratch) const;
  // Return true if success, false otherwise.
  bool DecodeFrom(const Slice& encoding);
  bool DecodeFrom(Slice* input);

  Stat() {
#ifndef NDEBUG
    memset(this, 0, sizeof(Stat));
#endif
  }

  void AssertAllSet() {
#ifndef NDEBUG
    assert(is_file_ino_set_);
    assert(is_file_size_set_);
    assert(is_file_mode_set_);
    assert(is_zeroth_server_set_);
    assert(is_user_id_set_);
    assert(is_group_id_set_);
    assert(is_modify_time_set_);
    assert(is_change_time_set_);
#endif
  }

  uint64_t InodeNo() const { return DecodeFixed64(file_ino_); }
  uint64_t FileSize() const { return DecodeFixed64(file_size_); }
  uint32_t FileMode() const { return DecodeFixed32(file_mode_); }
  uint32_t ZerothServer() const { return DecodeFixed32(zeroth_server_); }
  uint32_t UserId() const { return DecodeFixed32(user_id_); }
  uint32_t GroupId() const { return DecodeFixed32(group_id_); }
  uint64_t ModifyTime() const { return DecodeFixed64(modify_time_); }
  uint64_t ChangeTime() const { return DecodeFixed64(change_time_); }

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

// The result of lookup requests sent to clients during pathname resolution.
// If the lease due date is not zero, the client may cache
// and reuse the result until the specified due. Not used in tablefs.
class LookupEntry {
  char dir_ino_[8];
  char dir_mode_[4];
  char zeroth_server_[4];
  char user_id_[4];
  char group_id_[4];
  // Lease states
  char lease_due_[8];  // Absolute time in microseconds

#ifndef NDEBUG
  bool is_dir_ino_set_;
  bool is_dir_mode_set_;
  bool is_zeroth_server_set_;
  bool is_user_id_set_;
  bool is_group_id_set_;
  bool is_lease_due_set_;
#endif

 public:
  // scratch[...] should at least have sizeof(DirEntry) of bytes,
  // although the real size used after encoding may be much smaller.
  Slice EncodeTo(char* scratch) const;
  // Return true if success, false otherwise
  bool DecodeFrom(const Slice& encoding);
  bool DecodeFrom(Slice* input);

  LookupEntry() {
#ifndef NDEBUG
    memset(this, 0, sizeof(Stat));
#endif
  }

  void AssertAllSet() {
#ifndef NDEBUG
    assert(is_dir_ino_set_);
    assert(is_dir_mode_set_);
    assert(is_zeroth_server_set_);
    assert(is_user_id_set_);
    assert(is_group_id_set_);
    assert(is_lease_due_set_);
#endif
  }

  uint64_t InodeNo() const { return DecodeFixed64(dir_ino_); }
  uint32_t DirMode() const { return DecodeFixed32(dir_mode_); }
  uint32_t ZerothServer() const { return DecodeFixed32(zeroth_server_); }
  uint32_t UserId() const { return DecodeFixed32(user_id_); }
  uint32_t GroupId() const { return DecodeFixed32(group_id_); }
  uint64_t LeaseDue() const { return DecodeFixed64(lease_due_); }

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

}  // namespace pdlfs
