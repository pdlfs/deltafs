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
#include "pdlfs-common/status.h"

#if defined(DELTAFS_BBOS)
#include <bbos/bbos_api.h>
#endif

namespace pdlfs {
namespace bbos {

// Factory method

// Create a new bbos client instance with an existing mercury context.
// The existing mercury context may be NULL so a new and private mercury context
// will be created by bbos.
extern Status BbosInit(Env**, const char* hg_local, const char* hg_remote,
                       void* hg_class, void* hg_ctx);

// Object types defined by bbos
enum BbosType {
  kData = WRITE_OPTIMIZED,  // Sequential writes and random reads
  kIndex = READ_OPTIMIZED,  // Sequential writes and reads

  // Default to write optimized
  kDefault = kData
};

// Convert bbos error codes to standard deltafs error status
extern Status BbosError(const std::string& err_msg, int err_num);

// Determine object type according to a given file name
inline BbosType TryResolveBbosType(const std::string& name) {
  if (name.rfind(".idx") != std::string::npos) {
    return BbosType::kIndex;
  } else if (name.rfind(".dat") != std::string::npos) {
    return BbosType::kData;
  } else {
    return BbosType::kDefault;
  }
}

#if defined(DELTAFS_BBOS)
// Thread-unsafe sequential read-only file abstraction built on top of bbos
class BbosSequentialFile : public SequentialFile {
 private:
  std::string obj_name_;
  bbos_handle_t bbos_;
  uint64_t off_;

 public:
  BbosSequentialFile(const std::string& obj_name, bbos_handle_t bbos)
      : obj_name_(obj_name), bbos_(bbos), off_(0) {}

  virtual ~BbosSequentialFile() {}

  virtual Status Skip(uint64_t n) {
    off_ += n;
    return Status::OK();
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    ssize_t ret = bbos_read(bbos_, obj_name_.c_str(), scratch, off_, n);
    if (ret < 0) {
      std::string bbos_err_msg("cannot read from bbos object '");
      bbos_err_msg += obj_name_;
      bbos_err_msg += "'";
      *result = Slice();
      return BbosError(bbos_err_msg, ret);
    } else {
      off_ += ret;
      *result = Slice(scratch, ret);
      return Status::OK();
    }
  }
};

// Thread-unsafe random access file built on top of bbos
class BbosRandomAccessFile : public RandomAccessFile {
 private:
  std::string obj_name_;
  bbos_handle_t bbos_;

 public:
  BbosRandomAccessFile(const std::string& obj_name, bbos_handle_t bbos)
      : obj_name_(obj_name), bbos_(bbos) {}

  virtual ~BbosRandomAccessFile() {}

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    ssize_t ret = bbos_read(bbos_, obj_name_.c_str(), scratch, offset, n);
    if (ret < 0) {
      std::string bbos_err_msg("cannot read from bbos object '");
      bbos_err_msg += obj_name_;
      bbos_err_msg += "'";
      *result = Slice();
      return BbosError(bbos_err_msg, ret);
    } else {
      *result = Slice(scratch, ret);
      return Status::OK();
    }
  }
};

// Thread-unsafe append-only file built on top of bbos
class BbosWritableFile : public WritableFile {
 private:
  std::string obj_name_;
  bbos_handle_t bbos_;

 public:
  BbosWritableFile(const std::string& obj_name, bbos_handle_t bbos)
      : obj_name_(obj_name), bbos_(bbos) {}

  virtual ~BbosWritableFile() {}

  virtual Status Append(const Slice& buf) {
    uint64_t size = buf.size();
    const char* data = buf.data();
    // No partial writes
    do {
      ssize_t ret =
          bbos_append(bbos_, obj_name_.c_str(), const_cast<char*>(data), size);
      if (ret < 0) {
        std::string bbos_err_msg("cannot write into bbos object '");
        bbos_err_msg += obj_name_;
        bbos_err_msg += "'";
        return BbosError(bbos_err_msg, ret);
      } else {
        assert(size >= ret);
        size -= ret;
        data += ret;
      }
    } while (size > 0);
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();  // Do nothing
  }

  virtual Status Sync() {
    return Status::OK();  // XXX: fix me when bbos implements sync
  }

  virtual Status Close() {
    return Status::OK();  // Do nothing
  }
};
#endif

}  // namespace bbos
}  // namespace pdlfs
