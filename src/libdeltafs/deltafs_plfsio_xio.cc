/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_xio.h"

namespace pdlfs {
namespace plfsio {

namespace xio {

class RollingLogFile : public WritableFile {
 public:
  // *base must remain alive during the lifetime of this class. *base will be
  // implicitly closed and deleted by the destructor of this class.
  explicit RollingLogFile(WritableFile* base) : base_(base) {}

  virtual ~RollingLogFile() {
    if (base_ != NULL) {
      base_->Close();
      delete base_;
    }
  }

  // REQUIRES: Close() has not been called.
  virtual Status Append(const Slice& data) {
    if (base_ == NULL) {
      return Status::Disconnected(Slice());
    } else {
      return base_->Append(data);
    }
  }

  // REQUIRES: Close() has not been called.
  virtual Status Flush() {
    if (base_ == NULL) {
      return Status::Disconnected(Slice());
    } else {
      return base_->Flush();
    }
  }

  // REQUIRES: Close() has not been called.
  virtual Status Sync() {
    if (base_ == NULL) {
      return Status::Disconnected(Slice());
    } else {
      return base_->Sync();
    }
  }

  // To ensure data durability, Flush() and Sync() must be called
  // before Close() may be called.
  virtual Status Close() {
    if (base_ != NULL) {
      Status status = base_->Close();
      delete base_;
      base_ = NULL;
      return status;
    } else {
      return Status::OK();
    }
  }

 private:
  // Switch to a new log file. To ensure data durability,
  // Sync() must be called before Rotate(new_base) may be called.
  // Return OK on success, or a non-OK status on errors.
  Status Rotate(WritableFile* new_base) {
    Status status;
    if (base_ != NULL) {
      status = base_->Flush();  // Pre-close file and catch potential errors
      if (status.ok()) {
        base_->Close();  // Ignore errors
        delete base_;
      }
    }
    // Do not switch if there are outstanding errors on the
    // previous log file. This avoids data loss.
    if (status.ok()) {
      base_ = new_base;
    }
    return status;
  }

  // No copying allowed
  void operator=(const RollingLogFile& r);
  RollingLogFile(const RollingLogFile&);

  // State below requires external synchronization
  WritableFile* base_;

  friend class LogSink;
};

static std::string Lrank(int rank) {
  char tmp[20];
  if (rank != -1) {
    snprintf(tmp, sizeof(tmp), "L-%08x", rank);
    return tmp;
  } else {
    return "????????";
  }
}

static std::string Lpart(int sub_partition) {
  char tmp[10];
  if (sub_partition != -1) {
    snprintf(tmp, sizeof(tmp), ".%02x", sub_partition);
    return tmp;
  } else {
    return "";
  }
}

static std::string Lsuffix(LogType type) {
  if (type == LogType::kIndex) {
    return ".idx";
  } else {
    return ".dat";
  }
}

static std::string Lset(int index) {
  char tmp[20];
  if (index != -1) {
    snprintf(tmp, sizeof(tmp), "R-%04x", index);
    return tmp;
  } else {
    return "";
  }
}

static std::string Lname(const std::string& prefix, int index,  // Rolling index
                         const LogOptions& options) {
  std::string result = prefix;
  result += "/" + Lset(index) + "/" + Lrank(options.rank);
  result += Lsuffix(options.type);
  result += Lpart(options.sub_partition);
  return result;
}

LogSink::~LogSink() {
  if (file_ != NULL) {
    Finish();
  }
}

Status LogSink::Lrotate(int index, bool sync) {
  if (vf_ == NULL) {
    return Status::AssertionFailed("Log rotation not enabled", filename_);
  } else if (file_ == NULL) {
    return Status::AssertionFailed("Log already closed", filename_);
  } else {
    if (mu_ != NULL) {
      mu_->AssertHeld();
    }

    Status status;
    // Potentially buffered data must be written out
    if (buf_ != NULL) {
      status = buf_->EmptyBuffer();
    } else {
      status = file_->Flush();  // Pre-catch potential storage errors
    }
    // Force data sync if requested
    if (sync && status.ok()) status = file_->Sync();
    if (!status.ok()) {
      return status;
    }

    WritableFile* new_base;
    std::string p = prefix_ + "/" + Lset(index);
    env_->CreateDir(
        p.c_str());  // Ignore error since the directory may exist already
    std::string filename = Lname(prefix_, index, options_);
    status = env_->NewWritableFile(filename.c_str(), &new_base);
    if (status.ok()) {
      status = vf_->Rotate(new_base);
      if (status.ok()) {
        prev_off_ = off_;  // Remember previous write offset
        filename_ = filename;
      } else {  // This does not remove the file
        new_base->Close();
        delete new_base;
      }
    }

    return status;
  }
}

uint64_t LogSink::Ptell() const {
  uint64_t result = off_ - prev_off_;
  assert(off_ >= prev_off_);
  return result;
}

Status LogSink::Lclose(bool sync) {
  Status status;
  if (file_ == NULL) {
    status = finish_status_;  // Return the previous finish result
  } else {
    if (mu_ != NULL) {
      mu_->AssertHeld();
    }
    if (buf_ != NULL) {
      status = buf_->EmptyBuffer();  // Force buffer flush
    } else {
      status = file_->Flush();
    }
    if (sync && status.ok()) status = file_->Sync();
    if (status.ok()) {
      // Transient storage errors that might happen during
      // file closing will become final. The calling process won't
      // be able to re-try the failed writes.
      status = Finish();
      if (!status.ok()) {
        finish_status_ = status;
      }
    }
  }
  return status;
}

// To ensure data durability, Lsync() or Lclose(sync=true)
// must be called before Finish().
Status LogSink::Finish() {
  assert(file_ != NULL);
  // Delayed writes are likely sent to storage. Data
  // durability is not promised.
  Status status = file_->Close();
  delete file_;
  file_ = NULL;
  return status;
}

void LogSink::Unref() {
  assert(refs_ > 0);
  refs_--;
  if (refs_ == 0) {
    delete this;
  }
}

Status LogSink::Open(const LogOptions& options, const std::string& prefix,
                     LogSink** result) {
  *result = NULL;
  int index = -1;  // Initial log rolling index
  if (options.rotation != kNoRotation) {
    index = 0;
  }
  Env* const env = options.env;
  std::string p = prefix + "/" + Lset(index);
  env->CreateDir(
      p.c_str());  // Ignore error since the directory may exist already
  std::string filename = Lname(prefix, index, options);
  WritableFile* base = NULL;
  Status status = env->NewWritableFile(filename.c_str(), &base);
  if (status.ok()) {
    assert(base != NULL);
  } else {
    return status;
  }

  RollingLogFile* vf = NULL;
  if (options.rotation != kNoRotation) {
    vf = new RollingLogFile(base);
    base = vf;
  }

  WritableFile* file;
  // Link to external stats for I/O monitoring
  if (options.stats != NULL) {
    file = new MeasuredWritableFile(options.stats, base);
  } else {
    file = base;
  }
  BufferedLogFile* wb = NULL;
  if (options.min_buf != 0) {
    wb = new BufferedLogFile(file, options.min_buf, options.max_buf);
    file = wb;
  } else {
    // No write buffering?
  }

  LogSink* sink = new LogSink(options, prefix, wb, vf);
  sink->filename_ = filename;
  sink->file_ = file;
  sink->Ref();

  *result = sink;
  return status;
}
}

}  // namespace plfsio
}  // namespace pdlfs
