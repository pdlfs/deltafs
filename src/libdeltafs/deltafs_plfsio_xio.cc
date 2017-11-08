/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_xio.h"
#include "deltafs_plfsio.h"
#include "deltafs_plfsio_format.h"

#include "pdlfs-common/logging.h"
#include "pdlfs-common/strutil.h"

#include <algorithm>
#include <vector>

namespace pdlfs {
namespace plfsio {

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
    snprintf(tmp, sizeof(tmp), "T-%04x", index);
    return tmp;
  } else {
    return "";
  }
}

template <typename T>
static std::string Lname(const std::string& prefix, int index,  // Rolling index
                         const T& options) {
  std::string result = prefix;
  if (index != -1) result += "/" + Lset(index);
  result += "/" + Lrank(options.rank) + Lsuffix(options.type);
  result += Lpart(options.sub_partition);
  return result;
}

LogSink::~LogSink() {
  if (file_ != NULL) {
    Finish();
  }
}

Status LogSink::Lrotate(int index, bool sync) {
  if (rlog_ == NULL) {
    return Status::AssertionFailed("Log rotation not enabled", filename_);
  } else if (file_ == NULL) {
    return Status::AssertionFailed("Log already closed", filename_);
  } else {
    if (mu_ != NULL) {
      mu_->AssertHeld();
    }

    Status status;
    // Potentially buffered data must be written out
    if (buf_file_ != NULL) {
      status = buf_file_->EmptyBuffer();
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
    if (index != -1)
      env_->CreateDir(
          p.c_str());  // Ignore error since the directory may exist already
    std::string filename = Lname(prefix_, index, opts_);
    status = env_->NewWritableFile(filename.c_str(), &new_base);
    if (status.ok()) {
      status = rlog_->Rotate(new_base);
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

// Return the current physical write offset.
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
    if (buf_file_ != NULL) {
      status = buf_file_->EmptyBuffer();  // Force buffer flush
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

// Default options for writing log data.
LogSink::LogOptions::LogOptions()
    : rank(0),
      sub_partition(-1),
      max_buf(4096),
      min_buf(4096),
      rotation(kNoRotation),
      type(kData),
      mu(NULL),
      stats(NULL),
      env(Env::Default()) {}

Status LogSink::Open(const LogOptions& opts, const std::string& prefix,
                     LogSink** result) {
  *result = NULL;
  int index = -1;  // Initial log rolling index
  if (opts.rotation != kNoRotation) {
    index = 0;
  }
  Env* const env = opts.env;
  std::string p = prefix + "/" + Lset(index);
  if (index != -1)
    env->CreateDir(
        p.c_str());  // Ignore error since the directory may exist already
  std::string filename = Lname(prefix, index, opts);
  WritableFile* base = NULL;
  Status status = env->NewWritableFile(filename.c_str(), &base);
  if (status.ok()) {
    assert(base != NULL);
  } else {
    return status;
  }

  RollingLogFile* vf = NULL;
  if (opts.rotation != kNoRotation) {
    vf = new RollingLogFile(base);
    base = vf;
  }

  WritableFile* file;
  // Link to external stats for I/O monitoring
  if (opts.stats != NULL) {
    file = new MeasuredWritableFile(opts.stats, base);
  } else {
    file = base;
  }
  BufferedFile* wb = NULL;
  if (opts.min_buf != 0) {
    wb = new BufferedFile(file, opts.min_buf, opts.max_buf);
    file = wb;
  } else {
    // No write buffering?
  }

#if VERBOSE >= 3
  Verbose(__LOG_ARGS__, 3, "Writing into %s, buffer=%s", filename.c_str(),
          PrettySize(opts.max_buf).c_str());
#endif
  LogSink* sink = new LogSink(opts, prefix, wb, vf);
  sink->filename_ = filename;
  sink->file_ = file;
  sink->Ref();

  *result = sink;
  return status;
}

void LogSource::Unref() {
  assert(refs_ > 0);
  refs_--;
  if (refs_ == 0) {
    delete this;
  }
}

LogSource::~LogSource() {
  for (size_t i = 0; i < num_files_; i++) {
    delete files_[i].first;
  }
  delete[] files_;
}

// Default options for read logged data.
LogSource::LogOptions::LogOptions()
    : rank(0),
      sub_partition(-1),
      num_rotas(-1),
      type(kData),
      seq_stats(NULL),
      stats(NULL),
      io_size(4096),
      env(Env::Default()) {}

static Status OpenWithEagerSeqReads(
    const std::string& filename, size_t io_size, Env* env,
    SequentialFileStats* stats,
    std::vector<std::pair<RandomAccessFile*, uint64_t> >* result) {
  SequentialFile* base = NULL;
  uint64_t size = 0;
  Status status = env->NewSequentialFile(filename.c_str(), &base);
  if (status.ok()) {
    status = env->GetFileSize(filename.c_str(), &size);
    if (!status.ok()) {
      delete base;
    }
  }
  if (!status.ok()) {
    return status;
  }

  SequentialFile* file = base;
  if (stats != NULL) {
    file = new MeasuredSequentialFile(stats, base);
  }
  WholeFileBufferedRandomAccessFile* cached_file =
      new WholeFileBufferedRandomAccessFile(file, size, io_size);
  status = cached_file->Load();
  if (!status.ok()) {
    delete cached_file;
    return status;
  }

#if VERBOSE >= 3
  Verbose(__LOG_ARGS__, 3, "Reading from %s (eagerly pre-fetched), size=%s",
          filename.c_str(), PrettySize(size).c_str());
#endif
  result->push_back(std::make_pair(cached_file, size));
  return status;
}

static Status RandomAccessOpen(
    const std::string& filename, Env* env, RandomAccessFileStats* stats,
    std::vector<std::pair<RandomAccessFile*, uint64_t> >* result) {
  RandomAccessFile* base = NULL;
  uint64_t size = 0;
  Status status = env->NewRandomAccessFile(filename.c_str(), &base);
  if (status.ok()) {
    status = env->GetFileSize(filename.c_str(), &size);
    if (!status.ok()) {
      delete base;
    }
  }
  if (!status.ok()) {
    return status;
  }

  RandomAccessFile* file = base;
  if (stats != NULL) {
    file = new MeasuredRandomAccessFile(stats, base);
  }
#if VERBOSE >= 3
  Verbose(__LOG_ARGS__, 3, "Reading from %s (random access), size=%s",
          filename.c_str(), PrettySize(size).c_str());
#endif
  result->push_back(std::make_pair(file, size));
  return status;
}

// Eagerly pre-fetch the entire file data in case of index logs.
// Return OK on success, or a non-OK status on errors.
static Status TryOpenIt(
    const std::string& f, const LogSource::LogOptions& opts,
    std::vector<std::pair<RandomAccessFile*, uint64_t> >* r) {
  if (opts.type == LogType::kIndex)
    return OpenWithEagerSeqReads(f, opts.io_size, opts.env, opts.seq_stats, r);
  return RandomAccessOpen(f, opts.env, opts.stats, r);
};

Status LogSource::Open(const LogOptions& opts, const std::string& prefix,
                       LogSource** result) {
  *result = NULL;
  Status status;
  std::vector<std::pair<RandomAccessFile*, uint64_t> > sources;
  if (opts.num_rotas == -1) {
    status = TryOpenIt(Lname(prefix, opts.num_rotas, opts), opts, &sources);
  } else {
    for (int i = 0; i < opts.num_rotas; i++) {
      status = TryOpenIt(Lname(prefix, i, opts), opts, &sources);
      if (!status.ok()) {
        break;
      }
    }
  }

  if (status.ok()) {
    LogSource* src = new LogSource(opts, prefix);
    std::pair<RandomAccessFile*, uint64_t>* files =
        new std::pair<RandomAccessFile*, uint64_t>[ sources.size() ];
    for (size_t i = 0; i < sources.size(); i++) {
      files[i].second = static_cast<uint64_t>(sources[i].second);
      files[i].first = sources[i].first;
    }
    src->num_files_ = sources.size();
    src->files_ = files;
    src->Ref();

    sources.clear();
    *result = src;
  }

  // In case of errors, delete any allocated resources
  std::vector<std::pair<RandomAccessFile*, uint64_t> >::iterator it;
  for (it = sources.begin(); it != sources.end(); ++it) {
    RandomAccessFile* f = it->first;
    delete f;
  }

  return status;
}

static DirOptions SanitizeDirOptions(const DirOptions& opts) {
  DirOptions result = opts;
  if (result.env == NULL) {
    result.env = Env::Default();
  }
  return result;
}

static Status Delete(const char* filename, Env* env) {
#if VERBOSE >= 3
  Verbose(__LOG_ARGS__, 3, "Removing %s ...", filename);
#endif
  return env->DeleteFile(filename);
}

Status DestroyDir(const std::string& prefix, const DirOptions& opts) {
  Status status;
  DirOptions options = SanitizeDirOptions(opts);
  std::vector<std::string> garbage;  // Log files pending deletion
  garbage.push_back(FooterFileName(prefix));
  Env* const env = options.env;
  if (options.is_env_pfs) {
    std::vector<std::string> names;
    status = env->GetChildren(prefix.c_str(), &names);
    if (status.ok()) {
      for (size_t i = 0; i < names.size() && status.ok(); i++) {
        if (!names[i].empty() && names[i][0] != '.') {
          std::string entry = prefix + "/" + names[i];
          if (names[i][0] == 'T') {  // Log rotation set
            std::vector<std::string> subnames;
            status = env->GetChildren(entry.c_str(), &subnames);
            if (status.ok()) {
              for (size_t ii = 0; ii < subnames.size(); ii++) {
                if (!subnames[ii].empty() && subnames[ii][0] != '.') {
                  if (subnames[ii][0] == 'L') {
                    garbage.push_back(entry + "/" + subnames[ii]);
                  } else {
                    // Skip
                  }
                }
              }
            }

          } else if (names[i][0] == 'L') {
            garbage.push_back(entry);  // Regular log file
          } else {
            // Skip
          }
        }
      }
    }
  }

  // Bulk file deletes
  if (status.ok()) {
    std::sort(garbage.begin(), garbage.end());
    std::vector<std::string>::iterator it;
    for (it = garbage.begin(); it != garbage.end(); ++it) {
      status = Delete(it->c_str(), env);
      if (!status.ok()) {
        break;
      }
    }
  }

  return status;
}

}  // namespace plfsio
}  // namespace pdlfs
