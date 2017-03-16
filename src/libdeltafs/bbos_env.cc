/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <dirent.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <deque>

#include "../../../external/pdlfs-common/include/pdlfs-common/env.h"
#include <bbos/bbos_api.h>

namespace pdlfs {
inline Status IOError(const Slice& err_context, int err_number) {
  if (err_number != ENOENT && err_number != EEXIST) {
    return Status::IOError(err_context, strerror(err_number));
  } else if (err_number == EEXIST) {
    return Status::AlreadyExists(err_context);
  } else {
    return Status::NotFound(err_context);
  }
}

class BbosBufferedSequentialFile : public SequentialFile {
 private:
  bbos_handle_t bb_handle_;
  std::string obj_name_;

 public:
  BbosBufferedSequentialFile(bbos_handle_t bb_h, std::string fname)
    : bb_handle_(bb_h), obj_name_(fname.c_str()) {}

  virtual ~BbosBufferedSequentialFile() { }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    ssize_t data_read = 0;
    size_t total_data_read = 0;
    do {
      data_read = bbos_read(bb_handle_, obj_name_.c_str(), (void *)(scratch + total_data_read),
                            total_data_read, total_data_read - data_read);
      if(data_read < 0) {
        return IOError(obj_name_, data_read);
      }

      total_data_read += data_read;
    } while(total_data_read < n);

    return Status::OK();
  }

  virtual Status Skip(uint64_t n) {
    return Status::OK();
  }
};

class BbosSequentialFile : public SequentialFile {
 private:
  bbos_handle_t bb_handle_;
  std::string obj_name_;

 public:
  BbosSequentialFile(bbos_handle_t bb_h, std::string fname)
      : bb_handle_(bb_h), obj_name_(fname.c_str()) {}

  virtual ~BbosSequentialFile() { }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    ssize_t data_read = 0;
    size_t total_data_read = 0;
    do {
      data_read = bbos_read(bb_handle_, obj_name_.c_str(), (void *)(scratch + total_data_read),
                            total_data_read, total_data_read - data_read);
      if(data_read < 0) {
        return IOError(obj_name_, data_read);
      }

      total_data_read += data_read;
    } while(total_data_read < n);

    return Status::OK();
  }

  virtual Status Skip(uint64_t n) {
    return Status::OK();
  }
};

class BbosRandomAccessFile : public RandomAccessFile {
 private:
  bbos_handle_t bb_handle_;
  std::string obj_name_;

 public:
  BbosRandomAccessFile(bbos_handle_t bb_h, std::string fname)
      : bb_handle_(bb_h), obj_name_(fname.c_str()) {}

  virtual ~BbosRandomAccessFile() { }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    ssize_t data_read = 0;
    size_t total_data_read = 0;
    do {
      data_read = bbos_read(bb_handle_, obj_name_.c_str(), (void *)(scratch + total_data_read + offset),
                            total_data_read, total_data_read - data_read);
      if(data_read < 0) {
        return IOError(obj_name_, data_read);
      }

      total_data_read += data_read;
    } while(total_data_read < n);

    return Status::OK();
  }
};

class BbosBufferedWritableFile : public WritableFile {
 private:
  bbos_handle_t bb_handle_;
  std::string obj_name_;

 public:
  BbosBufferedWritableFile(bbos_handle_t bb_h, std::string fname)
      : bb_handle_(bb_h), obj_name_(fname.c_str()) {}

  virtual ~BbosBufferedWritableFile() { }

  virtual Status Append(const Slice& data) {
    size_t total_data_written = 0;
    ssize_t data_written = 0;
    do {
      data_written = bbos_append(bb_handle_, obj_name_.c_str(),
                                 (void *)(data.data() + total_data_written),
                                 data.size() - total_data_written);
      if(data_written < 0) {
        return IOError(obj_name_, data_written);
      }
      total_data_written += data_written;
    } while(total_data_written < data.size());
    return Status::OK();
  }

  virtual Status Close() {
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  Status SyncDirIfManifest() {
    return Status::OK();
  }

  virtual Status Sync() {
    return Status::OK();
  }
};

class BbosWritableFile : public WritableFile {
 private:
  bbos_handle_t bb_handle_;
  std::string obj_name_;

 public:
  BbosWritableFile(bbos_handle_t bb_h, std::string fname)
      : bb_handle_(bb_h), obj_name_(fname.c_str()) {}

  virtual ~BbosWritableFile() { }

  virtual Status Append(const Slice& buf) {
    size_t total_data_written = 0;
    ssize_t data_written = 0;
    do {
      data_written = bbos_append(bb_handle_, obj_name_.c_str(),
                                 (void *)(buf.data() + total_data_written),
                                 buf.size() - total_data_written);
      if(data_written < 0) {
        return IOError(obj_name_, data_written);
      }
      total_data_written += data_written;
    } while(total_data_written < buf.size());
    return Status::OK();
  }

  virtual Status Close() {
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status SyncDirIfManifest() {
    return Status::OK();
  }

  virtual Status Sync() {
    return Status::OK();
  }
};

class BbosEnv: public Env {
 private:
  bbos_handle_t bb_handle_;

 public:
  explicit BbosEnv() {
    if(bbos_init("bmi+tcp://localhost:19900", "bmi+tcp://localhost:19900", &bb_handle_) != BB_SUCCESS) {
      return;
    }
  }
  virtual ~BbosEnv() { abort(); }

  virtual Status NewSequentialFile(const Slice& fname,
                                   SequentialFile** result) {
    int ret = bbos_mkobj(bb_handle_, fname.c_str(), WRITE_OPTIMIZED);
    if(ret == BB_SUCCESS) {
      *result = new BbosSequentialFile(bb_handle_, fname.ToString());
      return Status::OK();
    }
    *result = NULL;
    return IOError(fname, ret);
  }

  virtual Status NewRandomAccessFile(const Slice& fname,
                                     RandomAccessFile** result) {
    int ret = bbos_mkobj(bb_handle_, fname.c_str(), WRITE_OPTIMIZED);
    if(ret == BB_SUCCESS) {
      *result = new BbosRandomAccessFile(bb_handle_, fname.ToString());
      return Status::OK();
    }
    *result = NULL;
    return IOError(fname, ret);
  }

  virtual Status NewWritableFile(const Slice& fname, WritableFile** result) {
    int ret = bbos_mkobj(bb_handle_, fname.c_str(), WRITE_OPTIMIZED);
    if(ret == BB_SUCCESS) {
      *result = new BbosWritableFile(bb_handle_, fname.ToString());
      return Status::OK();
    }
    *result = NULL;
    return IOError(fname, ret);
  }

  virtual bool FileExists(const Slice& fname) {
    uint64_t size;
    Status s;
    s = GetFileSize(fname, &size);
    if(!s.ok()) {
      return true;
    }
    return false;
  }

  virtual Status GetChildren(const Slice& dir,
                             std::vector<std::string>* result) = 0;

  virtual Status DeleteFile(const Slice& fname) {
    return Status::OK();
  }

  virtual Status CreateDir(const Slice& dirname) = 0;

  virtual Status AttachDir(const Slice& dirname) = 0;

  virtual Status DeleteDir(const Slice& dirname) = 0;

  virtual Status DetachDir(const Slice& dirname) = 0;

  virtual Status GetFileSize(const Slice& fname, uint64_t* file_size) {
    Status s;
    off_t sz = bbos_get_size(bb_handle_, fname.c_str());
    if(sz < 0) {
      s = IOError(fname, sz);
    } else {
      *file_size = (uint64_t) sz;
    }
    return s;
  }

  virtual Status CopyFile(const Slice& src, const Slice& target) {
    Status status;
    ssize_t total_data_read = 0;
    ssize_t total_data_written = 0;
    ssize_t data_read = 0;
    ssize_t data_written = 0;
    /* XXXSAK: replace size to be malloc'd by PFS_CHUNK_SIZE */
    int read_chunk_size = 8 * 1024 * 1024;
    void *buf = (void *) malloc (read_chunk_size);
    /* We could decide to perform a GetFileSize and then loop to read the file.
     * But doing this the following way prevents one RPC for GetFileSize.
     */
    do {
      data_read = bbos_read(bb_handle_, src.c_str(), (void *)buf,
                            total_data_read, read_chunk_size);
      if(data_read < 0) {
        status = IOError(src, data_read);
        return status;
      }

      total_data_written = 0;
      do {
        data_written = bbos_append(bb_handle_, target.c_str(),
                                   (void *)((char *)buf + total_data_written),
                                   data_read - total_data_written);
        if(data_written < 0) {
          status = IOError(target, data_written);
          return status;
        }
        total_data_written += data_written;
      } while(total_data_written < data_read);

      total_data_read += data_read;
    } while(data_read < read_chunk_size);

    free(buf);

    return Status::OK();
  }

  virtual Status RenameFile(const Slice& src, const Slice& target) = 0;

  virtual Status LockFile(const Slice& fname, FileLock** lock) = 0;

  virtual Status UnlockFile(FileLock* lock) = 0;

  virtual void Schedule(void (*function)(void* arg), void* arg) = 0;

  virtual void StartThread(void (*function)(void* arg), void* arg) = 0;

  virtual Status GetTestDirectory(std::string* path) = 0;

  virtual Status NewLogger(const std::string& fname, Logger** result) = 0;

  virtual uint64_t NowMicros() = 0;

  virtual void SleepForMicroseconds(int micros) = 0;

  virtual Status FetchHostname(std::string* hostname) = 0;

  virtual Status FetchHostIPAddrs(std::vector<std::string>* ips) = 0;
};

}  // namespace pdlfs
