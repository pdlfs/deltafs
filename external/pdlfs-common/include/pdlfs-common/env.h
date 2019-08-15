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

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */
#pragma once

#include "pdlfs-common/slice.h"
#include "pdlfs-common/status.h"

#include <stdarg.h>
#include <stdint.h>
#include <string>
#include <vector>

namespace pdlfs {

// An Env is an interface used by high-level file/data systems to access
// operating system functionalities such as the file system.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations should be safe for concurrent access from
// multiple threads without any external synchronization.
class Env;
class FileLock;
class Logger;
class RandomAccessFile;
class SequentialFile;
class Slice;
class WritableFile;

class Env {
 public:
  Env() {}
  virtual ~Env();

  // Load a specific environment implementation as is requested by the caller.
  // If the implementation is not available, NULL is returned.
  // Set *is_system to true iff the returned Env belongs to system and
  // must not be deleted.  Otherwise, the result should be
  // deleted when it is no longer needed.
  static Env* Open(const char* env_name, const char* env_conf, bool* is_system);

  // Return a default environment suitable for the current operating
  // system.  Sophisticated users may wish to provide their own Env
  // implementation instead of relying on this default environment.
  //
  // The result of Default() belongs to system and cannot be deleted.
  static Env* Default();

  // Create a brand new sequentially-readable file with the specified name.
  // On success, stores a pointer to the new file in *r and returns OK.
  // On failure stores NULL in *r and returns non-OK.  If the file does not
  // exist, returns a non-OK status.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual Status NewSequentialFile(const char* f, SequentialFile** r) = 0;

  // Create a brand new random access read-only file with the specified name.
  // On success, stores a pointer to the new file in *r and returns OK.
  // On failure stores NULL in *r and returns non-OK.  If the file does not
  // exist, returns a non-OK status.
  //
  // The returned file may be concurrently accessed by multiple threads.
  virtual Status NewRandomAccessFile(const char* f, RandomAccessFile** r) = 0;

  // Create an object that writes to a new file with the specified name.
  // Deletes any existing file with the same name and creates a new file.
  // On success, stores a pointer to the new file in *r and returns OK.
  // On failure stores NULL in *r and returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual Status NewWritableFile(const char* f, WritableFile** r) = 0;

  // Returns true iff the named file exists.
  virtual bool FileExists(const char* f) = 0;

  // Store in *r the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *r are dropped.
  virtual Status GetChildren(const char* dir, std::vector<std::string>* r) = 0;

  // Delete the named file.
  virtual Status DeleteFile(const char* f) = 0;

  // Create the specified directory.
  virtual Status CreateDir(const char* dir) = 0;

  // Load a directory created by another process.
  // This call makes no sense for Env implementations that are backed by a posix
  // file system but is usually needed for implementations that are backed by a
  // shared object storage service.
  // Return OK if the directory is attached and ready to use.
  // For Env implementations that are backed by a posix file system, return OK
  // if the directory exists.
  virtual Status AttachDir(const char* dir) = 0;

  // Delete the specified directory.
  virtual Status DeleteDir(const char* dir) = 0;

  // Forget a directory previously loaded or created. After this call, the
  // directory appears to have never been created before.
  // This call makes no sense for Env implementations that are backed by a posix
  // file system but is useful for implementations that are backed by a shared
  // object storage.
  // Return OK is the directory is detached and appears deleted. Return OK does
  // not mean the directory has been physically removed.
  // Return NotSupported for Env implementations that are backed by a posix file
  // system.
  virtual Status DetachDir(const char* dir) = 0;

  // Store the size of the named file in *file_size.
  virtual Status GetFileSize(const char* f, uint64_t* file_size) = 0;

  // Copy file src to dst.
  virtual Status CopyFile(const char* src, const char* dst) = 0;

  // Rename file src to dst.
  virtual Status RenameFile(const char* src, const char* dst) = 0;

  // Lock the specified file.  Used to prevent concurrent access to
  // the same db by multiple processes.  On failure, stores NULL in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK.  The caller should call
  // UnlockFile(*lock) to release the lock.  If the process exits,
  // the lock will be automatically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failure.  I.e., this call does not wait for existing locks
  // to go away.
  //
  // May create the named file if it does not already exist.
  virtual Status LockFile(const char* fname, FileLock** lock) = 0;

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  virtual Status UnlockFile(FileLock* lock) = 0;

  // Arrange to run "(*function)(arg)" once in a background thread.
  //
  // "function" may run in an unspecified thread.  Multiple functions
  // added to the same Env may run concurrently in different threads.
  // I.e., the caller may not assume that background work items are
  // serialized.
  virtual void Schedule(void (*function)(void*), void* arg) = 0;

  // Start a new thread, invoking "function(arg)" within the new thread.
  // When "function(arg)" returns, the thread will be destroyed.
  virtual void StartThread(void (*function)(void*), void* arg) = 0;

  // *path is set to a temporary directory that can be used for testing. It may
  // or many not have just been created. The directory may or may not differ
  // between runs of the same process, but subsequent calls will return the
  // same directory.
  virtual Status GetTestDirectory(std::string* path) = 0;

  // Create and return a log file for storing informational messages.
  virtual Status NewLogger(const char* fname, Logger** result) = 0;

  // Returns the number of micro-seconds since some fixed point in time. Only
  // useful for computing deltas of time.
  virtual uint64_t NowMicros() = 0;

  // Sleep/delay the thread for the prescribed number of micro-seconds.
  virtual void SleepForMicroseconds(int micros) = 0;

  // Obtain the network name of the local machine.
  virtual Status FetchHostname(std::string* hostname) = 0;

  // Obtain all the IP addresses bind to the local machine.
  virtual Status FetchHostIPAddrs(std::vector<std::string>* ips) = 0;

 private:
  // No copying allowed
  void operator=(const Env&);
  Env(const Env&);
};

// A file abstraction for reading sequentially through a file
class SequentialFile {
 public:
  SequentialFile() {}
  virtual ~SequentialFile();

  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
  // written by this routine.  Sets "*result" to the data that was
  // read (including if fewer than "n" bytes were successfully read).
  // May set "*result" to point at data in "scratch[0..n-1]", so
  // "scratch[0..n-1]" must be live when "*result" is used.
  // If an error was encountered, returns a non-OK status.
  //
  // REQUIRES: External synchronization
  virtual Status Read(size_t n, Slice* result, char* scratch) = 0;

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  virtual Status Skip(uint64_t n) = 0;

 private:
  // No copying allowed
  SequentialFile(const SequentialFile&);
  void operator=(const SequentialFile&);
};

// A file abstraction for randomly reading the contents of a file.
class RandomAccessFile {
 public:
  RandomAccessFile() {}
  virtual ~RandomAccessFile();

  // Read up to "n" bytes from the file starting at "offset".
  // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
  // to the data that was read (including if fewer than "n" bytes were
  // successfully read).  May set "*result" to point at data in
  // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
  // "*result" is used.  If an error was encountered, returns a non-OK
  // status.
  //
  // Safe for concurrent use by multiple threads.
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const = 0;

 private:
  // No copying allowed
  RandomAccessFile(const RandomAccessFile&);
  void operator=(const RandomAccessFile&);
};

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class WritableFile {
 public:
  WritableFile() {}
  virtual ~WritableFile();

  virtual Status Append(const Slice& data) = 0;
  virtual Status Close() = 0;
  virtual Status Flush() = 0;
  virtual Status Sync() = 0;

 private:
  // No copying allowed
  void operator=(const WritableFile&);
  WritableFile(const WritableFile&);
};

// A WritableFile implementation with all calls
// implemented as an non-operation.
class WritableFileWrapper : public WritableFile {
 public:
  WritableFileWrapper() {}
  virtual ~WritableFileWrapper();

  virtual Status Append(const Slice& data) { return Status::OK(); }

  virtual Status Flush() { return Status::OK(); }
  virtual Status Sync() { return Status::OK(); }

  virtual Status Close() {
    // Do nothing
    return Status::OK();
  }
};

// An interface for writing log messages.
class Logger {
 public:
  Logger() {}
  virtual ~Logger();

  // Return the global logger that is shared by the entire process.
  // The result of Default() belongs to the system and cannot be deleted.
  static Logger* Default();

  // Write an entry to the log file with the specified format.
  virtual void Logv(const char* file, int line, int severity, int verbose,
                    const char* format, va_list ap) = 0;

 private:
  // No copying allowed
  void operator=(const Logger&);
  Logger(const Logger&);
};

// Identifies a locked file.
class FileLock {
 public:
  FileLock() {}
  virtual ~FileLock();

 private:
  // No copying allowed
  void operator=(const FileLock&);
  FileLock(const FileLock&);
};

// A utility routine: write "data" to the named file.
extern Status WriteStringToFile(Env* env, const Slice& data, const char* fname);
// A utility routine: write "data" to the named file and Sync() it.
extern Status WriteStringToFileSync(Env* env, const Slice& data,
                                    const char* fname);

// A utility routine: read contents of named file into *data
extern Status ReadFileToString(Env* env, const char* fname, std::string* data);

// Background execution service.
class ThreadPool {
 public:
  ThreadPool() {}
  virtual ~ThreadPool();

  // Instantiate a new thread pool with a fixed number of threads. The caller
  // should delete the pool to free associated resources.
  // If "eager_init" is true, children threads will be created immediately.
  // A caller may optionally set "attr" to alter default thread behaviour.
  static ThreadPool* NewFixed(int num_threads, bool eager_init = false,
                              void* attr = NULL);

  // Arrange to run "(*function)(arg)" once in one of a pool of
  // background threads.
  //
  // "function" may run in an unspecified thread.  Multiple functions
  // added to the same pool may run concurrently in different threads.
  // I.e., the caller may not assume that background work items are
  // serialized.
  virtual void Schedule(void (*function)(void*), void* arg) = 0;

  // Return a description of the pool implementation.
  virtual std::string ToDebugString() = 0;

  // Stop executing any tasks. Tasks already scheduled will keep running. Tasks
  // not yet scheduled won't be scheduled. Tasks submitted in future will be
  // queued but won't be scheduled.
  virtual void Pause() = 0;

  // Resume executing tasks.
  virtual void Resume() = 0;

 private:
  // No copying allowed
  void operator=(const ThreadPool&);
  ThreadPool(const ThreadPool&);
};

// An implementation of Env that forwards all calls to another Env.
// May be useful to clients who wish to override just part of the
// functionality of another Env.
class EnvWrapper : public Env {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t
  explicit EnvWrapper(Env* t) : target_(t) {}
  virtual ~EnvWrapper();

  // Return the target to which this Env forwards all calls
  Env* target() const { return target_; }

  // The following text is boilerplate that forwards all methods to target()
  virtual Status NewSequentialFile(const char* f, SequentialFile** r) {
    return target_->NewSequentialFile(f, r);
  }

  virtual Status NewRandomAccessFile(const char* f, RandomAccessFile** r) {
    return target_->NewRandomAccessFile(f, r);
  }

  virtual Status NewWritableFile(const char* f, WritableFile** r) {
    return target_->NewWritableFile(f, r);
  }

  virtual bool FileExists(const char* f) { return target_->FileExists(f); }

  virtual Status GetChildren(const char* d, std::vector<std::string>* r) {
    return target_->GetChildren(d, r);
  }

  virtual Status DeleteFile(const char* f) { return target_->DeleteFile(f); }

  virtual Status CreateDir(const char* d) { return target_->CreateDir(d); }

  virtual Status AttachDir(const char* d) { return target_->AttachDir(d); }

  virtual Status DeleteDir(const char* d) { return target_->DeleteDir(d); }

  virtual Status DetachDir(const char* d) { return target_->DetachDir(d); }

  virtual Status GetFileSize(const char* f, uint64_t* s) {
    return target_->GetFileSize(f, s);
  }

  virtual Status CopyFile(const char* s, const char* t) {
    return target_->CopyFile(s, t);
  }

  virtual Status RenameFile(const char* s, const char* t) {
    return target_->RenameFile(s, t);
  }

  virtual Status LockFile(const char* f, FileLock** l) {
    return target_->LockFile(f, l);
  }

  virtual Status UnlockFile(FileLock* l) { return target_->UnlockFile(l); }

  virtual void Schedule(void (*f)(void*), void* a) {
    return target_->Schedule(f, a);
  }

  virtual void StartThread(void (*f)(void*), void* a) {
    return target_->StartThread(f, a);
  }

  virtual Status GetTestDirectory(std::string* path) {
    return target_->GetTestDirectory(path);
  }

  virtual Status NewLogger(const char* fname, Logger** result) {
    return target_->NewLogger(fname, result);
  }

  virtual uint64_t NowMicros() { return target_->NowMicros(); }

  void SleepForMicroseconds(int micros) {
    target_->SleepForMicroseconds(micros);
  }

  virtual Status FetchHostname(std::string* hostname) {
    return target_->FetchHostname(hostname);
  }

  virtual Status FetchHostIPAddrs(std::vector<std::string>* ips) {
    return target_->FetchHostIPAddrs(ips);
  }

 private:
  Env* target_;
};

}  // namespace pdlfs
