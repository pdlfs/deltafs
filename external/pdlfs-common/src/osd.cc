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

#include "pdlfs-common/osd.h"
#include "pdlfs-common/env.h"

namespace pdlfs {

Osd::~Osd() {}

namespace {
// A simple OSD wrapper implementation that routes everything to an Env
// instance. The caller specifies a path prefix so that all data objects will
// be stored under that path. If "env" is NULL, the result of Env::Default()
// will be used. The caller must delete the result when it is no longer needed.
// In addition, "*env" must remain live while the result is in use.
class OsdWrapper : public Osd {
 public:
  OsdWrapper(Env* env, const char* prefix) : env_(env) {
    prefix_ = prefix;
    env_->CreateDir(prefix_.c_str());  // Ignore error. Dir may exist.
    prefix_.append("/obj_");
  }

  virtual ~OsdWrapper() {}

  virtual Status NewSequentialObj(const char* name, SequentialFile** r) {
    const std::string fp = prefix_ + name;
    return env_->NewSequentialFile(fp.c_str(), r);
  }

  virtual Status NewRandomAccessObj(const char* name, RandomAccessFile** r) {
    const std::string fp = prefix_ + name;
    return env_->NewRandomAccessFile(fp.c_str(), r);
  }

  virtual Status NewWritableObj(const char* name, WritableFile** r) {
    const std::string fp = prefix_ + name;
    return env_->NewWritableFile(fp.c_str(), r);
  }

  virtual bool Exists(const char* name) {
    const std::string fp = prefix_ + name;
    return env_->FileExists(fp.c_str());
  }

  virtual Status Size(const char* name, uint64_t* obj_size) {
    const std::string fp = prefix_ + name;
    return env_->GetFileSize(fp.c_str(), obj_size);
  }

  virtual Status Delete(const char* name) {
    const std::string fp = prefix_ + name;
    return env_->DeleteFile(fp.c_str());
  }

  virtual Status Put(const char* name, const Slice& data) {
    const std::string fp = prefix_ + name;
    return WriteStringToFile(env_, data, fp.c_str());
  }

  virtual Status Get(const char* name, std::string* data) {
    const std::string fp = prefix_ + name;
    return ReadFileToString(env_, fp.c_str(), data);
  }

  virtual Status Copy(const char* src, const char* dst) {
    const std::string fp1 = prefix_ + src;
    const std::string fp2 = prefix_ + dst;
    return env_->CopyFile(fp1.c_str(), fp2.c_str());
  }

 private:
  // No copying allowed
  void operator=(const OsdWrapper&);
  OsdWrapper(const OsdWrapper&);

  std::string prefix_;
  Env* const env_;
};
}  // namespace

Osd* Osd::FromEnv(const char* prefix, Env* env) {
  if (env == NULL) env = Env::Default();
  return new OsdWrapper(env, prefix);
}

}  // namespace pdlfs
