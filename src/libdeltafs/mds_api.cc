/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "mds_api.h"

namespace pdlfs {

MDS::~MDS() {}

MDS::RPC::CLI::~CLI() {}

MDS::RPC::SRV::~SRV() {}

MDSWrapper::~MDSWrapper() {}

template <typename T>
static char* EncodeDirId(char* dst, const T& options) {
  dst = EncodeVarint64(dst, options.reg_id);
  dst = EncodeVarint64(dst, options.snap_id);
  dst = EncodeVarint64(dst, options.dir_ino);
  return dst;
}

template <typename T>
static void PutDirId(std::string* dst, const T& options) {
  PutVarint64(dst, options.reg_id);
  PutVarint64(dst, options.snap_id);
  PutVarint64(dst, options.dir_ino);
}

template <typename T>
static bool GetDirId(Slice* input, T* options) {
  if (!GetVarint64(input, &options->reg_id) ||
      !GetVarint64(input, &options->snap_id) ||
      !GetVarint64(input, &options->dir_ino)) {
    return false;
  } else {
    return true;
  }
}

Status MDS::RPC::CLI::Fstat(const FstatOptions& options, FstatRet* ret) {
  Status s;
  Msg in;
  if (options.name.size() <= 1000) {
    char* scratch = &in.buf[0];
    char* p = scratch;
    p = EncodeDirId(p, options);
    p = EncodeLengthPrefixedSlice(p, options.name_hash);
    p = EncodeLengthPrefixedSlice(p, options.name);
    p = EncodeVarint32(p, options.session_id);
    p = EncodeVarint64(p, options.op_due);
    in.contents = Slice(scratch, p - scratch);
  } else {
    PutDirId(&in.extra_buf, options);
    PutLengthPrefixedSlice(&in.extra_buf, options.name_hash);
    PutLengthPrefixedSlice(&in.extra_buf, options.name);
    PutVarint32(&in.extra_buf, options.session_id);
    PutVarint64(&in.extra_buf, options.op_due);
    in.contents = Slice(in.extra_buf);
    if (in.contents.size() > 4000) {
      s = Status::BufferFull(Slice());
    }
  }

  Msg out;
  if (s.ok()) {
    try {
      stub_->FSTAT(in, out);
    } catch (int rpc_err) {
      s = Status::Disconnected(Slice());
    }
    if (s.ok()) {
      if (out.err == -1) {
        Redirect re(out.contents.data(), out.contents.size());
        throw re;
      } else if (out.err != 0) {
        s = Status::FromCode(out.err);
      } else if (!ret->stat.DecodeFrom(out.contents)) {
        s = Status::Corruption(Slice());
      }
    }
  }
  return s;
}

void MDS::RPC::SRV::FSTAT(Msg& in, Msg& out) {
  Status s;
  FstatOptions options;
  FstatRet ret;
  Slice input = in.contents;
  if (!GetDirId(&input, &options) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      s = mds_->Fstat(options, &ret);
    } catch (Redirect& re) {
      out.extra_buf = re;
      out.contents = Slice(out.extra_buf);
      out.err = -1;
      return;
    }
  }
  if (s.ok()) {
    out.contents = ret.stat.EncodeTo(out.buf);
    out.err = 0;
  } else {
    out.err = s.err_code();
  }
}

Status MDS::RPC::CLI::Fcreat(const FcreatOptions& options, FcreatRet* ret) {
  Status s;
  Msg in;
  if (options.name.size() <= 1000) {
    char* scratch = &in.buf[0];
    char* p = scratch;
    p = EncodeDirId(p, options);
    p = EncodeLengthPrefixedSlice(p, options.name_hash);
    p = EncodeLengthPrefixedSlice(p, options.name);
    p = EncodeVarint32(p, options.mode);
    p = EncodeVarint32(p, options.uid);
    p = EncodeVarint32(p, options.gid);
    p = EncodeVarint32(p, options.session_id);
    p = EncodeVarint64(p, options.op_due);
    in.contents = Slice(scratch, p - scratch);
  } else {
    PutDirId(&in.extra_buf, options);
    PutLengthPrefixedSlice(&in.extra_buf, options.name_hash);
    PutLengthPrefixedSlice(&in.extra_buf, options.name);
    PutVarint32(&in.extra_buf, options.mode);
    PutVarint32(&in.extra_buf, options.uid);
    PutVarint32(&in.extra_buf, options.gid);
    PutVarint32(&in.extra_buf, options.session_id);
    PutVarint64(&in.extra_buf, options.op_due);
    in.contents = Slice(in.extra_buf);
    if (in.contents.size() > 4000) {
      s = Status::BufferFull(Slice());
    }
  }

  Msg out;
  if (s.ok()) {
    try {
      stub_->FCRET(in, out);
    } catch (int rpc_err) {
      s = Status::Disconnected(Slice());
    }
    if (s.ok()) {
      if (out.err == -1) {
        Redirect re(out.contents.data(), out.contents.size());
        throw re;
      } else if (out.err != 0) {
        s = Status::FromCode(out.err);
      } else if (!ret->stat.DecodeFrom(out.contents)) {
        s = Status::Corruption(Slice());
      }
    }
  }
  return s;
}

void MDS::RPC::SRV::FCRET(Msg& in, Msg& out) {
  Status s;
  FcreatOptions options;
  FcreatRet ret;
  Slice input = in.contents;
  if (!GetDirId(&input, &options) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint32(&input, &options.mode) ||
      !GetVarint32(&input, &options.uid) ||
      !GetVarint32(&input, &options.gid) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      s = mds_->Fcreat(options, &ret);
    } catch (Redirect& re) {
      out.extra_buf = re;
      out.contents = Slice(out.extra_buf);
      out.err = -1;
      return;
    }
  }
  if (s.ok()) {
    out.contents = ret.stat.EncodeTo(out.buf);
    out.err = 0;
  } else {
    out.err = s.err_code();
  }
}

Status MDS::RPC::CLI::Mkdir(const MkdirOptions& options, MkdirRet* ret) {
  Status s;
  Msg in;
  if (options.name.size() <= 1000) {
    char* scratch = &in.buf[0];
    char* p = scratch;
    p = EncodeDirId(p, options);
    p = EncodeLengthPrefixedSlice(p, options.name_hash);
    p = EncodeLengthPrefixedSlice(p, options.name);
    p = EncodeVarint32(p, options.mode);
    p = EncodeVarint32(p, options.uid);
    p = EncodeVarint32(p, options.gid);
    p = EncodeVarint32(p, options.zserver);
    p = EncodeVarint32(p, options.session_id);
    p = EncodeVarint64(p, options.op_due);
    in.contents = Slice(scratch, p - scratch);
  } else {
    PutDirId(&in.extra_buf, options);
    PutLengthPrefixedSlice(&in.extra_buf, options.name_hash);
    PutLengthPrefixedSlice(&in.extra_buf, options.name);
    PutVarint32(&in.extra_buf, options.mode);
    PutVarint32(&in.extra_buf, options.uid);
    PutVarint32(&in.extra_buf, options.gid);
    PutVarint32(&in.extra_buf, options.zserver);
    PutVarint32(&in.extra_buf, options.session_id);
    PutVarint64(&in.extra_buf, options.op_due);
    in.contents = Slice(in.extra_buf);
    if (in.contents.size() > 4000) {
      s = Status::BufferFull(Slice());
    }
  }

  Msg out;
  if (s.ok()) {
    try {
      stub_->MKDIR(in, out);
    } catch (int rpc_err) {
      s = Status::Disconnected(Slice());
    }
    if (s.ok()) {
      if (out.err == -1) {
        Redirect re(out.contents.data(), out.contents.size());
        throw re;
      } else if (out.err != 0) {
        s = Status::FromCode(out.err);
      } else if (!ret->stat.DecodeFrom(out.contents)) {
        s = Status::Corruption(Slice());
      }
    }
  }
  return s;
}

void MDS::RPC::SRV::MKDIR(Msg& in, Msg& out) {
  Status s;
  MkdirOptions options;
  MkdirRet ret;
  Slice input = in.contents;
  if (!GetDirId(&input, &options) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint32(&input, &options.mode) ||
      !GetVarint32(&input, &options.uid) ||
      !GetVarint32(&input, &options.gid) ||
      !GetVarint32(&input, &options.zserver) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      mds_->Mkdir(options, &ret);
    } catch (Redirect& re) {
      out.extra_buf = re;
      out.contents = Slice(out.extra_buf);
      out.err = -1;
      return;
    }
  }
  if (s.ok()) {
    out.contents = ret.stat.EncodeTo(out.buf);
    out.err = 0;
  } else {
    out.err = s.err_code();
  }
}

Status MDS::RPC::CLI::Lookup(const LookupOptions& options, LookupRet* ret) {
  Status s;
  Msg in;
  if (options.name.size() <= 1000) {
    char* scratch = &in.buf[0];
    char* p = scratch;
    p = EncodeDirId(p, options);
    p = EncodeLengthPrefixedSlice(p, options.name_hash);
    p = EncodeLengthPrefixedSlice(p, options.name);
    p = EncodeVarint32(p, options.session_id);
    p = EncodeVarint64(p, options.op_due);
    in.contents = Slice(scratch, p - scratch);
  } else {
    PutDirId(&in.extra_buf, options);
    PutLengthPrefixedSlice(&in.extra_buf, options.name_hash);
    PutLengthPrefixedSlice(&in.extra_buf, options.name);
    PutVarint32(&in.extra_buf, options.session_id);
    PutVarint64(&in.extra_buf, options.op_due);
    in.contents = Slice(in.extra_buf);
    if (in.contents.size() > 4000) {
      s = Status::BufferFull(Slice());
    }
  }

  Msg out;
  if (s.ok()) {
    try {
      stub_->LOKUP(in, out);
    } catch (int rpc_err) {
      s = Status::Disconnected(Slice());
    }
    if (s.ok()) {
      if (out.err == -1) {
        Redirect re(out.contents.data(), out.contents.size());
        throw re;
      } else if (out.err != 0) {
        s = Status::FromCode(out.err);
      } else if (!ret->stat.DecodeFrom(out.contents)) {
        s = Status::Corruption(Slice());
      }
    }
  }
  return s;
}

void MDS::RPC::SRV::LOKUP(Msg& in, Msg& out) {
  Status s;
  LookupOptions options;
  LookupRet ret;
  Slice input = in.contents;
  if (!GetDirId(&input, &options) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      s = mds_->Lookup(options, &ret);
    } catch (Redirect& re) {
      out.extra_buf = re;
      out.contents = Slice(out.extra_buf);
      out.err = -1;
      return;
    }
  }
  if (s.ok()) {
    out.contents = ret.stat.EncodeTo(out.buf);
    out.err = 0;
  } else {
    out.err = s.err_code();
  }
}

Status MDS::RPC::CLI::Chmod(const ChmodOptions& options, ChmodRet* ret) {
  Status s;
  Msg in;
  if (options.name.size() <= 1000) {
    char* scratch = &in.buf[0];
    char* p = scratch;
    p = EncodeDirId(p, options);
    p = EncodeLengthPrefixedSlice(p, options.name_hash);
    p = EncodeLengthPrefixedSlice(p, options.name);
    p = EncodeVarint32(p, options.mode);
    p = EncodeVarint32(p, options.session_id);
    p = EncodeVarint64(p, options.op_due);
    in.contents = Slice(scratch, p - scratch);
  } else {
    PutDirId(&in.extra_buf, options);
    PutLengthPrefixedSlice(&in.extra_buf, options.name_hash);
    PutLengthPrefixedSlice(&in.extra_buf, options.name);
    PutVarint32(&in.extra_buf, options.mode);
    PutVarint32(&in.extra_buf, options.session_id);
    PutVarint64(&in.extra_buf, options.op_due);
    in.contents = Slice(in.extra_buf);
    if (in.contents.size() > 4000) {
      s = Status::BufferFull(Slice());
    }
  }

  Msg out;
  if (s.ok()) {
    try {
      stub_->CHMOD(in, out);
    } catch (int rpc_err) {
      s = Status::Disconnected(Slice());
    }
    if (s.ok()) {
      if (out.err == -1) {
        Redirect re(out.contents.data(), out.contents.size());
        throw re;
      } else if (out.err != 0) {
        s = Status::FromCode(out.err);
      } else if (!ret->stat.DecodeFrom(out.contents)) {
        s = Status::Corruption(Slice());
      }
    }
  }
  return s;
}

void MDS::RPC::SRV::CHMOD(Msg& in, Msg& out) {
  Status s;
  ChmodOptions options;
  ChmodRet ret;
  Slice input = in.contents;
  if (!GetDirId(&input, &options) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint32(&input, &options.mode) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      s = mds_->Chmod(options, &ret);
    } catch (Redirect& re) {
      out.extra_buf = re;
      out.contents = Slice(out.extra_buf);
      out.err = -1;
      return;
    }
  }
  if (s.ok()) {
    out.contents = ret.stat.EncodeTo(out.buf);
    out.err = 0;
  } else {
    out.err = s.err_code();
  }
}

Status MDS::RPC::CLI::Listdir(const ListdirOptions& options, ListdirRet* ret) {
  Status s;
  Msg in;
  char* scratch = &in.buf[0];
  char* p = scratch;
  p = EncodeDirId(p, options);
  p = EncodeVarint32(p, options.session_id);
  p = EncodeVarint64(p, options.op_due);
  in.contents = Slice(scratch, p - scratch);
  Msg out;
  try {
    stub_->LSDIR(in, out);
  } catch (int rpc_err) {
    s = Status::Disconnected(Slice());
  }
  if (s.ok()) {
    std::vector<std::string>* names = &ret->names;
    if (out.err != 0) {
      s = Status::FromCode(out.err);
    } else {
      uint32_t num;
      Slice name;
      Slice encoding = out.contents;
      if (GetVarint32(&encoding, &num)) {
        while (num-- != 0) {
          if (GetLengthPrefixedSlice(&encoding, &name)) {
            names->push_back(name.ToString());
          } else {
            break;
          }
        }
      }
    }
  }
  return s;
}

void MDS::RPC::SRV::LSDIR(Msg& in, Msg& out) {
  Status s;
  ListdirOptions options;
  ListdirRet ret;
  Slice input = in.contents;
  if (!GetDirId(&input, &options) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    s = mds_->Listdir(options, &ret);
  }
  if (s.ok()) {
    out.extra_buf.reserve(4000);
    PutVarint32(&out.extra_buf, ret.names.size());
    for (std::vector<std::string>::iterator it = ret.names.begin();
         it != ret.names.end(); ++it) {
      PutLengthPrefixedSlice(&out.extra_buf, *it);
    }
    out.contents = Slice(out.extra_buf);
    out.err = 0;
  } else {
    out.err = s.err_code();
  }
}

void MDS::RPC::SRV::NONOP(Msg& in, Msg& out) {
  // Do nothing
  out.err = 0;
}

void MDS::RPC::SRV::CHOWN(Msg& in, Msg& out) {
  // FIXME
  Status s = Status::NotSupported(Slice());
  out.err = s.err_code();
}

void MDS::RPC::SRV::UNLNK(Msg& in, Msg& out) {
  // FIXME
  Status s = Status::NotSupported(Slice());
  out.err = s.err_code();
}

void MDS::RPC::SRV::RENME(Msg& in, Msg& out) {
  // FIXME
  Status s = Status::NotSupported(Slice());
  out.err = s.err_code();
}

void MDS::RPC::SRV::RMDIR(Msg& in, Msg& out) {
  // FIXME
  Status s = Status::NotSupported(Slice());
  out.err = s.err_code();
}

}  // namespace pdlfs
