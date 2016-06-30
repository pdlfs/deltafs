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

Status MDS::RPC::CLI::Fstat(const FstatOptions& options, FstatRet* ret) {
  Status s;
  Msg in;
  Msg out;
  char* scratch = &in.buf[0];
  char* p = scratch;
  p = EncodeVarint64(p, options.dir_ino);
  p = EncodeLengthPrefixedSlice(p, options.name_hash);
  p = EncodeLengthPrefixedSlice(p, options.name);
  in.contents = Slice(scratch, p - scratch);
  try {
    stub_->FSTAT(in, out);
  } catch (int rpc_err) {
    // FIXME
  }
  if (out.err != 0) {
    s = Status::FromCode(out.err);
  } else if (!ret->stat.DecodeFrom(out.contents)) {
    s = Status::Corruption(Slice());
  }
  return s;
}

void MDS::RPC::SRV::FSTAT(Msg& in, Msg& out) {
  Status s;
  FstatOptions options;
  FstatRet ret;
  Slice input = in.contents;
  if (!GetVarint64(&input, &options.dir_ino) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      s = mds_->Fstat(options, &ret);
    } catch (Redirect& redirect) {
      // FIXME
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
  Msg out;
  char* scratch = &in.buf[0];
  char* p = scratch;
  p = EncodeVarint64(p, options.dir_ino);
  p = EncodeLengthPrefixedSlice(p, options.name_hash);
  p = EncodeLengthPrefixedSlice(p, options.name);
  p = EncodeVarint32(p, options.mode);
  p = EncodeVarint32(p, options.uid);
  p = EncodeVarint32(p, options.gid);
  in.contents = Slice(scratch, p - scratch);
  try {
    stub_->FCRET(in, out);
  } catch (int rpc_err) {
    // FIXME
  }
  if (out.err != 0) {
    s = Status::FromCode(out.err);
  } else if (!ret->stat.DecodeFrom(out.contents)) {
    s = Status::Corruption(Slice());
  }
  return s;
}

void MDS::RPC::SRV::FCRET(Msg& in, Msg& out) {
  Status s;
  FcreatOptions options;
  FcreatRet ret;
  Slice input = in.contents;
  if (!GetVarint64(&input, &options.dir_ino) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint32(&input, &options.mode) ||
      !GetVarint32(&input, &options.uid) ||
      !GetVarint32(&input, &options.gid)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      s = mds_->Fcreat(options, &ret);
    } catch (Redirect& redirect) {
      // FIXME
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
  Msg out;
  char* scratch = &in.buf[0];
  char* p = scratch;
  p = EncodeVarint64(p, options.dir_ino);
  p = EncodeLengthPrefixedSlice(p, options.name_hash);
  p = EncodeLengthPrefixedSlice(p, options.name);
  p = EncodeVarint32(p, options.mode);
  p = EncodeVarint32(p, options.uid);
  p = EncodeVarint32(p, options.gid);
  p = EncodeVarint32(p, options.zserver);
  in.contents = Slice(scratch, p - scratch);
  try {
    stub_->MKDIR(in, out);
  } catch (int rpc_err) {
    // FIXME
  }
  if (out.err != 0) {
    s = Status::FromCode(out.err);
  } else if (!ret->stat.DecodeFrom(out.contents)) {
    s = Status::Corruption(Slice());
  }
  return s;
}

void MDS::RPC::SRV::MKDIR(Msg& in, Msg& out) {
  Status s;
  MkdirOptions options;
  MkdirRet ret;
  Slice input = in.contents;
  if (!GetVarint64(&input, &options.dir_ino) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint32(&input, &options.mode) ||
      !GetVarint32(&input, &options.uid) ||
      !GetVarint32(&input, &options.gid) ||
      !GetVarint32(&input, &options.zserver)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      mds_->Mkdir(options, &ret);
    } catch (Redirect& redirect) {
      // FIXME
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
  Msg out;
  char* scratch = &in.buf[0];
  char* p = scratch;
  p = EncodeVarint64(p, options.dir_ino);
  p = EncodeLengthPrefixedSlice(p, options.name_hash);
  p = EncodeLengthPrefixedSlice(p, options.name);
  in.contents = Slice(scratch, p - scratch);
  try {
    stub_->LOKUP(in, out);
  } catch (int rpc_err) {
    // FIXME
  }
  if (out.err != 0) {
    s = Status::FromCode(out.err);
  } else if (!ret->entry.DecodeFrom(out.contents)) {
    s = Status::Corruption(Slice());
  }
  return s;
}

void MDS::RPC::SRV::LOKUP(Msg& in, Msg& out) {
  Status s;
  LookupOptions options;
  LookupRet ret;
  Slice input = in.contents;
  if (!GetVarint64(&input, &options.dir_ino) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      s = mds_->Lookup(options, &ret);
    } catch (Redirect& redirect) {
      // FIXME
    }
  }
  if (s.ok()) {
    out.contents = ret.entry.EncodeTo(out.buf);
    out.err = 0;
  } else {
    out.err = s.err_code();
  }
}

Status MDS::RPC::CLI::Chmod(const ChmodOptions& options, ChmodRet* ret) {
  Status s;
  Msg in;
  Msg out;
  char* scratch = &in.buf[0];
  char* p = scratch;
  p = EncodeVarint64(p, options.dir_ino);
  p = EncodeLengthPrefixedSlice(p, options.name_hash);
  p = EncodeLengthPrefixedSlice(p, options.name);
  p = EncodeVarint32(p, options.mode);
  in.contents = Slice(scratch, p - scratch);
  try {
    stub_->CHMOD(in, out);
  } catch (int rpc_err) {
    // FIXME
  }
  if (out.err != 0) {
    s = Status::FromCode(out.err);
  } else if (!ret->stat.DecodeFrom(out.contents)) {
    s = Status::Corruption(Slice());
  }
  return s;
}

void MDS::RPC::SRV::CHMOD(Msg& in, Msg& out) {
  Status s;
  ChmodOptions options;
  ChmodRet ret;
  Slice input = in.contents;
  if (!GetVarint64(&input, &options.dir_ino) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint32(&input, &options.mode)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      s = mds_->Chmod(options, &ret);
    } catch (Redirect& redirect) {
      // FIXME
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
  Msg out;
  char* scratch = &in.buf[0];
  char* p = scratch;
  p = EncodeVarint64(p, options.dir_ino);
  in.contents = Slice(scratch, p - scratch);
  try {
    stub_->LSDIR(in, out);
  } catch (int rpc_err) {
    // FIXME
  }
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
  return s;
}

void MDS::RPC::SRV::LSDIR(Msg& in, Msg& out) {
  Status s;
  ListdirOptions options;
  ListdirRet ret;
  Slice input = in.contents;
  if (!GetVarint64(&input, &options.dir_ino)) {
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
