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

MDSTracer::~MDSTracer() {}

static char* EncodeDirId(char* dst, const DirId& id) {
  dst = EncodeVarint64(dst, id.reg);
  dst = EncodeVarint64(dst, id.snap);
  dst = EncodeVarint64(dst, id.ino);
  return dst;
}

static void PutDirId(std::string* dst, const DirId& id) {
  PutVarint64(dst, id.reg);
  PutVarint64(dst, id.snap);
  PutVarint64(dst, id.ino);
}

static bool GetDirId(Slice* input, DirId* id) {
  if (!GetVarint64(input, &id->reg) || !GetVarint64(input, &id->snap) ||
      !GetVarint64(input, &id->ino)) {
    return false;
  } else {
    return true;
  }
}

#ifndef NDEBUG
#define MDS_RPC_DEBUG 1
#else
#define MDS_RPC_DEBUG 0
#endif

static const bool kDebugRPC = MDS_RPC_DEBUG;

#undef MDS_RPC_DEBUG

Status MDS::RPC::CLI::Fstat(const FstatOptions& options, FstatRet* ret) {
  Status s;
  Msg in;
  if (!kDebugRPC) {
    char* scratch = &in.buf[0];
    char* p = scratch;
    p = EncodeDirId(p, options.dir_id);
    p = EncodeLengthPrefixedSlice(p, options.name_hash);
    p = EncodeLengthPrefixedSlice(p, options.name);
    p = EncodeVarint32(p, options.session_id);
    p = EncodeVarint64(p, options.op_due);
    in.contents = Slice(scratch, p - scratch);
  } else {
    PutDirId(&in.extra_buf, options.dir_id);
    PutLengthPrefixedSlice(&in.extra_buf, options.name_hash);
    PutLengthPrefixedSlice(&in.extra_buf, options.name);
    PutVarint32(&in.extra_buf, options.session_id);
    PutVarint64(&in.extra_buf, options.op_due);
    in.contents = Slice(in.extra_buf);
    if (in.contents.size() > sizeof(in.buf)) {
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
  if (!GetDirId(&input, &options.dir_id) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      s = mds_->Fstat(options, &ret);
    } catch (Redirect& re) {
      out.extra_buf.swap(re);
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
  if (!kDebugRPC) {
    char* scratch = &in.buf[0];
    char* p = scratch;
    p = EncodeDirId(p, options.dir_id);
    p = EncodeLengthPrefixedSlice(p, options.name_hash);
    p = EncodeLengthPrefixedSlice(p, options.name);
    p = EncodeVarint32(p, options.mode);
    p = EncodeVarint32(p, options.uid);
    p = EncodeVarint32(p, options.gid);
    p = EncodeVarint32(p, options.session_id);
    p = EncodeVarint64(p, options.op_due);
    in.contents = Slice(scratch, p - scratch);
  } else {
    PutDirId(&in.extra_buf, options.dir_id);
    PutLengthPrefixedSlice(&in.extra_buf, options.name_hash);
    PutLengthPrefixedSlice(&in.extra_buf, options.name);
    PutVarint32(&in.extra_buf, options.mode);
    PutVarint32(&in.extra_buf, options.uid);
    PutVarint32(&in.extra_buf, options.gid);
    PutVarint32(&in.extra_buf, options.session_id);
    PutVarint64(&in.extra_buf, options.op_due);
    in.contents = Slice(in.extra_buf);
    if (in.contents.size() > sizeof(in.buf)) {
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
  if (!GetDirId(&input, &options.dir_id) ||
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
      out.extra_buf.swap(re);
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
  if (!kDebugRPC) {
    char* scratch = &in.buf[0];
    char* p = scratch;
    p = EncodeDirId(p, options.dir_id);
    p = EncodeLengthPrefixedSlice(p, options.name_hash);
    p = EncodeLengthPrefixedSlice(p, options.name);
    p = EncodeVarint32(p, options.mode);
    p = EncodeVarint32(p, options.uid);
    p = EncodeVarint32(p, options.gid);
    p = EncodeVarint32(p, options.session_id);
    p = EncodeVarint64(p, options.op_due);
    in.contents = Slice(scratch, p - scratch);
  } else {
    PutDirId(&in.extra_buf, options.dir_id);
    PutLengthPrefixedSlice(&in.extra_buf, options.name_hash);
    PutLengthPrefixedSlice(&in.extra_buf, options.name);
    PutVarint32(&in.extra_buf, options.mode);
    PutVarint32(&in.extra_buf, options.uid);
    PutVarint32(&in.extra_buf, options.gid);
    PutVarint32(&in.extra_buf, options.session_id);
    PutVarint64(&in.extra_buf, options.op_due);
    in.contents = Slice(in.extra_buf);
    if (in.contents.size() > sizeof(in.buf)) {
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
  if (!GetDirId(&input, &options.dir_id) ||
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
      s = mds_->Mkdir(options, &ret);
    } catch (Redirect& re) {
      out.extra_buf.swap(re);
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
  if (!kDebugRPC) {
    char* scratch = &in.buf[0];
    char* p = scratch;
    p = EncodeDirId(p, options.dir_id);
    p = EncodeLengthPrefixedSlice(p, options.name_hash);
    p = EncodeLengthPrefixedSlice(p, options.name);
    p = EncodeVarint32(p, options.session_id);
    p = EncodeVarint64(p, options.op_due);
    in.contents = Slice(scratch, p - scratch);
  } else {
    PutDirId(&in.extra_buf, options.dir_id);
    PutLengthPrefixedSlice(&in.extra_buf, options.name_hash);
    PutLengthPrefixedSlice(&in.extra_buf, options.name);
    PutVarint32(&in.extra_buf, options.session_id);
    PutVarint64(&in.extra_buf, options.op_due);
    in.contents = Slice(in.extra_buf);
    if (in.contents.size() > sizeof(in.buf)) {
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
  if (!GetDirId(&input, &options.dir_id) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      s = mds_->Lookup(options, &ret);
    } catch (Redirect& re) {
      out.extra_buf.swap(re);
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
  if (!kDebugRPC) {
    char* scratch = &in.buf[0];
    char* p = scratch;
    p = EncodeDirId(p, options.dir_id);
    p = EncodeLengthPrefixedSlice(p, options.name_hash);
    p = EncodeLengthPrefixedSlice(p, options.name);
    p = EncodeVarint32(p, options.mode);
    p = EncodeVarint32(p, options.session_id);
    p = EncodeVarint64(p, options.op_due);
    in.contents = Slice(scratch, p - scratch);
  } else {
    PutDirId(&in.extra_buf, options.dir_id);
    PutLengthPrefixedSlice(&in.extra_buf, options.name_hash);
    PutLengthPrefixedSlice(&in.extra_buf, options.name);
    PutVarint32(&in.extra_buf, options.mode);
    PutVarint32(&in.extra_buf, options.session_id);
    PutVarint64(&in.extra_buf, options.op_due);
    in.contents = Slice(in.extra_buf);
    if (in.contents.size() > sizeof(in.buf)) {
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
  if (!GetDirId(&input, &options.dir_id) ||
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
      out.extra_buf.swap(re);
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
  p = EncodeDirId(p, options.dir_id);
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
    std::vector<std::string>* names = ret->names;
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
  std::vector<std::string> names;
  ListdirRet ret;
  ret.names = &names;
  Slice input = in.contents;
  if (!GetDirId(&input, &options.dir_id) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    s = mds_->Listdir(options, &ret);
  }
  if (s.ok()) {
    PutVarint32(&out.extra_buf, names.size());
    for (std::vector<std::string>::iterator it = names.begin();
         it != names.end(); ++it) {
      PutLengthPrefixedSlice(&out.extra_buf, *it);
    }
    out.contents = Slice(out.extra_buf);
    out.err = 0;
  } else {
    out.err = s.err_code();
  }
}

Status MDS::RPC::CLI::Readidx(const ReadidxOptions& options, ReadidxRet* ret) {
  Status s;
  Msg in;
  char* scratch = &in.buf[0];
  char* p = scratch;
  p = EncodeDirId(p, options.dir_id);
  p = EncodeVarint32(p, options.session_id);
  p = EncodeVarint64(p, options.op_due);
  in.contents = Slice(scratch, p - scratch);
  Msg out;
  try {
    stub_->RDIDX(in, out);
  } catch (int rpc_err) {
    s = Status::Disconnected(Slice());
  }
  if (s.ok()) {
    if (out.err != 0) {
      s = Status::FromCode(out.err);
    } else {
      ret->idx.assign(out.contents.data(), out.contents.size());
    }
  }
  return s;
}

void MDS::RPC::SRV::RDIDX(Msg& in, Msg& out) {
  Status s;
  ReadidxOptions options;
  ReadidxRet ret;
  Slice input = in.contents;
  if (!GetDirId(&input, &options.dir_id) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    s = mds_->Readidx(options, &ret);
  }
  if (s.ok()) {
    out.extra_buf.swap(ret.idx);
    out.contents = Slice(out.extra_buf);
    out.err = 0;
  } else {
    out.err = s.err_code();
  }
}

Status MDS::RPC::CLI::Opensession(const OpensessionOptions& options,
                                  OpensessionRet* ret) {
  Status s;
  Msg in;
  Msg out;
  try {
    stub_->OPSES(in, out);
  } catch (int rpc_err) {
    s = Status::Disconnected(Slice());
  }
  if (s.ok()) {
    if (out.err != 0) {
      s = Status::FromCode(out.err);
    } else {
      Slice msg = out.contents;
      Slice env_name;
      Slice env_conf;
      uint32_t id;
      if (!GetLengthPrefixedSlice(&msg, &env_name) ||
          !GetLengthPrefixedSlice(&msg, &env_conf) || !GetVarint32(&msg, &id)) {
        s = Status::Corruption(Slice());
      } else {
        ret->env_name = env_name.ToString();
        ret->env_conf = env_conf.ToString();
        ret->session_id = id;
      }
    }
  }
  return s;
}

void MDS::RPC::SRV::OPSES(Msg& in, Msg& out) {
  Status s;
  OpensessionOptions options;
  OpensessionRet ret;
  s = mds_->Opensession(options, &ret);
  if (s.ok()) {
    PutLengthPrefixedSlice(&out.extra_buf, ret.env_name);
    PutLengthPrefixedSlice(&out.extra_buf, ret.env_conf);
    PutVarint32(&out.extra_buf, ret.session_id);
    out.contents = Slice(out.extra_buf);
    out.err = 0;
  } else {
    out.err = s.err_code();
  }
}

Status MDS::RPC::CLI::Getinput(const GetinputOptions& options,
                               GetinputRet* ret) {
  Status s;
  Msg in;
  Msg out;
  try {
    stub_->GINPT(in, out);
  } catch (int rpc_err) {
    s = Status::Disconnected(Slice());
  }
  if (s.ok()) {
    if (out.err != 0) {
      s = Status::FromCode(out.err);
    } else {
      Slice msg = out.contents;
      Slice info;
      if (!GetLengthPrefixedSlice(&msg, &info)) {
        s = Status::Corruption(Slice());
      } else {
        ret->info = info.ToString();
      }
    }
  }
  return s;
}

void MDS::RPC::SRV::GINPT(Msg& in, Msg& out) {
  Status s;
  GetinputOptions options;
  GetinputRet ret;
  s = mds_->Getinput(options, &ret);
  if (s.ok()) {
    PutLengthPrefixedSlice(&out.extra_buf, ret.info);
    out.contents = Slice(out.extra_buf);
    out.err = 0;
  } else {
    out.err = s.err_code();
  }
}

Status MDS::RPC::CLI::Getoutput(const GetoutputOptions& options,
                                GetoutputRet* ret) {
  Status s;
  Msg in;
  Msg out;
  try {
    stub_->GOUPT(in, out);
  } catch (int rpc_err) {
    s = Status::Disconnected(Slice());
  }
  if (s.ok()) {
    if (out.err != 0) {
      s = Status::FromCode(out.err);
    } else {
      Slice msg = out.contents;
      Slice info;
      if (!GetLengthPrefixedSlice(&msg, &info)) {
        s = Status::Corruption(Slice());
      } else {
        ret->info = info.ToString();
      }
    }
  }
  return s;
}

void MDS::RPC::SRV::GOUPT(Msg& in, Msg& out) {
  Status s;
  GetoutputOptions options;
  GetoutputRet ret;
  s = mds_->Getoutput(options, &ret);
  if (s.ok()) {
    PutLengthPrefixedSlice(&out.extra_buf, ret.info);
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

}  // namespace pdlfs
