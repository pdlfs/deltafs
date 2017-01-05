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

MDSMonitor::~MDSMonitor() {}

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
#define DELTAFS_RPC_DEBUG 1
#else
#define DELTAFS_RPC_DEBUG 0
#endif  // NDEBUG

static const bool kDebugRPC = DELTAFS_RPC_DEBUG;
#undef DELTAFS_RPC_DEBUG
// RPC op types
namespace {
/* clang-format off */
enum {
  kNonop, kFstat, kFcreat, kMkdir,
  kChmod, kChown, kUperm, kUtime, kTrunc,
  kUnlink, kLookup, kListdir, kReadidx,
  kOpensession,
  kGetinput,
  kGetoutput
};
/* clang-format on */
}  // namespace

// Convenient method that adds op code to a message.
static inline rpc::If::Message& AddOp(rpc::If::Message& msg, int op) {
  msg.op = op;
  return msg;
}

// RPC dispatcher
Status MDS::RPC::SRV::Call(Msg& in, Msg& out) RPCNOEXCEPT {
  switch (in.op) {
    case kLookup:
      LOKUP(in, out);
      break;
    case kFstat:
      FSTAT(in, out);
      break;
    case kFcreat:
      FCRET(in, out);
      break;
    case kTrunc:
      TRUNC(in, out);
      break;
    case kUnlink:
      UNLNK(in, out);
      break;
    case kMkdir:
      MKDIR(in, out);
      break;
    case kChmod:
      CHMOD(in, out);
      break;
    case kChown:
      CHOWN(in, out);
      break;
    case kUperm:
      UPERM(in, out);
      break;
    case kUtime:
      UTIME(in, out);
      break;
    case kListdir:
      LSDIR(in, out);
      break;
    case kReadidx:
      RDIDX(in, out);
      break;
    case kOpensession:
      OPSES(in, out);
      break;
    case kGetinput:
      GINPT(in, out);
      break;
    case kGetoutput:
      GOUPT(in, out);
      break;
    case kNonop:
      out.err = 0;
      break;
    default:
      out.err = Status::kNotSupported;
  }

  return Status::OK();
}

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
    s = stub_->Call(AddOp(in, kFstat), out);
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
  assert(in.op == kFstat);
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
    p = EncodeVarint32(p, options.flags);
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
    PutVarint32(&in.extra_buf, options.flags);
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
    s = stub_->Call(AddOp(in, kFcreat), out);
    if (s.ok()) {
      Slice contents = out.contents;
      if (out.err == -1) {
        Redirect re(contents.data(), contents.size());
        throw re;
      } else if (out.err != 0) {
        s = Status::FromCode(out.err);
      } else if (!ret->stat.DecodeFrom(&contents)) {
        s = Status::Corruption(Slice());
      } else if (contents.empty()) {
        s = Status::Corruption(Slice());
      } else {
        ret->created = contents[0];
      }
    }
  }
  return s;
}

void MDS::RPC::SRV::FCRET(Msg& in, Msg& out) {
  Status s;
  FcreatOptions options;
  FcreatRet ret;
  assert(in.op == kFcreat);
  Slice input = in.contents;
  if (!GetDirId(&input, &options.dir_id) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint32(&input, &options.flags) ||
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
    out.buf[out.contents.size()] = static_cast<char>(ret.created);
    out.contents = Slice(out.buf, out.contents.size() + 1);
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
    p = EncodeVarint32(p, options.flags);
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
    PutVarint32(&in.extra_buf, options.flags);
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
    s = stub_->Call(AddOp(in, kMkdir), out);
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
  assert(in.op == kMkdir);
  Slice input = in.contents;
  if (!GetDirId(&input, &options.dir_id) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint32(&input, &options.flags) ||
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
    s = stub_->Call(AddOp(in, kLookup), out);
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
  assert(in.op == kLookup);
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
    s = stub_->Call(AddOp(in, kChmod), out);
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
  assert(in.op == kChmod);
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

Status MDS::RPC::CLI::Chown(const ChownOptions& options, ChownRet* ret) {
  Status s;
  Msg in;
  if (!kDebugRPC) {
    char* scratch = &in.buf[0];
    char* p = scratch;
    p = EncodeDirId(p, options.dir_id);
    p = EncodeLengthPrefixedSlice(p, options.name_hash);
    p = EncodeLengthPrefixedSlice(p, options.name);
    p = EncodeVarint32(p, options.uid);
    p = EncodeVarint32(p, options.gid);
    p = EncodeVarint32(p, options.session_id);
    p = EncodeVarint64(p, options.op_due);
    in.contents = Slice(scratch, p - scratch);
  } else {
    PutDirId(&in.extra_buf, options.dir_id);
    PutLengthPrefixedSlice(&in.extra_buf, options.name_hash);
    PutLengthPrefixedSlice(&in.extra_buf, options.name);
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
    s = stub_->Call(AddOp(in, kChown), out);
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

void MDS::RPC::SRV::CHOWN(Msg& in, Msg& out) {
  Status s;
  ChownOptions options;
  ChownRet ret;
  assert(in.op == kChown);
  Slice input = in.contents;
  if (!GetDirId(&input, &options.dir_id) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint32(&input, &options.uid) ||
      !GetVarint32(&input, &options.gid) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      s = mds_->Chown(options, &ret);
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

Status MDS::RPC::CLI::Uperm(const UpermOptions& options, UpermRet* ret) {
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
    s = stub_->Call(AddOp(in, kUperm), out);
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

void MDS::RPC::SRV::UPERM(Msg& in, Msg& out) {
  Status s;
  UpermOptions options;
  UpermRet ret;
  assert(in.op == kUperm);
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
      s = mds_->Uperm(options, &ret);
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

Status MDS::RPC::CLI::Utime(const UtimeOptions& options, UtimeRet* ret) {
  Status s;
  Msg in;
  if (!kDebugRPC) {
    char* scratch = &in.buf[0];
    char* p = scratch;
    p = EncodeDirId(p, options.dir_id);
    p = EncodeLengthPrefixedSlice(p, options.name_hash);
    p = EncodeLengthPrefixedSlice(p, options.name);
    p = EncodeVarint64(p, options.atime);
    p = EncodeVarint64(p, options.mtime);
    p = EncodeVarint32(p, options.session_id);
    p = EncodeVarint64(p, options.op_due);
    in.contents = Slice(scratch, p - scratch);
  } else {
    PutDirId(&in.extra_buf, options.dir_id);
    PutLengthPrefixedSlice(&in.extra_buf, options.name_hash);
    PutLengthPrefixedSlice(&in.extra_buf, options.name);
    PutVarint64(&in.extra_buf, options.atime);
    PutVarint64(&in.extra_buf, options.mtime);
    PutVarint32(&in.extra_buf, options.session_id);
    PutVarint64(&in.extra_buf, options.op_due);
    in.contents = Slice(in.extra_buf);
    if (in.contents.size() > sizeof(in.buf)) {
      s = Status::BufferFull(Slice());
    }
  }

  Msg out;
  if (s.ok()) {
    s = stub_->Call(AddOp(in, kUtime), out);
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

void MDS::RPC::SRV::UTIME(Msg& in, Msg& out) {
  Status s;
  UtimeOptions options;
  UtimeRet ret;
  assert(in.op == kUtime);
  Slice input = in.contents;
  if (!GetDirId(&input, &options.dir_id) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint64(&input, &options.atime) ||
      !GetVarint64(&input, &options.mtime) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      s = mds_->Utime(options, &ret);
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

Status MDS::RPC::CLI::Trunc(const TruncOptions& options, TruncRet* ret) {
  Status s;
  Msg in;
  if (!kDebugRPC) {
    char* scratch = &in.buf[0];
    char* p = scratch;
    p = EncodeDirId(p, options.dir_id);
    p = EncodeLengthPrefixedSlice(p, options.name_hash);
    p = EncodeLengthPrefixedSlice(p, options.name);
    p = EncodeVarint64(p, options.mtime);
    p = EncodeVarint64(p, options.size);
    p = EncodeVarint32(p, options.session_id);
    p = EncodeVarint64(p, options.op_due);
    in.contents = Slice(scratch, p - scratch);
  } else {
    PutDirId(&in.extra_buf, options.dir_id);
    PutLengthPrefixedSlice(&in.extra_buf, options.name_hash);
    PutLengthPrefixedSlice(&in.extra_buf, options.name);
    PutVarint64(&in.extra_buf, options.mtime);
    PutVarint64(&in.extra_buf, options.size);
    PutVarint32(&in.extra_buf, options.session_id);
    PutVarint64(&in.extra_buf, options.op_due);
    in.contents = Slice(in.extra_buf);
    if (in.contents.size() > sizeof(in.buf)) {
      s = Status::BufferFull(Slice());
    }
  }

  Msg out;
  if (s.ok()) {
    s = stub_->Call(AddOp(in, kTrunc), out);
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

void MDS::RPC::SRV::TRUNC(Msg& in, Msg& out) {
  Status s;
  TruncOptions options;
  TruncRet ret;
  assert(in.op == kTrunc);
  Slice input = in.contents;
  if (!GetDirId(&input, &options.dir_id) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint64(&input, &options.mtime) ||
      !GetVarint64(&input, &options.size) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      s = mds_->Trunc(options, &ret);
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

Status MDS::RPC::CLI::Unlink(const UnlinkOptions& options, UnlinkRet* ret) {
  Status s;
  Msg in;
  if (!kDebugRPC) {
    char* scratch = &in.buf[0];
    char* p = scratch;
    p = EncodeDirId(p, options.dir_id);
    p = EncodeLengthPrefixedSlice(p, options.name_hash);
    p = EncodeLengthPrefixedSlice(p, options.name);
    p = EncodeVarint32(p, options.flags);
    p = EncodeVarint32(p, options.session_id);
    p = EncodeVarint64(p, options.op_due);
    in.contents = Slice(scratch, p - scratch);
  } else {
    PutDirId(&in.extra_buf, options.dir_id);
    PutLengthPrefixedSlice(&in.extra_buf, options.name_hash);
    PutLengthPrefixedSlice(&in.extra_buf, options.name);
    PutVarint32(&in.extra_buf, options.flags);
    PutVarint32(&in.extra_buf, options.session_id);
    PutVarint64(&in.extra_buf, options.op_due);
    in.contents = Slice(in.extra_buf);
    if (in.contents.size() > sizeof(in.buf)) {
      s = Status::BufferFull(Slice());
    }
  }

  Msg out;
  if (s.ok()) {
    s = stub_->Call(AddOp(in, kUnlink), out);
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

void MDS::RPC::SRV::UNLNK(Msg& in, Msg& out) {
  Status s;
  UnlinkOptions options;
  UnlinkRet ret;
  assert(in.op == kUnlink);
  Slice input = in.contents;
  if (!GetDirId(&input, &options.dir_id) ||
      !GetLengthPrefixedSlice(&input, &options.name_hash) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetVarint32(&input, &options.flags) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    try {
      s = mds_->Unlink(options, &ret);
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
  s = stub_->Call(AddOp(in, kListdir), out);
  if (s.ok()) {
    std::vector<std::string>* names = ret->names;
    if (out.err != 0) {
      s = Status::FromCode(out.err);
    } else {
      Slice name;
      Slice encoding = out.contents;
      if (encoding.size() < 4) {
        s = Status::Corruption(Slice());
      } else {
        uint32_t num = DecodeFixed32(encoding.data() + encoding.size() - 4);
        encoding.remove_suffix(4);
        while (num-- != 0) {
          if (GetLengthPrefixedSlice(&encoding, &name)) {
            names->push_back(name.ToString());
          } else {
            s = Status::Corruption(Slice());
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
  assert(in.op == kListdir);
  Slice input = in.contents;
  if (!GetDirId(&input, &options.dir_id) ||
      !GetVarint32(&input, &options.session_id) ||
      !GetVarint64(&input, &options.op_due)) {
    s = Status::InvalidArgument(Slice());
  } else {
    s = mds_->Listdir(options, &ret);
  }
  if (s.ok()) {
    size_t num_entries = 0;
    for (std::vector<std::string>::iterator it = names.begin();
         it != names.end(); ++it) {
      PutLengthPrefixedSlice(&out.extra_buf, *it);
      num_entries++;
      if (out.extra_buf.size() >= 1000) {
        break;  // Silently discard rest entries
      }
    }
    PutFixed32(&out.extra_buf, num_entries);
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
  s = stub_->Call(AddOp(in, kReadidx), out);
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
  assert(in.op == kReadidx);
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
  s = stub_->Call(AddOp(in, kOpensession), out);
  if (s.ok()) {
    if (out.err != 0) {
      s = Status::FromCode(out.err);
    } else {
      Slice msg = out.contents;
      Slice env_name;
      Slice env_conf;
      Slice fio_name;
      Slice fio_conf;
      uint32_t id;
      if (!GetLengthPrefixedSlice(&msg, &env_name) ||
          !GetLengthPrefixedSlice(&msg, &env_conf) ||
          !GetLengthPrefixedSlice(&msg, &fio_name) ||
          !GetLengthPrefixedSlice(&msg, &fio_conf) || !GetVarint32(&msg, &id)) {
        s = Status::Corruption(Slice());
      } else {
        ret->env_name = env_name.ToString();
        ret->env_conf = env_conf.ToString();
        ret->fio_name = fio_name.ToString();
        ret->fio_conf = fio_conf.ToString();
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
  assert(in.op == kOpensession);
  s = mds_->Opensession(options, &ret);
  if (s.ok()) {
    PutLengthPrefixedSlice(&out.extra_buf, ret.env_name);
    PutLengthPrefixedSlice(&out.extra_buf, ret.env_conf);
    PutLengthPrefixedSlice(&out.extra_buf, ret.fio_name);
    PutLengthPrefixedSlice(&out.extra_buf, ret.fio_conf);
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
  s = stub_->Call(AddOp(in, kGetinput), out);
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
  assert(in.op == kGetinput);
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
  s = stub_->Call(AddOp(in, kGetoutput), out);
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
  assert(in.op == kGetoutput);
  s = mds_->Getoutput(options, &ret);
  if (s.ok()) {
    PutLengthPrefixedSlice(&out.extra_buf, ret.info);
    out.contents = Slice(out.extra_buf);
    out.err = 0;
  } else {
    out.err = s.err_code();
  }
}

void MDSMonitor::Reset() {
  Reset_Fstat_count();
  Reset_Fcreat_count();
  Reset_Mkdir_count();
  Reset_Chmod_count();
  Reset_Chown_count();
  Reset_Uperm_count();
  Reset_Utime_count();
  Reset_Trunc_count();
  Reset_Unlink_count();
  Reset_Lookup_count();
  Reset_Listdir_count();
  Reset_Readidx_count();
}

}  // namespace pdlfs
