/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/logging.h"
#include "pdlfs-common/pdlfs_config.h"
#if defined(MARGO) && defined(MERCURY)
#include "margo_rpc.h"

namespace pdlfs {
namespace rpc {

static void utl_rpc(void* handle) {
  MercuryRPC::RPCCallbackDecorator(reinterpret_cast<hg_handle*>(handle));
}

DEFINE_MARGO_RPC_HANDLER(utl_rpc)

void MargoRPC::RegisterRPC() {
  hg_rpc_id_ = HG_Register_name(hg_->hg_class_, "deltafs_ult_rpc",
                                MercuryRPC::RPCMessageCoder,
                                MercuryRPC::RPCMessageCoder, utl_rpc_handler);
  if (hg_->listen_) {
    HG_Register_data(hg_->hg_class_, hg_rpc_id_, hg_, NULL);
  }
}

void MargoRPC::Ref() { ++refs_; }

void MargoRPC::Unref() {
  --refs_;
  assert(refs_ >= 0);
  if (refs_ <= 0) {
    delete this;
  }
}

MargoRPC::MargoRPC(bool listen, const RPCOptions& options)
    : num_io_threads_(options.num_io_threads),
      rpc_timeout_(options.rpc_timeout),
      refs_(0) {
  margo_id_ = MARGO_INSTANCE_NULL;
  if (ABT_initialized() != 0) {
    Error(__LOG_ARGS__, "ABT not yet initialized");
    abort();
  } else {
    hg_ = new MercuryRPC(listen, options);
    RegisterRPC();
    hg_->Ref();
  }
}

Status MargoRPC::Start() {
  if (num_io_threads_ > 1) {
    margo_id_ = margo_init(true, num_io_threads_ - 1, hg_->hg_context_);
  } else {
    margo_id_ = margo_init(true, -1, hg_->hg_context_);
  }
  if (margo_id_ == MARGO_INSTANCE_NULL) {
    Error(__LOG_ARGS__, "margo init failed");
    abort();
  } else {
    return Status::OK();
  }
}

Status MargoRPC::Stop() {
  if (margo_id_ != MARGO_INSTANCE_NULL) {
    margo_finalize(margo_id_);
    margo_id_ = MARGO_INSTANCE_NULL;
  }
  return Status::OK();
}

MargoRPC::~MargoRPC() {
  Stop();
  hg_->Unref();
}

hg_return_t MargoRPC::Lookup(const std::string& target, AddrEntry** result) {
  AddrEntry* e = hg_->LookupCache(target);
  if (e == NULL) {
    hg_addr_t res;
    hg_return_t ret = margo_addr_lookup(margo_id_, target.c_str(), &res);
    if (ret == HG_SUCCESS) {
      Addr* addr = new Addr(hg_->hg_class_, res);
      e = hg_->Bind(target, addr);
    }
    *result = e;
    return ret;
  } else {
    *result = e;
    return HG_SUCCESS;
  }
}

void MargoRPC::Client::Call(Message& in, Message& out) {
  AddrEntry* entry = NULL;
  hg_return_t r = rpc_->Lookup(addr_, &entry);
  if (r != HG_SUCCESS) throw EHOSTUNREACH;
  assert(entry != NULL);
  hg_addr_t addr = entry->value->rep;
  hg_handle_t handle;
  hg_return_t ret =
      HG_Create(rpc_->hg_->hg_context_, addr, rpc_->hg_rpc_id_, &handle);
  if (ret == HG_SUCCESS) {
    ret = margo_forward_timed(rpc_->margo_id_, handle, &in,
                              rpc_->rpc_timeout_ / 1000);
    if (ret == HG_SUCCESS) {
      ret = HG_Get_output(handle, &out);
    }
    HG_Destroy(handle);
  }
  rpc_->Release(entry);
  if (ret != HG_SUCCESS) {
    throw ENETUNREACH;
  }
}

}  // namespace rpc
}  // namespace pdlfs

#endif
