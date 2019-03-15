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

#include "pdlfs-common/logging.h"
#include "pdlfs-common/pdlfs_config.h"

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

Status MargoRPC::Client::Call(Message& in, Message& out) RPCNOEXCEPT {
  AddrEntry* entry = NULL;
  hg_return_t ret = rpc_->Lookup(addr_, &entry);
  if (ret != HG_SUCCESS) return Status::Disconnected(Slice());
  assert(entry != NULL);
  hg_addr_t addr = entry->value->rep;
  hg_handle_t handle;
  ret = HG_Create(rpc_->hg_->hg_context_, addr, rpc_->hg_rpc_id_, &handle);
  if (ret == HG_SUCCESS) {
    ret = margo_forward_timed(rpc_->margo_id_, handle, &in,
                              rpc_->rpc_timeout_ / 1000);
    if (ret == HG_SUCCESS) {
      ret = HG_Get_output(handle, &out);
      if (ret == HG_SUCCESS) {
        /*
         * XXX: HG_Get_output() adds a reference to the handle.
         * we need to call HG_Free_output() to drop this reference.
         * this will also call the prog with the HG_FREE op.  that
         * would free anything we malloc'd and attached to state->out.
         * that would be a problem (since we are not finished with
         * state->out yet), but we know that our proc function does
         * not malloc anything (see RPCMessageCoder() above here).
         * instead it copies all the data out of the handle into state->out.
         * in fact, HG_FREE doesn't do anything in RPCMessageCoder().
         * so for this special case, it is safe to call HG_Free_output()
         * directly after calling HG_Get_output().  (longer term
         * our RPC framework may need an additional call for clients
         * to indicate they are done so anything malloc'd on the out
         * structure can be freed...)
         */
        HG_Free_output(handle, &out);
      }
    }
    HG_Destroy(handle);
  }
  rpc_->Release(entry);
  if (ret != HG_SUCCESS) {
    return Status::Disconnected(Slice());
  } else {
    return Status::OK();
  }
}

}  // namespace rpc
}  // namespace pdlfs
