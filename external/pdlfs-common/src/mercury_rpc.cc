/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/logging.h"
#if defined(MERCURY)
#include "mercury_rpc.h"

namespace pdlfs {
namespace rpc {

hg_return_t MercuryRPC::RPCMessageCoder(hg_proc_t proc, void* data) {
  hg_return_t ret;
  If::Message* msg = reinterpret_cast<If::Message*>(data);
  hg_proc_op_t op = hg_proc_get_op(proc);

  switch (op) {
    case HG_ENCODE: {
      hg_int8_t op_code = static_cast<int8_t>(msg->op);
      ret = hg_proc_hg_int8_t(proc, &op_code);
      if (ret == HG_SUCCESS) {
        hg_int8_t err_code = static_cast<int8_t>(msg->err);
        ret = hg_proc_hg_int8_t(proc, &err_code);
        if (ret == HG_SUCCESS) {
          hg_uint16_t len = static_cast<uint16_t>(msg->contents.size());
          ret = hg_proc_hg_uint16_t(proc, &len);
          if (ret == HG_SUCCESS) {
            if (len > 0) {
              char* p = const_cast<char*>(&msg->contents[0]);
              ret = hg_proc_memcpy(proc, p, len);
            }
          }
        }
      }
      break;
    }

    case HG_DECODE: {
      hg_int8_t op_code;
      ret = hg_proc_hg_int8_t(proc, &op_code);
      if (ret == HG_SUCCESS) {
        msg->op = op_code;
        hg_int8_t err;
        ret = hg_proc_hg_int8_t(proc, &err);
        if (ret == HG_SUCCESS) {
          msg->err = err;
          hg_uint16_t len;
          ret = hg_proc_hg_uint16_t(proc, &len);
          if (ret == HG_SUCCESS) {
            if (len > 0) {
              char* p;
              if (len <= sizeof(msg->buf)) {
                p = &msg->buf[0];
              } else {
                // Hacking std::string to avoid an extra copy of data
                msg->extra_buf.reserve(len);
                msg->extra_buf.resize(1);
                p = &msg->extra_buf[0];
              }
              ret = hg_proc_memcpy(proc, p, len);
              msg->contents = Slice(p, len);
            }
          }
        }
      }
      break;
    }

    default:
      ret = HG_SUCCESS;
  }

  return ret;
}

hg_return_t MercuryRPC::RPCCallbackDecorator(hg_handle_t handle) {
  MercuryRPC* rpc = registered_data(handle);
  if (rpc->pool_ != NULL) {
    rpc->pool_->Schedule(RPCWrapper, handle);
    return HG_SUCCESS;
  } else {
    return RPCCallback(handle);
  }
}

hg_return_t MercuryRPC::RPCCallback(hg_handle_t handle) {
  If::Message input;
  If::Message output;
  hg_return_t ret = HG_Get_input(handle, &input);
  if (ret == HG_SUCCESS) {
    registered_data(handle)->fs_->Call(input, output);
    ret = HG_Respond(handle, NULL, NULL, &output);
  }
  HG_Destroy(handle);
  return ret;
}

namespace {
struct RPCState {
  bool reply_received;
  port::Mutex* mutex;
  port::CondVar* cv;
  hg_handle_t handle;
  hg_return_t ret;
  void* out;
};
}  // namespace

hg_return_t MercuryRPC::Client::SaveReply(const hg_cb_info* info) {
  RPCState* state = reinterpret_cast<RPCState*>(info->arg);
  MutexLock l(state->mutex);
  state->ret = info->ret;
  if (state->ret == HG_SUCCESS) {
    state->ret = HG_Get_output(state->handle, state->out);
  }
  state->reply_received = true;
  state->cv->SignalAll();
  return HG_SUCCESS;
}

void MercuryRPC::Client::Call(Message& in, Message& out) {
  hg_addr_t hg_addr;
  hg_return_t r = rpc_->Lookup(addr_, &hg_addr);
  if (r != HG_SUCCESS) throw EHOSTUNREACH;
  hg_handle_t handle;
  hg_return_t ret =
      HG_Create(rpc_->hg_context_, hg_addr, rpc_->hg_rpc_id_, &handle);
  if (ret == HG_SUCCESS) {
    RPCState state;
    state.out = &out;
    state.mutex = &mu_;
    state.cv = &cv_;
    state.reply_received = false;
    state.handle = handle;
    MutexLock l(state.mutex);
    ret = HG_Forward(handle, SaveReply, &state, &in);
    if (ret == HG_SUCCESS) {
      while (!state.reply_received) state.cv->Wait();
    }
    HG_Destroy(handle);
  }
  if (ret != HG_SUCCESS) {
    throw ENETUNREACH;
  }
}


void MercuryRPC::Ref() { ++refs_; }

void MercuryRPC::Unref() {
  --refs_;
  assert(refs_ >= 0);
  if (refs_ <= 0) {
    delete this;
  }
}

MercuryRPC::MercuryRPC(bool listen, const RPCOptions& options)
    : listen_(listen),
      env_(options.env),
      fs_(options.fs),
      refs_(0),
      pool_(options.extra_workers),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      bg_loop_running_(false),
      bg_error_(false),
      lookup_cv_(&mutex_) {
  assert(!options.uri.empty());

  hg_class_ = HG_Init(options.uri.c_str(), (listen) ? HG_TRUE : HG_FALSE);
  if (hg_class_) hg_context_ = HG_Context_create(hg_class_);
  if (hg_class_ == NULL || hg_context_ == NULL) {
    Error(__LOG_ARGS__, "hg init call failed");
    abort();
  }

  RegisterRPC();
}

Status MercuryRPC::TEST_Start() {
  MutexLock l(&mutex_);
  if (bg_loop_running_) {
    return Status::AlreadyExists(Slice());
  } else {
    bg_loop_running_ = true;
    env_->StartThread(TEST_LoopForever, this);
    return Status::OK();
  }
}

Status MercuryRPC::TEST_Stop() {
  MutexLock l(&mutex_);
  shutting_down_.Release_Store(this);
  // Wait until the background thread stops
  while (bg_loop_running_) {
    bg_cv_.Wait();
  }
  return Status::OK();
}

MercuryRPC::~MercuryRPC() {
  mutex_.Lock();
  shutting_down_.Release_Store(this);
  // Wait until the background thread stops
  while (bg_loop_running_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  for (AddrTable::iterator it = addrs_.begin(); it != addrs_.end(); ++it) {
    HG_Addr_free(hg_class_, it->second);
  }

  HG_Context_destroy(hg_context_);
  HG_Finalize(hg_class_);
}

namespace {
struct LookupState {
  const std::string* addr;
  MercuryRPC* rpc;
  hg_return_t ret;
  bool ok;
};
}  // namespace

hg_return_t MercuryRPC::SaveAddr(const hg_cb_info* info) {
  LookupState* state = reinterpret_cast<LookupState*>(info->arg);
  MercuryRPC* const rpc = state->rpc;
  state->ret = info->ret;

  MutexLock l(&rpc->mutex_);
  if (state->ret == HG_SUCCESS) {
    hg_addr_t result = info->info.lookup.addr;
    if (rpc->addrs_.find(*state->addr) == rpc->addrs_.end()) {
      rpc->addrs_.insert(std::make_pair(*state->addr, result));
    } else {
      HG_Addr_free(rpc->hg_class_, result);
    }
  }

  state->ok = true;
  rpc->lookup_cv_.SignalAll();
  return HG_SUCCESS;
}

hg_return_t MercuryRPC::Lookup(const std::string& addr, hg_addr_t* result) {
  MutexLock l(&mutex_);
  AddrTable::iterator it = addrs_.find(addr);
  if (it != addrs_.end()) {
    *result = it->second;
    return HG_SUCCESS;
  } else {
    LookupState state;
    state.rpc = this;
    state.addr = &addr;
    state.ok = false;
    HG_Addr_lookup(hg_context_, SaveAddr, &state, addr.c_str(),
                   HG_OP_ID_IGNORE);
    while (!state.ok) {
      lookup_cv_.Wait();
    }
    if (state.ret == HG_SUCCESS) {
      it = addrs_.find(addr);
      assert(it != addrs_.end());
      *result = it->second;
    }
    return state.ret;
  }
}

std::string MercuryRPC::ToString(hg_addr_t addr) {
  std::string rv;
  char tmp[64];
  tmp[0] = 0;    // XXX: in case HG_Addr_to_string_fails()
  hg_size_t len = sizeof(tmp);
  HG_Addr_to_string(hg_class_, tmp, &len, addr);  // XXX: ignored ret val
  rv = tmp;
  return rv;
}

void MercuryRPC::TEST_LoopForever(void* arg) {
  MercuryRPC* rpc = reinterpret_cast<MercuryRPC*>(arg);
  port::AtomicPointer* shutting_down = &rpc->shutting_down_;

  hg_return_t ret;
  unsigned int actual_count;
  bool error = false;

  while (!error && !shutting_down->Acquire_Load()) {
    do {
      actual_count = 0;
      ret = HG_Trigger(rpc->hg_context_, 0, 1, &actual_count);
      if (ret != HG_SUCCESS && ret != HG_TIMEOUT) {
        error = true;
      }
    } while (!error && actual_count != 0 && !shutting_down->Acquire_Load());

    if (!error && !shutting_down->Acquire_Load()) {
      ret = HG_Progress(rpc->hg_context_, 1000);
      if (ret != HG_SUCCESS && ret != HG_TIMEOUT) {
        error = true;
      }
    }
  }

  rpc->mutex_.Lock();
  rpc->bg_loop_running_ = false;
  rpc->bg_error_ = error;
  rpc->bg_cv_.SignalAll();
  rpc->mutex_.Unlock();

  if (error) {
    Error(__LOG_ARGS__, "Error in local RPC bg_loop [errno=%d]", ret);
  }
}

void MercuryRPC::LocalLooper::BGLoop() {
  hg_context_t* ctx = rpc_->hg_context_;
  hg_return_t ret = HG_SUCCESS;

  while (true) {
    if (shutting_down_.Acquire_Load() || ret != HG_SUCCESS) {
      mutex_.Lock();
      bg_loops_--;
      assert(bg_loops_ >= 0);
      bg_cv_.SignalAll();
      mutex_.Unlock();

      if (ret != HG_SUCCESS) {
        Error(__LOG_ARGS__, "Error in local RPC bg_loop [errno=%d]", ret);
      }
      return;
    }

    ret = HG_Progress(ctx, 1000);  // Timeouts in 1000 ms
    if (ret == HG_SUCCESS) {
      unsigned int actual_count = 1;
      while (actual_count != 0 && !shutting_down_.Acquire_Load()) {
        ret = HG_Trigger(ctx, 0, 1, &actual_count);
        if (ret == HG_TIMEOUT) {
          ret = HG_SUCCESS;
          break;
        } else if (ret != HG_SUCCESS) {
          break;
        }
      }
    } else if (ret == HG_TIMEOUT) {
      ret = HG_SUCCESS;
    }
  }
}

}  // namespace rpc
}  // namespace pdlfs

#endif
