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
    RPCCallback(handle);
  }

  // XXX: Shall we record background errors?
  return HG_SUCCESS;
}

hg_return_t MercuryRPC::RPCCallback(hg_handle_t handle) {
  If::Message input;
  If::Message output;
  hg_return_t ret = HG_Get_input(handle, &input);
  if (ret == HG_SUCCESS) {
    registered_data(handle)->fs_->Call(input, output);  // Execute callback
    ret = HG_Respond(handle, NULL, NULL, &output);
    HG_Free_input(handle, &input);
  }
  HG_Destroy(handle);
  return ret;
}

void MercuryRPC::Append(Timer* t) {
  t->next = &timers_;
  t->prev = timers_.prev;
  t->prev->next = t;
  t->next->prev = t;
}

void MercuryRPC::Remove(Timer* t) {
  t->next->prev = t->prev;
  t->prev->next = t->next;
}

void MercuryRPC::CheckTimers() {
  MutexLock ml(&mutex_);
  uint64_t now = env_->NowMicros();
  for (Timer* t = timers_.next; t != &timers_;) {
    Timer* next = t->next;
    if (now >= t->due) {
      Remove(t);
      HG_Cancel(t->handle);
      t->next = NULL;
      t->prev = NULL;
    }
    t = next;
  }
}

void MercuryRPC::AddTimerFor(hg_handle_t handle, Timer* timer) {
  MutexLock ml(&mutex_);
  timer->handle = handle;
  timer->due = env_->NowMicros() + rpc_timeout_;
  Append(timer);
}

void MercuryRPC::RemoveTimer(Timer* timer) {
  MutexLock ml(&mutex_);
  if (timer->next != NULL && timer->prev != NULL) {
    Remove(timer);
  }
}

namespace {
struct RPCState {
  bool rpc_done;
  port::Mutex* rpc_mu;
  port::CondVar* rpc_cv;
  hg_return_t ret;
  void* out;
};
}

static hg_return_t SaveReply(const hg_cb_info* info) {
  RPCState* state = reinterpret_cast<RPCState*>(info->arg);
  hg_handle_t handle = info->info.forward.handle;
  state->ret = info->ret;
  if (state->ret == HG_SUCCESS) {
    state->ret = HG_Get_output(handle, state->out);
    if (state->ret == HG_SUCCESS) {
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
      HG_Free_output(handle, state->out);
    }
  }

  state->rpc_mu->Lock();
  state->rpc_done = true;
  state->rpc_cv->SignalAll();
  state->rpc_mu->Unlock();
  return HG_SUCCESS;
}

Status MercuryRPC::Client::Call(Message& in, Message& out) RPCNOEXCEPT {
  AddrEntry* addr_entry = NULL;
  hg_return_t ret = rpc_->Lookup(addr_, &addr_entry);
  if (ret != HG_SUCCESS) return Status::Disconnected(Slice());
  assert(addr_entry != NULL);
  hg_addr_t addr = addr_entry->value->rep;
  hg_handle_t handle;
  ret = HG_Create(rpc_->hg_context_, addr, rpc_->hg_rpc_id_, &handle);
  if (ret == HG_SUCCESS) {
    RPCState state;
    state.rpc_done = false;
    state.rpc_mu = &mu_;
    state.rpc_cv = &cv_;
    state.out = &out;
    Timer timer;
    // XXX: HG_Forward is non-blocking so we wait on the callback
    ret = HG_Forward(handle, SaveReply, &state, &in);
    if (ret == HG_SUCCESS) {
      rpc_->AddTimerFor(handle, &timer);
      MutexLock ml(&mu_);
      while (!state.rpc_done) {
        cv_.Wait();
      }
      rpc_->RemoveTimer(&timer);
      ret = state.ret;
    }

    HG_Destroy(handle);
  }
  rpc_->Release(addr_entry);
  if (ret != HG_SUCCESS) {
    return Status::Disconnected(Slice());
  } else {
    return Status::OK();
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
      lookup_cv_(&mutex_),
      addr_cache_(options.addr_cache_size),
      refs_(0),
      rpc_timeout_(options.rpc_timeout),
      pool_(options.extra_workers),
      env_(options.env),
      fs_(options.fs) {
  hg_class_ = HG_Init(options.uri.c_str(), (listen) ? HG_TRUE : HG_FALSE);
  if (hg_class_) hg_context_ = HG_Context_create(hg_class_);
  if (hg_class_ == NULL || hg_context_ == NULL) {
    Error(__LOG_ARGS__, "hg init call failed");
    abort();
  } else {
    RegisterRPC();
  }

  timers_.prev = &timers_;
  timers_.next = &timers_;
}

MercuryRPC::~MercuryRPC() {
  mutex_.Lock();
  addr_cache_.Prune();  // Purge all cached addrs
  assert(addr_cache_.Empty());
  mutex_.Unlock();

  HG_Context_destroy(hg_context_);
  HG_Finalize(hg_class_);
}

namespace {
struct LookupState {
  bool lookup_done;
  port::Mutex* lookup_mu;
  port::CondVar* lookup_cv;
  hg_return_t ret;
  hg_addr_t addr;
};
}

static hg_return_t SaveAddr(const hg_cb_info* info) {
  LookupState* state = reinterpret_cast<LookupState*>(info->arg);
  state->ret = info->ret;
  if (state->ret == HG_SUCCESS) {
    state->addr = info->info.lookup.addr;
  }

  state->lookup_mu->Lock();
  state->lookup_done = true;
  state->lookup_cv->SignalAll();
  state->lookup_mu->Unlock();
  return HG_SUCCESS;
}

static void FreeAddr(const Slice& k, MercuryRPC::Addr* addr) {
  HG_Addr_free(addr->clazz, addr->rep);
  delete addr;
}

hg_return_t MercuryRPC::Lookup(const std::string& target, AddrEntry** result) {
  MutexLock ml(&mutex_);
  uint32_t hash = Hash(target.data(), target.size(), 0);
  AddrEntry* e = addr_cache_.Lookup(target, hash);
  if (e == NULL) {
    hg_return_t ret;
    mutex_.Unlock();
    LookupState state;
    state.lookup_done = false;
    state.lookup_mu = &mutex_;
    state.lookup_cv = &lookup_cv_;
    state.addr = NULL;
    ret = HG_Addr_lookup(hg_context_, SaveAddr, &state, target.c_str(),
                         HG_OP_ID_IGNORE);
    mutex_.Lock();
    if (ret == HG_SUCCESS) {
      while (!state.lookup_done) {
        lookup_cv_.Wait();
      }
      ret = state.ret;
      if (ret == HG_SUCCESS) {
        Addr* addr = new Addr(hg_class_, state.addr);
        e = addr_cache_.Insert(target, hash, addr, 1, FreeAddr);
      }
    }
    *result = e;
    return ret;
  } else {
    *result = e;
    return HG_SUCCESS;
  }
}

// Convenient methods written for Margo
MercuryRPC::AddrEntry* MercuryRPC::LookupCache(const std::string& target) {
  MutexLock ml(&mutex_);
  uint32_t hash = Hash(target.data(), target.size(), 0);
  return addr_cache_.Lookup(target, hash);
}

MercuryRPC::AddrEntry* MercuryRPC::Bind(const std::string& target, Addr* addr) {
  MutexLock ml(&mutex_);
  uint32_t hash = Hash(target.data(), target.size(), 0);
  return addr_cache_.Insert(target, hash, addr, 1, FreeAddr);
}

void MercuryRPC::Release(AddrEntry* entry) {
  MutexLock ml(&mutex_);
  addr_cache_.Release(entry);
}

std::string MercuryRPC::ToString(hg_addr_t addr) {
  char tmp[64];
  tmp[0] = 0;  // XXX: in case HG_Addr_to_string_fails()
  hg_size_t len = sizeof(tmp);
  HG_Addr_to_string(hg_class_, tmp, &len, addr);  // XXX: ignored ret val
  return tmp;
}

// If there is a single looping thread, both HG_Progress and HG_Trigger
// are called within that thread. If there are more than 1 threads,
// the first thread only calls HG_Progress and the rest threads
// call HG_Trigger.
void MercuryRPC::LocalLooper::BGLoop() {
  mutex_.Lock();
  int id = bg_id_++;
  mutex_.Unlock();
  const int timeout = 200;  // in milliseconds
  hg_context_t* ctx = rpc_->hg_context_;
  hg_return_t ret = HG_SUCCESS;
  int size = max_bg_loops_;

  while (true) {
    if (shutting_down_.Acquire_Load() || ret != HG_SUCCESS) {
      mutex_.Lock();
      assert(bg_loops_ > 0);
      bg_loops_--;
      bg_cv_.SignalAll();
      if (ret != HG_SUCCESS) {
        if (!status_.ok()) {
          status_ = Status::IOError(Slice());
        }
      }
      mutex_.Unlock();
      return;
    }

    if (id == 0) {
      ret = HG_Progress(ctx, timeout);
    }

    if (ret == HG_SUCCESS) {
      if (size <= 1 || id > 0) {
        unsigned int actual_count = 1;
        while (actual_count != 0 && !shutting_down_.Acquire_Load()) {
          if (id != 0) {
            ret = HG_Trigger(ctx, timeout, 1, &actual_count);
          } else {
            ret = HG_Trigger(ctx, 0, 1, &actual_count);
          }
          if (ret == HG_TIMEOUT) {
            ret = HG_SUCCESS;
            break;
          } else if (ret != HG_SUCCESS) {
            break;
          }
        }
      }
    } else if (ret == HG_TIMEOUT) {
      ret = HG_SUCCESS;
    }

    if (id == 0 && ret == HG_SUCCESS) {
      rpc_->CheckTimers();
    }

    if (ret != HG_SUCCESS) {
      Error(__LOG_ARGS__, "error during mercury rpc looping: %s (errno=%d)",
            HG_Error_to_string(ret), ret);
      if (ignore_rpc_error_) {
        ret = HG_SUCCESS;
      }
    }
  }
}

}  // namespace rpc
}  // namespace pdlfs
