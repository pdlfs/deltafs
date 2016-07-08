/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/logging.h"
#if defined(MERCURY)
#include "mercury_rpc.h"

namespace pdlfs {
namespace rpc {

#define LOG_ERROR(msg, ret) \
  Error(Logger::Default(), __FILE__, __LINE__, msg, ret)

hg_return_t MercuryRPC::If_Message_cb(hg_proc_t proc, void* data) {
  hg_return_t ret;
  If::Message* msg = reinterpret_cast<If::Message*>(data);
  hg_proc_op_t op = hg_proc_get_op(proc);

  switch (op) {
    case HG_ENCODE: {
      hg_int8_t err = msg->err;
      ret = hg_proc_hg_int8_t(proc, &err);
      if (ret == HG_SUCCESS) {
        hg_uint16_t len = msg->contents.size();
        ret = hg_proc_hg_uint16_t(proc, &len);
        if (ret == HG_SUCCESS) {
          if (len > 0) {
            char* p = const_cast<char*>(&msg->contents[0]);
            ret = hg_proc_memcpy(proc, p, len);
          }
        }
      }
      break;
    }

    case HG_DECODE: {
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
              msg->extra_buf.reserve(len);
              msg->extra_buf.resize(1);
              p = &msg->extra_buf[0];
            }
            ret = hg_proc_memcpy(proc, p, len);
            msg->contents = Slice(p, len);
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

#define SRV_CB(OP)                                                  \
  hg_return_t MercuryRPC::If_##OP##_decorator(hg_handle_t handle) { \
    MercuryRPC* rpc = registered_data(handle);                      \
    if (rpc->pool_ == NULL) {                                       \
      return If_##OP##_cb(handle);                                  \
    } else {                                                        \
      rpc->pool_->Schedule(If_##OP##_wrapper, handle);              \
      return HG_SUCCESS;                                            \
    }                                                               \
  }                                                                 \
                                                                    \
  hg_return_t MercuryRPC::If_##OP##_cb(hg_handle_t handle) {        \
    If::Message input;                                              \
    If::Message output;                                             \
    hg_return_t ret = HG_Get_input(handle, &input);                 \
    if (ret == HG_SUCCESS) {                                        \
      registered_data(handle)->fs_->OP(input, output);              \
      ret = HG_Respond(handle, NULL, NULL, &output);                \
    }                                                               \
    HG_Destroy(handle);                                             \
    return ret;                                                     \
  }

SRV_CB(NONOP)
SRV_CB(FSTAT)
SRV_CB(MKDIR)
SRV_CB(FCRET)
SRV_CB(CHMOD)
SRV_CB(CHOWN)
SRV_CB(UNLNK)
SRV_CB(RMDIR)
SRV_CB(RENME)
SRV_CB(LOKUP)
SRV_CB(LSDIR)
SRV_CB(RDIDX)

#undef SRV_CB

namespace {
struct OpState {
  bool reply_received;
  port::Mutex* mutex;
  port::CondVar* cv;
  hg_handle_t handle;
  hg_return_t ret;
  void* out;
};
}  // namespace

hg_return_t MercuryRPC::Client::SaveReply(const hg_cb_info* info) {
  OpState* state = reinterpret_cast<OpState*>(info->arg);
  MutexLock l(state->mutex);
  state->ret = info->ret;
  if (state->ret == HG_SUCCESS) {
    state->ret = HG_Get_output(state->handle, state->out);
  }
  state->reply_received = true;
  state->cv->SignalAll();
  return HG_SUCCESS;
}

#define CLI_STUB(OP)                                                         \
  void MercuryRPC::Client::OP(Message& in, Message& out) {                   \
    na_addr_t na_addr;                                                       \
    na_return_t r = rpc_->Lookup(addr_, &na_addr);                           \
    if (r != NA_SUCCESS) throw EHOSTUNREACH;                                 \
    hg_handle_t handle;                                                      \
    hg_return_t ret =                                                        \
        HG_Create(rpc_->hg_context_, na_addr, rpc_->hg_##OP##_id_, &handle); \
    if (ret == HG_SUCCESS) {                                                 \
      OpState state;                                                         \
      state.out = &out;                                                      \
      state.mutex = &mu_;                                                    \
      state.cv = &cv_;                                                       \
      state.reply_received = false;                                          \
      state.handle = handle;                                                 \
      MutexLock l(state.mutex);                                              \
      ret = HG_Forward(handle, SaveReply, &state, &in);                      \
      if (ret == HG_SUCCESS) {                                               \
        while (!state.reply_received) state.cv->Wait();                      \
      }                                                                      \
      HG_Destroy(handle);                                                    \
    }                                                                        \
    if (ret != HG_SUCCESS) {                                                 \
      throw ENETUNREACH;                                                     \
    }                                                                        \
  }

CLI_STUB(NONOP)
CLI_STUB(FSTAT)
CLI_STUB(MKDIR)
CLI_STUB(FCRET)
CLI_STUB(CHMOD)
CLI_STUB(CHOWN)
CLI_STUB(UNLNK)
CLI_STUB(RMDIR)
CLI_STUB(RENME)
CLI_STUB(LOKUP)
CLI_STUB(LSDIR)
CLI_STUB(RDIDX)

#undef CLI_STUB

template <typename T>
T* MercuryCall(const char* label, T* ptr) {
  if (ptr == NULL) {
    fprintf(stderr, "mercury %s failed\n", label);
    abort();
  } else {
    return ptr;
  }
}

#define Mercury_NA_Initialize(a, b) \
  MercuryCall("NA_Initialize", NA_Initialize(a, b))
#define Mercury_NA_Context_create(a) \
  MercuryCall("NA_Context_create", NA_Context_create(a));
#define Mercury_HG_Init_na(a, b) MercuryCall("HG_Init_na", HG_Init_na(a, b))
#define Mercury_HG_Context_create(a) \
  MercuryCall("HG_Context_create", HG_Context_create(a))

void MercuryRPC::Ref() { ++refs_; }

void MercuryRPC::Unref() {
  --refs_;
  assert(refs_ >= 0);
  if (refs_ <= 0) {
    delete this;
  }
}

MercuryRPC::MercuryRPC(bool listen, const RPCOptions& options)
    : env_(options.env),
      fs_(options.fs),
      refs_(0),
      pool_(options.extra_workers),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      bg_loop_running_(false),
      bg_error_(false),
      lookup_cv_(&mutex_) {
  na_class_ = Mercury_NA_Initialize(options.uri.c_str(), listen);
  na_context_ = Mercury_NA_Context_create(na_class_);
  hg_class_ = Mercury_HG_Init_na(na_class_, na_context_);
  hg_context_ = Mercury_HG_Context_create(hg_class_);

  Register_NONOP();
  Register_FSTAT();
  Register_MKDIR();
  Register_FCRET();
  Register_CHMOD();
  Register_CHOWN();
  Register_UNLNK();
  Register_RMDIR();
  Register_RENME();
  Register_LOKUP();
  Register_LSDIR();
  Register_RDIDX();
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
    NA_Addr_free(na_class_, it->second);
  }

  HG_Context_destroy(hg_context_);
  HG_Finalize(hg_class_);
  NA_Context_destroy(na_class_, na_context_);
  NA_Finalize(na_class_);
}

namespace {
struct LookupState {
  const std::string* addr;
  MercuryRPC* rpc;
  na_return_t ret;
  bool ok;
};
}  // namespace

na_return_t MercuryRPC::SaveAddr(const na_cb_info* info) {
  LookupState* state = reinterpret_cast<LookupState*>(info->arg);
  MercuryRPC* const rpc = state->rpc;
  state->ret = info->ret;

  MutexLock l(&rpc->mutex_);
  if (state->ret == NA_SUCCESS) {
    na_addr_t result = info->info.lookup.addr;
    if (rpc->addrs_.find(*state->addr) == rpc->addrs_.end()) {
      rpc->addrs_.insert(std::make_pair(*state->addr, result));
    } else {
      NA_Addr_free(rpc->na_class_, result);
    }
  }

  state->ok = true;
  rpc->lookup_cv_.SignalAll();
  return NA_SUCCESS;
}

na_return_t MercuryRPC::Lookup(const std::string& addr, na_addr_t* result) {
  MutexLock l(&mutex_);
  AddrTable::iterator it = addrs_.find(addr);
  if (it != addrs_.end()) {
    *result = it->second;
    return NA_SUCCESS;
  } else {
    LookupState state;
    state.rpc = this;
    state.addr = &addr;
    state.ok = false;
    NA_Addr_lookup(na_class_, na_context_, SaveAddr, &state, addr.c_str(),
                   NA_OP_ID_IGNORE);
    while (!state.ok) {
      lookup_cv_.Wait();
    }
    if (state.ret == NA_SUCCESS) {
      it = addrs_.find(addr);
      assert(it != addrs_.end());
      *result = it->second;
    }
    return state.ret;
  }
}

na_return_t MercuryRPC::LookupSelf(na_addr_t* result) {
  assert(NA_Is_listening(na_class_));
  MutexLock l(&mutex_);
  AddrTable::iterator it = addrs_.find("__SELF__");
  if (it != addrs_.end()) {
    *result = it->second;
    return NA_SUCCESS;
  } else {
    mutex_.Unlock();
    na_return_t ret = NA_Addr_self(na_class_, result);
    mutex_.Lock();
    if (ret == NA_SUCCESS) {
      it = addrs_.find("__SELF__");
      if (it == addrs_.end()) {
        addrs_.insert(std::make_pair("__SELF__", *result));
      } else {
        NA_Addr_free(na_class_, *result);
        *result = it->second;
      }
    }
    return ret;
  }
}

std::string MercuryRPC::ToString(na_addr_t addr) {
  char tmp[50];
  size_t len = sizeof(tmp);
  NA_Addr_to_string(na_class_, tmp, &len, addr);
  return tmp;
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
    LOG_ERROR("Error in local RPC bg_loop [errno=%d]", ret);
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
        LOG_ERROR("Error in local RPC bg_loop [errno=%d]", ret);
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
