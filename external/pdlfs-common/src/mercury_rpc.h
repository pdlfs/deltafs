#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#if defined(MERCURY)
#include <mercury.h>
#include <mercury_proc.h>
#include <map>
#include <string>

#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/rpc.h"

namespace pdlfs {
namespace rpc {

class MercuryRPC {
 public:
  void Ref();
  void Unref();
  std::string ToString(na_addr_t addr);
  na_return_t LookupSelf(na_addr_t* result);
  na_return_t Lookup(const std::string& addr, na_addr_t* result);
  MercuryRPC(bool listen, const RPCOptions&);

  na_class_t* na_class_;
  na_context_t* na_context_;
  hg_class_t* hg_class_;
  hg_context_t* hg_context_;

  class LocalLooper;
  class Client;

#define REG_ARGS(OP) #OP, If_Message_cb, If_Message_cb, If_##OP##_decorator
  static hg_return_t If_Message_cb(hg_proc_t proc, void* data);

#define REG_RPC(OP)                                            \
  static hg_return_t If_##OP##_decorator(hg_handle_t handle);  \
  static hg_return_t If_##OP##_cb(hg_handle_t handle);         \
  static void If_##OP##_wrapper(void* arg) {                   \
    hg_handle_t handle = reinterpret_cast<hg_handle_t>(arg);   \
    If_##OP##_cb(handle);                                      \
  }                                                            \
                                                               \
  hg_id_t hg_##OP##_id_;                                       \
                                                               \
  void Register_##OP() {                                       \
    hg_##OP##_id_ = HG_Register_name(hg_class_, REG_ARGS(OP)); \
    if (NA_Is_listening(na_class_)) {                          \
      HG_Register_data(hg_class_, hg_##OP##_id_, this, NULL);  \
    }                                                          \
  }

  REG_RPC(NONOP)
  REG_RPC(FSTAT)
  REG_RPC(MKDIR)
  REG_RPC(FCRET)
  REG_RPC(CHMOD)
  REG_RPC(CHOWN)
  REG_RPC(UTIME)
  REG_RPC(TRUNC)
  REG_RPC(SATTR)
  REG_RPC(UNLNK)
  REG_RPC(RMDIR)
  REG_RPC(RENME)
  REG_RPC(LOKUP)
  REG_RPC(LSDIR)
  REG_RPC(RDIDX)
  REG_RPC(OPSES)
  REG_RPC(GINPT)
  REG_RPC(GOUPT)

#undef REG_RPC
#undef REG_ARGS

  // Start or stop the background looping thread.
  Status TEST_Start();
  Status TEST_Stop();

 private:
  ~MercuryRPC();

  static inline MercuryRPC* registered_data(hg_handle_t handle) {
    hg_info* info = HG_Get_info(handle);
    void* data = HG_Registered_data(info->hg_class, info->id);
    MercuryRPC* rpc = reinterpret_cast<MercuryRPC*>(data);
    assert(rpc != NULL);
    return rpc;
  }

  Env* env_;
  If* fs_;
  int refs_;
  ThreadPool* pool_;

  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;
  bool bg_loop_running_;
  bool bg_error_;

  port::CondVar lookup_cv_;
  typedef std::map<std::string, na_addr_t> AddrTable;
  AddrTable addrs_;
  static na_return_t SaveAddr(const na_cb_info* info);
  static void TEST_LoopForever(void* arg);

  // No copying allowed
  void operator=(const MercuryRPC&);
  MercuryRPC(const MercuryRPC&);
};

// ====================
// Mercury looper
// ====================

class MercuryRPC::LocalLooper {
 private:
  MercuryRPC* const rpc_;
  port::AtomicPointer shutting_down_;
  port::Mutex mutex_;
  port::CondVar bg_cv_;
  int max_bg_loops_;
  int bg_loops_;

  void BGLoop();
  static void BGLoopWrapper(void* arg) {
    LocalLooper* looper = reinterpret_cast<LocalLooper*>(arg);
    looper->BGLoop();
  }

 public:
  LocalLooper(MercuryRPC* rpc, const RPCOptions& options)
      : rpc_(rpc),
        shutting_down_(NULL),
        bg_cv_(&mutex_),
        max_bg_loops_(options.num_io_threads),
        bg_loops_(0) {
    rpc_->Ref();
  }

  ~LocalLooper() {
    Stop();
    if (rpc_ != NULL) {
      rpc_->Unref();
    }
  }

  Status Start() {
    mutex_.Lock();
    while (bg_loops_ < max_bg_loops_) {
      assert(bg_loops_ >= 0);
      bg_loops_++;
      rpc_->env_->StartThread(BGLoopWrapper, this);
    }
    mutex_.Unlock();
    return Status::OK();
  }

  Status Stop() {
    mutex_.Lock();
    shutting_down_.Release_Store(this);
    while (bg_loops_ != 0) {
      bg_cv_.Wait();
    }
    mutex_.Unlock();
    return Status::OK();
  }
};

// ====================
// Mercury client
// ====================

class MercuryRPC::Client : public If {
 public:
  Client(MercuryRPC* rpc, const std::string& addr)
      : rpc_(rpc), addr_(addr), cv_(&mu_) {
    rpc_->Ref();
  }

  virtual ~Client() {
    if (rpc_ != NULL) {
      rpc_->Unref();
    }
  }

#define DEC_RPC(OP) virtual void OP(Message& in, Message& out);

  DEC_RPC(NONOP)
  DEC_RPC(FSTAT)
  DEC_RPC(MKDIR)
  DEC_RPC(FCRET)
  DEC_RPC(CHMOD)
  DEC_RPC(CHOWN)
  DEC_RPC(UTIME)
  DEC_RPC(TRUNC)
  DEC_RPC(SATTR)
  DEC_RPC(UNLNK)
  DEC_RPC(RMDIR)
  DEC_RPC(RENME)
  DEC_RPC(LOKUP)
  DEC_RPC(LSDIR)
  DEC_RPC(RDIDX)
  DEC_RPC(OPSES)
  DEC_RPC(GINPT)
  DEC_RPC(GOUPT)

#undef DEC_RPC

 private:
  static hg_return_t SaveReply(const hg_cb_info* info);
  MercuryRPC* const rpc_;
  std::string addr_;  // To-be-resolved target RPC addr
  port::Mutex mu_;
  port::CondVar cv_;
  // No copying allowed
  void operator=(const Client&);
  Client(const Client&);
};

}  // namespace rpc
}  // namespace pdlfs

#endif
