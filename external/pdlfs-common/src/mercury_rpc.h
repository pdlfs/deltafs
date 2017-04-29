#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <mercury.h>
#include <mercury_proc.h>
#include <map>
#include <string>

#include "pdlfs-common/lru.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/rpc.h"

namespace pdlfs {
namespace rpc {

class MercuryRPC {
 public:
  struct Addr;
  std::string ToString(hg_addr_t addr);
  typedef LRUEntry<Addr> AddrEntry;
  void Release(AddrEntry* entry);
  AddrEntry* LookupCache(const std::string& addr);
  AddrEntry* Bind(const std::string& addr, Addr*);
  hg_return_t Lookup(const std::string& addr, AddrEntry** result);
  MercuryRPC(bool listen, const RPCOptions& options);
  bool ok() const { return !has_error_.Acquire_Load(); }
  Status status() const {
    if (has_error_.Acquire_Load()) {
      return Status::IOError(Slice());
    } else {
      return Status::OK();
    }
  }
  void SetError();
  void Unref();
  void Ref();

  hg_class_t* hg_class_;
  hg_context_t* hg_context_;
  bool listen_;

  class LocalLooper;
  class Client;

  static hg_return_t RPCMessageCoder(hg_proc_t proc, void* data);
  static hg_return_t RPCCallbackDecorator(hg_handle_t handle);
  static hg_return_t RPCCallback(hg_handle_t handle);
  static void RPCWrapper(void* arg) {
    hg_handle_t handle = reinterpret_cast<hg_handle_t>(arg);
    RPCCallback(handle);
  }

  hg_id_t hg_rpc_id_;

  void RegisterRPC() {
    hg_rpc_id_ = HG_Register_name(hg_class_, "deltafs_rpc", RPCMessageCoder,
                                  RPCMessageCoder, RPCCallbackDecorator);
    if (listen_) {
      HG_Register_data(hg_class_, hg_rpc_id_, this, NULL);
    }
  }

 private:
  ~MercuryRPC();
  // No copying allowed
  void operator=(const MercuryRPC&);
  MercuryRPC(const MercuryRPC&);

  static inline MercuryRPC* registered_data(hg_handle_t handle) {
    const hg_info* info = HG_Get_info(handle);
    void* data = HG_Registered_data(info->hg_class, info->id);
    MercuryRPC* rpc = reinterpret_cast<MercuryRPC*>(data);
    assert(rpc != NULL);
    return rpc;
  }

  struct Timer {
    Timer* prev;
    Timer* next;
    hg_handle_t handle;
    uint64_t due;
  };

  friend class Client;
  friend class LocalLooper;
  void AddTimerFor(hg_handle_t handle, Timer*);
  void RemoveTimer(Timer* timer);
  void CheckTimers();

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer has_error_;
  port::CondVar lookup_cv_;
  LRUCache<AddrEntry> addr_cache_;
  void Remove(Timer*);
  void Append(Timer*);
  Timer timers_;
  int refs_;

  // Constant after construction
  uint64_t rpc_timeout_;
  ThreadPool* pool_;
  Env* env_;
  If* fs_;
};

// ====================
// Mercury addr
// ====================

struct MercuryRPC::Addr {
  hg_class_t* clazz;
  hg_addr_t rep;

  Addr(hg_class_t* hgz, hg_addr_t hga) {
    clazz = hgz;
    rep = hga;
  }
};

// ====================
// Mercury looper
// ====================

class MercuryRPC::LocalLooper {
 private:
  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;
  int bg_loops_;
  int bg_id_;

  // Constant after construction
  bool ignore_rpc_error_;  // Keep looping even if we receive errors
  int max_bg_loops_;
  MercuryRPC* rpc_;

  void BGLoop();
  static void BGLoopWrapper(void* arg) {
    LocalLooper* looper = reinterpret_cast<LocalLooper*>(arg);
    looper->BGLoop();
  }

 public:
  LocalLooper(MercuryRPC* rpc, const RPCOptions& options)
      : shutting_down_(NULL),
        bg_cv_(&mutex_),
        bg_loops_(0),
        bg_id_(0),
        ignore_rpc_error_(false),
        max_bg_loops_(options.num_io_threads),
        rpc_(rpc) {
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
  explicit Client(MercuryRPC* rpc, const std::string& addr)
      : rpc_(rpc), addr_(addr), cv_(&mu_) {
    rpc_->Ref();
  }

  // Return OK on success, a non-OK status on RPC errors.
  virtual Status Call(Message& in, Message& out) RPCNOEXCEPT;

  virtual ~Client() {
    if (rpc_ != NULL) {
      rpc_->Unref();
    }
  }

 private:
  MercuryRPC* rpc_;
  std::string addr_;  // Unresolved target address

  port::Mutex mu_;
  port::CondVar cv_;
  // No copying allowed
  void operator=(const Client&);
  Client(const Client&);
};

}  // namespace rpc
}  // namespace pdlfs
