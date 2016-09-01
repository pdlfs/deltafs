/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/pdlfs_config.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

#if defined(MERCURY)
#include "mercury_rpc.h"
namespace pdlfs {
namespace rpc {

static const std::string kProto = "bmi+tcp://";
// True if multiple client threads can call RPC simultaneously
static const bool kAllowConcurrentRPC = false;

class MercuryServer : public If {
 public:
  MercuryServer() {
    env_ = Env::Default();
    pool_ = ThreadPool::NewFixed(2);
    RPCOptions options;
    options.env = env_;
    options.extra_workers = pool_;
    options.num_io_threads = 2;
    options.uri = kProto + "10101";
    options.fs = this;
    bool listen = true;
    rpc_ = new MercuryRPC(listen, options);
    looper_ = new MercuryRPC::LocalLooper(rpc_, options);
    self_ = new MercuryRPC::Client(rpc_, kProto + "127.0.0.1:10101");
    looper_->Start();
    rpc_->Ref();
  }

  virtual ~MercuryServer() {
    rpc_->Unref();
    looper_->Stop();
    delete self_;
    delete looper_;
    delete pool_;
  }

  virtual void Call(Message& in, Message& out) {
    out.op = in.op;
    out.err = in.err;
    out.contents = in.contents;
  }

  ThreadPool* pool_;
  MercuryRPC::LocalLooper* looper_;
  MercuryRPC::Client* self_;
  MercuryRPC* rpc_;
  Env* env_;
};

class MercuryTest {
 public:
  MercuryServer* server_;
  port::Mutex mu_;
  port::CondVar cv_;
  int num_tasks_;

  explicit MercuryTest() : cv_(&mu_), num_tasks_(0) {
    server_ = new MercuryServer();
  }

  ~MercuryTest() { delete server_; }

  void BGTask() {
    Random rnd(301);
    for (int i = 0; i < 1000; ++i) {
      std::string buf;
      If::Message input;
      input.contents = test::RandomString(&rnd, 1400, &buf);
      input.op = rnd.Uniform(128);
      input.err = rnd.Uniform(128);
      If::Message output;
      if (!kAllowConcurrentRPC) {
        mu_.Lock();
      }
      server_->self_->Call(input, output);
      if (!kAllowConcurrentRPC) {
        mu_.Unlock();
      }
      ASSERT_EQ(input.contents, output.contents);
      ASSERT_EQ(input.op, output.op);
      ASSERT_EQ(input.err, output.err);
    }
    mu_.Lock();
    assert(num_tasks_ > 0);
    num_tasks_--;
    cv_.SignalAll();
    mu_.Unlock();
  }

  static void BGTaskWrapper(void* arg) {
    MercuryTest* test = reinterpret_cast<MercuryTest*>(arg);
    test->BGTask();
  }

  void RunTasks(int num_tasks) {
    assert(num_tasks_ == 0);
    fprintf(stderr, "%d client threads\n", num_tasks);
    num_tasks_ = num_tasks;
    for (int i = 0; i < num_tasks_; ++i) {
      server_->env_->StartThread(BGTaskWrapper, this);
    }
    mu_.Lock();
    while (num_tasks_ != 0) {
      cv_.Wait();
    }
    mu_.Unlock();
  }
};

TEST(MercuryTest, SendReceive) {
  RunTasks(1);
  RunTasks(4);
  RunTasks(8);
}

}  // namespace rpc
}  // namespace pdlfs

#endif

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
