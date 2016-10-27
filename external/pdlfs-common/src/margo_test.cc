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

#include "margo_rpc.h"
namespace pdlfs {
namespace rpc {

class MargoServer : public If {
 public:
  MargoServer() {
    env_ = Env::Default();
    RPCOptions options;
    options.env = env_;
    options.num_io_threads = 4;
    options.uri = "bmi+tcp://10101";
    options.fs = this;
    bool listen = true;
    rpc_ = new MargoRPC(listen, options);
    self_ = new MargoRPC::Client(rpc_, "bmi+tcp://localhost:10101");
    rpc_->Start();
    rpc_->Ref();
  }

  virtual ~MargoServer() {
    rpc_->Unref();
    delete self_;
  }

  virtual void Call(Message& in, Message& out) {
    out.op = in.op;
    out.err = in.err;
    out.contents = in.contents;
  }

  MargoRPC::Client* self_;
  MargoRPC* rpc_;
  Env* env_;
};

class MargoTest {
 public:
  MargoServer* server_;
  port::Mutex mu_;
  port::CondVar cv_;
  int num_tasks_;

  explicit MargoTest() : cv_(&mu_), num_tasks_(0) {
    server_ = new MargoServer();
  }

  ~MargoTest() {
    assert(num_tasks_ == 0);
    delete server_;
  }

  void BGTask() {
    Random rnd(301);
    for (int i = 0; i < 1000; ++i) {
      std::string buf;
      If::Message input;
      input.contents = test::RandomString(&rnd, 4000, &buf);
      input.op = rnd.Uniform(128);
      input.err = rnd.Uniform(128);
      If::Message output;
      server_->self_->Call(input, output);
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
    MargoTest* test = reinterpret_cast<MargoTest*>(arg);
    test->BGTask();
  }

  void RunTasks(int num_tasks) {
    assert(num_tasks_ == 0);
    fprintf(stderr, "%d client threads\n", num_tasks);
    num_tasks_ = num_tasks;
    for (int i = 0; i < num_tasks_; i++) {
      server_->env_->StartThread(BGTaskWrapper, this);
    }
    mu_.Lock();
    while (num_tasks_ != 0) {
      cv_.Wait();
    }
    mu_.Unlock();
  }
};

TEST(MargoTest, SendReceive) {
  RunTasks(1);
  RunTasks(4);
  RunTasks(8);
}

}  // namespace rpc
}  // namespace pdlfs

int main(int argc, char** argv) {
  ABT_init(1, argv);
  int r = ::pdlfs::test::RunAllTests(&argc, &argv);
  ABT_finalize();
  return r;
}
