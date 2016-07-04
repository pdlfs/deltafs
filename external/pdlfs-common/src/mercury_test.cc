/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "mercury_rpc.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

#if defined(MERCURY)
namespace pdlfs {
namespace rpc {

class IfImpl : public IfWrapper {
 public:
  IfImpl() : IfWrapper() {}
  virtual ~IfImpl() {}

  virtual void NONOP(Message& in, Message& out) {
    out.err = in.err;
    out.contents = in.contents;
  }
};

class MercuryTest {
 public:
  std::string buf_;
  RPCOptions options_;
  IfImpl fs_;
  MercuryRPC* rpc_;
  ThreadPool* pool_;
  MercuryRPC::LocalLooper* looper_;
  MercuryRPC::Client* client_;
  Env* env_;

  MercuryTest() {
    pool_ = ThreadPool::NewFixed(2);
    env_ = Env::Default();
    options_.num_io_threads_ = 1;
    options_.env = env_;
    options_.fs = &fs_;
    options_.uri = "bmi+tcp://localhost:10101";
    options_.extra_workers = pool_;
    bool listen = true;
    rpc_ = new MercuryRPC(listen, options_);
    looper_ = new MercuryRPC::LocalLooper(rpc_, options_);
    client_ = new MercuryRPC::Client(rpc_, "tcp://localhost:10101");
    looper_->Start();
    rpc_->Ref();
  }

  ~MercuryTest() {
    delete client_;
    looper_->Stop();
    delete looper_;
    rpc_->Unref();
    delete pool_;
  }
};

TEST(MercuryTest, SendReceive) {
  Random rnd(301);
  for (int i = 0; i < 1000; ++i) {
    If::Message input;
    If::Message output;
    input.err = rnd.Uniform(128);
    input.contents = test::RandomString(&rnd, 4000, &buf_);
    client_->NONOP(input, output);
    ASSERT_EQ(input.err, output.err);
    ASSERT_EQ(input.contents, output.contents);
  }
}

}  // namespace rpc
}  // namespace pdlfs

#endif

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
