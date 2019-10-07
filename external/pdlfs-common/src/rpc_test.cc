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

#include "pdlfs-common/rpc.h"
#include "pdlfs-common/testharness.h"

namespace pdlfs {

class RPCTest : public rpc::If {
 public:
  RPCTest() {
    options_.uri = "ignored://127.0.0.1:22222";
    options_.fs = this;
  }

  virtual Status Call(Message& in, Message& out) RPCNOEXCEPT {
    out.extra_buf.assign(in.contents.data(), in.contents.size());
    out.contents = out.extra_buf;
    return Status::OK();
  }

  RPCOptions options_;
};

TEST(RPCTest, Open) {
  RPC* rpc = RPC::Open(options_);
  ASSERT_TRUE(rpc != NULL);
  ASSERT_OK(rpc->Start());
  delete rpc;
}

TEST(RPCTest, SendAndRecv) {
  RPC* rpc = RPC::Open(options_);
  ASSERT_TRUE(rpc != NULL);
  ASSERT_OK(rpc->Start());
  Env::Default()->SleepForMicroseconds(1 * 1000 * 1000);
  rpc::If* client = rpc->OpenStubFor("127.0.0.1:22222");
  ASSERT_TRUE(client != NULL);
  rpc::If::Message in, out;
  in.contents = Slice("xxyyzz");
  Status status = client->Call(in, out);
  ASSERT_OK(status);
  ASSERT_TRUE(out.contents == in.contents);
  ASSERT_OK(rpc->Stop());
  delete rpc;
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
