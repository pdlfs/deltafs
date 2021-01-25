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
#include "pdlfs-common/port.h"
#include "pdlfs-common/testharness.h"

#include <stdio.h>
#include <stdlib.h>

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
  SleepForMicroseconds(1 * 1000 * 1000);
  rpc::If* client = rpc->OpenStubFor("127.0.0.1:22222");
  ASSERT_TRUE(client != NULL);
  rpc::If::Message in, out;
  in.contents = Slice("xxyyzz");
  Status status = client->Call(in, out);
  ASSERT_OK(status);
  ASSERT_TRUE(out.contents == in.contents);
  ASSERT_OK(rpc->Stop());
  delete client;
  delete rpc;
}

namespace {
int GetOptionFromEnv(const char* key, int def) {
  const char* env = getenv(key);
  if (env && env[0]) {
    return atoi(env);
  } else {
    return def;
  }
}

int GetOption(const char* key, int def) {
  int opt = GetOptionFromEnv(key, def);
  fprintf(stderr, "%s=%d\n", key, opt);
  return opt;
}
}  // namespace

class RPCBench {
 public:
  RPCBench(rpc::Mode mode, const char* uri) : rpc_(NULL) {
    options_.mode = mode;
    options_.uri = uri;
  }

  ~RPCBench() {
    if (rpc_) {
      delete rpc_;
    }
  }

  RPCOptions options_;
  RPC* rpc_;
};

class RPCBenchServer : public RPCBench, public rpc::If {
 public:
  explicit RPCBenchServer(const char* uri)
      : RPCBench(rpc::kServerClient, uri), shutting_down_(NULL) {
    int n = GetOption("RPC_NUM_THREADS", 1);
    options_.num_rpc_threads = n;
    options_.fs = this;
    rpc_ = RPC::Open(options_);
  }

  virtual Status Call(Message& in, Message& out) RPCNOEXCEPT {
    out.extra_buf.assign(in.contents.data(), in.contents.size());
    out.contents = out.extra_buf;
    if (out.extra_buf[0] == 'b') {  // Client says goodbye
      shutting_down_.Release_Store(this);
    }
    return Status::OK();
  }

  void Run() {
    RPC* const r = rpc_;
    Status status = r->Start();
    if (!status.ok()) {
      fprintf(stderr, "Error starting server: %s\n", status.ToString().c_str());
      return;
    }
    while (!shutting_down_.Acquire_Load() && status.ok()) {
      SleepForMicroseconds(1000 * 1000);
      status = r->status();
    }
    if (status.ok()) {
      status = r->Stop();
    }
    if (!status.ok()) {
      fprintf(stderr, "Server stopped with error: %s\n",
              status.ToString().c_str());
    }
  }

  port::AtomicPointer shutting_down_;
};

class RPCBenchClient : public RPCBench {
 public:
  explicit RPCBenchClient(const char* uri) : RPCBench(rpc::kClientOnly, uri) {
    rpc_ = RPC::Open(options_);
  }

  void Run() {
    rpc::If* client = rpc_->OpenStubFor(options_.uri);
    int nrpcs = GetOption("RPC_NUM_SENDRECV", 1000 * 1000);
    rpc::If::Message in, out;
    Status status;
    for (int i = 0; i < nrpcs; ++i) {
      in.contents = Slice("xxx");
      status = client->Call(in, out);
      if (!status.ok()) {
        break;
      }
    }
    if (status.ok()) {
      in.contents = Slice("bye");
      status = client->Call(in, out);
    }
    if (!status.ok()) {
      fprintf(stderr, "Client stopped with error: %s\n",
              status.ToString().c_str());
    }
    delete client;
  }
};

}  // namespace pdlfs

static void BM_Usage() {
  fprintf(stderr, "Use --bench=[cli,srv] uri to run benchmarks.");
  fprintf(stderr, "\n");
  exit(EXIT_FAILURE);
}

static void BM_Main(int* argc, char*** argv) {
  pdlfs::Slice bench_name;
  if (*argc > 2) {
    bench_name = pdlfs::Slice((*argv)[*argc - 2]);
  } else {
    BM_Usage();
  }
  if (bench_name.starts_with("--bench=cli")) {
    pdlfs::RPCBenchClient c((*argv)[*argc - 1]);
    c.Run();
  } else if (bench_name.starts_with("--bench=srv")) {
    pdlfs::RPCBenchServer s((*argv)[*argc - 1]);
    s.Run();
  } else {
    BM_Usage();
  }
}

int main(int argc, char** argv) {
  pdlfs::Slice token1, token2;
  if (argc > 2) {
    token2 = pdlfs::Slice(argv[argc - 2]);
  }
  if (argc > 1) {
    token1 = pdlfs::Slice(argv[argc - 1]);
  }
  if (!token1.starts_with("--bench") && !token2.starts_with("--bench")) {
    return pdlfs::test::RunAllTests(&argc, &argv);
  } else {
    BM_Main(&argc, &argv);
    return 0;
  }
}
