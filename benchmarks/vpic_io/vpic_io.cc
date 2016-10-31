/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <getopt.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <vector>

#include "pdlfs-common/coding.h"
#include "pdlfs-common/hash.h"
#include "pdlfs-common/random.h"

// Abstract FS interface
#include "../io_client.h"

namespace pdlfs {

// REQUIRES: callers are required to initialize all fields
struct VPICbenchOptions : public ioclient::IOClientOptions {
  // Random seed.
  int seed;
  // True if clients are running in relaxed consistency mode (deltafs only)
  bool relaxed_consistency;
  // Continue running even on errors.
  // Some errors are not ignored.
  bool ignore_errors;
  // 3d grid (number of cells)
  int x, y, z;
  // Number of particles per cell
  int ppc;
};

// REQUIRES: callers are required to initialize all fields
struct VPICbenchReport {
  double duration;
  // Total number of particles dumped
  int ops;
  // Total number of errors
  int errors;
};

// A simple benchmark that simulates the write pattern of a VPIC app
// with a one-file-per-particle input deck.
class VPICbench {
 public:
  VPICbench(const VPICbenchOptions& options) : options_(options), rnd_(301) {
    io_ = ioclient::IOClient::Factory(options_);
    dump_seq_ = 0;
  }

  ~VPICbench() {
    if (io_ != NULL) {
      io_->Dispose();
      delete io_;
    }
  }

  Status Mkroot() {
    Status s = io_->MakeDirectory("/particles");
    if (options_.ignore_errors) {
      return Status::OK();
    } else {
      return s;
    }
  }

  Status Mkdir(int step_id) {
    Status s;
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/particles/T.%d", step_id);
    s = io_->MakeDirectory(tmp);
    if (options_.ignore_errors) {
      return Status::OK();
    } else {
      return s;
    }
  }

  Status WriteParticle(int step_id, long long particle_id) {
    Status s;
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/particles/T.%d/p_%lld", step_id, particle_id);
    char data[32];  // four 64-bit integers
    {
      char* p = data;
      for (int i = 0; i < sizeof(data) / 8; i++) {
        uint64_t r = rnd_.Next64();
        EncodeFixed64(p, r);
        p += 8;
      }
    }
    s = io_->Append(tmp, data, sizeof(data));
    if (options_.ignore_errors) {
      return Status::OK();
    } else {
      return s;
    }
  }

  uint64_t NumberParticles() {
    uint64_t result = options_.ppc;
    result *= options_.x;
    result *= options_.y;
    result *= options_.z;
    return result;
  }

  static uint32_t Rank(uint32_t seed, uint64_t particle) {
    char tmp[8];
    EncodeFixed64(tmp, particle);
    return Hash(tmp, sizeof(tmp), seed);
  }

  // Initialize io client.  Create a directory for particle dumps.
  // If in relaxed consistency mode, every client will create a directory.
  // Return a status report with local timing and error counts.
  VPICbenchReport Prepare() {
    double start = MPI_Wtime();
    VPICbenchReport report;
    report.errors = 0;
    report.ops = 0;
    Status s = io_->Init();
    if (s.ok() && (options_.rank == 0 || options_.relaxed_consistency)) {
      s = Mkroot();
      if (!s.ok()) {
        report.errors++;
      } else {
        report.ops++;
      }
    }

    report.duration = MPI_Wtime() - start;
    return report;
  }

  // Create a dump directory.  Each client writes a random disjoint subset
  // of particles.  If in relaxed consistency mode, every client will
  // create a dump directory.  Return a status report with
  // local timing and error counts.
  VPICbenchReport Dump() {
    double start = MPI_Wtime();
    VPICbenchReport report;
    report.errors = 0;
    report.ops = 0;
    Status s;
    if (options_.rank == 0 || options_.relaxed_consistency) {
      s = Mkdir(dump_seq_);
      if (!s.ok()) {
        report.errors++;
      } else {
        report.ops++;
      }
    }
    if (s.ok()) {
      const uint64_t num_particles = NumberParticles();
      for (uint64_t i = 0; i < num_particles; i++) {
        int r = Rank(dump_seq_, i) % options_.comm_sz;
        if (r == options_.rank) {
          s = WriteParticle(dump_seq_, i);
          if (!s.ok()) {
            report.errors++;
            break;
          } else {
            report.ops++;
          }
        }
      }
    }

    dump_seq_++;
    report.duration = MPI_Wtime() - start;
    return report;
  }

 private:
  int dump_seq_;
  const VPICbenchOptions options_;
  ioclient::IOClient* io_;
  Random rnd_;
};

}  // namespace pdlfs

static pdlfs::VPICbenchReport Merge(const pdlfs::VPICbenchReport& report) {
  pdlfs::VPICbenchReport result;

  MPI_Reduce(&report.duration, &result.duration, 1, MPI_DOUBLE, MPI_MAX, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&report.errors, &result.errors, 1, MPI_INT, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&report.ops, &result.ops, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

  return result;
}

static void Print(const pdlfs::VPICbenchReport& report) {
  printf("-- Performed %d ops in %.3f seconds: %d succ, %d fail\n",
         report.ops + report.errors, report.duration, report.ops,
         report.errors);
}

static void Print(const char* msg) {
  // Usually only called by rank 0
  printf("== %s\n", msg);
}

static pdlfs::VPICbenchOptions ParseOptions(int argc, char** argv) {
  pdlfs::VPICbenchOptions result;
  result.rank = 0;
  result.comm_sz = 1;
  result.relaxed_consistency = false;
  result.ignore_errors = false;
  result.x = result.y = result.z = 2;
  result.ppc = 2;
  result.argv = NULL;
  result.argc = 0;

  optind = 1;

  // Rewrite argc and argv
  result.argc = argc - optind + 1;
  assert(result.argc > 0);

  int i = 0;
  static std::vector<char*> buffer;
  buffer.resize(result.argc, NULL);
  result.argv = &buffer[0];
  result.argv[i] = argv[i];
  for (int j = optind; j < argc;) result.argv[++i] = argv[j++];
  return result;
}

int main(int argc, char** argv) {
  int rank;
  int size;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  pdlfs::VPICbenchOptions options = ParseOptions(argc, argv);
  options.rank = rank;
  options.comm_sz = size;
  pdlfs::VPICbench bench(options);
  pdlfs::VPICbenchReport report;
  report.errors = 0;

  if (report.errors == 0) {
    if (rank == 0) {
      Print("Prepare ... ");
    }
    MPI_Barrier(MPI_COMM_WORLD);
    report = Merge(bench.Prepare());
    if (rank == 0) {
      Print(report);
    }
  } else {
    if (rank == 0) {
      Print("Abort");
      goto end;
    }
  }

  if (report.errors == 0) {
    if (rank == 0) {
      Print("Dump ... ");
    }
    MPI_Barrier(MPI_COMM_WORLD);
    report = Merge(bench.Dump());
    if (rank == 0) {
      Print(report);
    }
  } else {
    if (rank == 0) {
      Print("Abort");
      goto end;
    }
  }

end:
  MPI_Finalize();
  return 0;
}
