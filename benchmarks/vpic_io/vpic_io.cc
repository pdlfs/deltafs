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
  // Number of dumps
  int num_dumps;
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
  VPICbench(const VPICbenchOptions& options)
      : dump_seq_(0), options_(options), rnd_(options.seed) {
    io_ = ioclient::IOClient::Factory(options_);
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

  Status MkStep(int step_id) {
    Status s;
    // TODO
    if (options_.ignore_errors) {
      return Status::OK();
    } else {
      return s;
    }
  }

  Status WriteParticle(int step_id, long long particle_id) {
    Status s;
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/particles/p_%lld", particle_id);
    char data[32];  // possibly eight 32-bit float numbers
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
    if (s.ok()) {
      report.ops++;
      if (options_.rank == 0 || options_.relaxed_consistency) {
        s = Mkroot();
        if (!s.ok()) {
          report.errors++;
        } else {
          report.ops++;
        }
      }
    } else {
      report.errors++;
    }

    report.duration = MPI_Wtime() - start;
    return report;
  }

  // In each dump, every client writes a random disjoint subset of particles.
  // Return a status report with local timing and error counts.
  VPICbenchReport Dump() {
    double start = MPI_Wtime();
    VPICbenchReport report;
    report.errors = 0;
    report.ops = 0;
    Status s = MkStep(dump_seq_);
    if (!s.ok()) {
      report.errors++;
    } else {
      report.ops++;
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

static void Help(const char* prog, FILE* out) {
  fprintf(out,
          "%s [options] <io_type> <io_conf>\n\n"
          "IO types: deltafs, posix\n\n"
          "Deltafs IO confs:\n\n"
          "  \"DELTAFS_Verbose?10|DELTAFS_LogToStderr?true\"\n\n"
          "Posix IO confs:\n\n"
          "  \"mount_point=/tmp/ioclient\"\n\n"
          "Options:\n\n"
          "  --relaxed-consistency  :  "
          "Executes in relaxed consistency mode (default: false)\n"
          "  --ignored-erros        :  "
          "Continue running even on errors (default: false)\n"
          "  --seed=n               :  "
          "Set the random seed (default: 301)\n"
          "  --num-dumps=n          :  "
          "Total number of particle dumps (default: 1)\n"
          "  --ppc=n                :  "
          "Number of particles per grid cell (default: 2)\n"
          "  --x/y/z=n              :  "
          "3d grid dimensions (default: 2)\n\n"
          "Deltafs VPIC IO benchmark\n",
          prog);
}

static pdlfs::VPICbenchOptions ParseOptions(int argc, char** argv) {
  pdlfs::VPICbenchOptions result;
  result.rank = 0;
  result.comm_sz = 1;
  result.num_dumps = 1;
  result.seed = 301;
  result.relaxed_consistency = false;
  result.ignore_errors = false;
  result.x = result.y = result.z = 2;
  result.ppc = 2;
  result.argv = NULL;
  result.argc = 0;

  {
    std::vector<struct option> optinfo;
    optinfo.push_back({"relaxed-consistency", 0, NULL, 1});
    optinfo.push_back({"ignore-errors", 0, NULL, 2});
    optinfo.push_back({"num-dumps", 1, NULL, 3});
    optinfo.push_back({"seed", 1, NULL, 4});
    optinfo.push_back({"ppc", 1, NULL, 'p'});
    optinfo.push_back({"x", 1, NULL, 'x'});
    optinfo.push_back({"y", 1, NULL, 'y'});
    optinfo.push_back({"z", 1, NULL, 'z'});
    optinfo.push_back({"help", 0, NULL, 'H'});
    optinfo.push_back({NULL, 0, NULL, 0});

    while (true) {
      int ignored_index;
      int c = getopt_long_only(argc, argv, "h", &optinfo[0], &ignored_index);
      if (c != -1) {
        switch (c) {
          case 1:
            result.relaxed_consistency = true;
            break;
          case 2:
            result.ignore_errors = true;
            break;
          case 3:
            result.num_dumps = atoi(optarg);
            break;
          case 4:
            result.seed = atoi(optarg);
            break;
          case 'p':
            result.ppc = atoi(optarg);
            break;
          case 'x':
            result.x = atoi(optarg);
            break;
          case 'y':
            result.y = atoi(optarg);
            break;
          case 'z':
            result.z = atoi(optarg);
            break;
          case 'H':
          case 'h':
            Help(argv[0], stdout);
            exit(EXIT_SUCCESS);
          default:
            Help(argv[0], stderr);
            exit(EXIT_FAILURE);
        }
      } else {
        break;  // No more options
      }
    }
  }

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
  int num_dumps = options.num_dumps;
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

  while (num_dumps-- != 0) {
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
  }

end:
  MPI_Finalize();
  return 0;
}
