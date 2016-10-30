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

// Abstract FS interface
#include "../io_client.h"

namespace pdlfs {

// REQUIRES: callers must explicitly initialize all fields
struct LDbenchOptions : public ioclient::IOClientOptions {
  // Total number of dirs to create (among all clients)
  int num_dirs;
  // Total number of empty files to create (among all clients)
  int num_files;
  // True if clients are running in relaxed consistency mode (deltafs only)
  bool relaxed_consistency;
  // Continue running even if we get errors.
  // Some errors are not ignored.
  bool ignore_errors;
  // True to skip the insertion phase.
  bool skip_inserts;
  // True to skip the reading phase.
  bool skip_reads;
};

// REQUIRES: callers must explicitly initialize all fields
struct LDbenchReport {
  double duration;
  // Total number of operations successfully executed
  int ops;
  // Total number of errors
  int errs;
};

// A simple benchmark that bulk inserts a large number of empty files
// into one or more directories.
class LDbench {  // LD stands for large directory
 public:
  LDbench(const LDbenchOptions& options) : options_(options) {
    io_ = ioclient::IOClient::Factory(options_);
  }

  ~LDbench() {
    if (io_ != NULL) {
      io_->Dispose();
      delete io_;
    }
  }

  Status Mkdir(int dir_no) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/d_%d", dir_no);
    Status s = io_->MakeDirectory(tmp);
    if (options_.ignore_errors) {
      return Status::OK();
    } else {
      return s;
    }
  }

  Status Mknod(int dir_no, int f) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/d_%d/f_%d", dir_no, f);
    Status s = io_->NewFile(tmp);
    if (options_.ignore_errors) {
      return Status::OK();
    } else {
      return s;
    }
  }

  Status Fstat(int dir_no, int f) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/d_%d/f_%d", dir_no, f);
    Status s = io_->GetAttr(tmp);
    if (options_.ignore_errors) {
      return Status::OK();
    } else {
      return s;
    }
  }

  static uint32_t Dir(uint32_t file_no) {
    char tmp[4];
    EncodeFixed32(tmp, file_no);
    return Hash(tmp, sizeof(tmp), 0);
  }

  // Initialize io client.  Collectively create parent directories.
  // If in relaxed consistency mode, every client will create all directories.
  // Return a status report with local timing and error counts.
  LDbenchReport Prepare() {
    double start = MPI_Wtime();
    LDbenchReport report;
    report.errs = 0;
    report.ops = 0;
    Status s = io_->Init();
    if (s.ok() && !options_.skip_inserts) {
      int dir_no = options_.relaxed_consistency ? 0 : options_.rank;
      while (dir_no < options_.num_dirs) {
        s = Mkdir(dir_no);
        if (!s.ok()) {
          report.errs++;
          break;
        } else {
          report.ops++;
        }
        if (!options_.relaxed_consistency) {
          dir_no += options_.comm_sz;
        } else {
          dir_no += 1;
        }
      }
    }

    report.duration = MPI_Wtime() - start;
    return report;
  }

  // Collectively create files under parent directories.
  // Return a status report with local timing and error counts.
  LDbenchReport BulkCreates() {
    double start = MPI_Wtime();
    LDbenchReport report;
    report.errs = 0;
    report.ops = 0;
    Status s;
    if (!options_.skip_inserts) {
      for (int f = options_.rank; f < options_.num_files;) {
        int dir_no = Dir(f) % options_.num_dirs;
        s = Mknod(dir_no, f);
        f += options_.comm_sz;
        if (!s.ok()) {
          report.errs++;
          break;
        } else {
          report.ops++;
        }
      }
    }

    report.duration = MPI_Wtime() - start;
    return report;
  }

  // Collectively read the metadata of all created files.
  // Return a status report with local timing and error counts.
  LDbenchReport Check() {
    double start = MPI_Wtime();
    LDbenchReport report;
    report.errs = 0;
    report.ops = 0;
    Status s;
    if (!options_.skip_reads) {
      for (int f = options_.rank; f < options_.num_files;) {
        int dir_no = Dir(f) % options_.num_dirs;
        s = Fstat(dir_no, f);
        f += options_.comm_sz;
        if (!s.ok()) {
          report.errs++;
          break;
        } else {
          report.ops++;
        }
      }
    }

    report.duration = MPI_Wtime() - start;
    return report;
  }

 private:
  const LDbenchOptions options_;
  ioclient::IOClient* io_;
};

}  // namespace pdlfs

static pdlfs::LDbenchReport Merge(const pdlfs::LDbenchReport& report) {
  pdlfs::LDbenchReport result;

  MPI_Reduce(&report.duration, &result.duration, 1, MPI_DOUBLE, MPI_MAX, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&report.errs, &result.errs, 1, MPI_INT, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(&report.ops, &result.ops, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

  return result;
}

static void Print(const pdlfs::LDbenchReport& report) {
  printf("-- Performed %d ops in %.3f seconds: %d succ, %d fail\n",
         report.ops + report.errs, report.duration, report.ops, report.errs);
}

static void Print(const char* msg) {
  // Usually only called by rank 0
  printf("== %s\n", msg);
}

static void Help(const char* prog, FILE* out) {
  fprintf(out,
          "%s [options] <io_type> <io_conf>\n\nOptions:\n\n"
          "  --relaxed-consistency  :  "
          "Executes in relaxed consistency mode\n"
          "  --ignored-erros        :  "
          "Continue running even on errors\n"
          "  --skip-inserts         :  "
          "Skip the insertion phase\n"
          "  --skip-reads           :  "
          "Skip the read phase\n"
          "  --num-files=n          :  "
          "Total number of files to create\n"
          "  --num-dirs=n           :  "
          "Total number of dirs to create\n\n"
          "Deltafs benchmark\n",
          prog);
}

static pdlfs::LDbenchOptions ParseOptions(int argc, char** argv) {
  pdlfs::LDbenchOptions result;
  result.rank = 0;
  result.comm_sz = 1;
  result.relaxed_consistency = false;
  result.ignore_errors = false;
  result.skip_inserts = false;
  result.skip_reads = false;
  result.num_files = 16;
  result.num_dirs = 1;
  result.argv = NULL;
  result.argc = 0;

  {
    std::vector<struct option> optinfo;
    optinfo.push_back({"relaxed-consistency", 0, NULL, 1});
    optinfo.push_back({"ignore-errors", 0, NULL, 2});
    optinfo.push_back({"skip-inserts", 0, NULL, 3});
    optinfo.push_back({"skip-reads", 0, NULL, 4});
    optinfo.push_back({"num-files", 1, NULL, 5});
    optinfo.push_back({"num-dirs", 1, NULL, 6});
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
            result.skip_inserts = true;
            break;
          case 4:
            result.skip_reads = true;
            break;
          case 5:
            result.num_files = atoi(optarg);
            break;
          case 6:
            result.num_dirs = atoi(optarg);
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
        break;
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
  pdlfs::LDbenchOptions options = ParseOptions(argc, argv);
  options.rank = rank;
  options.comm_sz = size;
  pdlfs::LDbench bench(options);
  pdlfs::LDbenchReport report;
  report.errs = 0;

  if (report.errs == 0) {
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

  if (report.errs == 0) {
    if (rank == 0) {
      Print("Bulk creations ... ");
    }
    MPI_Barrier(MPI_COMM_WORLD);
    report = Merge(bench.BulkCreates());
    if (rank == 0) {
      Print(report);
    }
  } else {
    if (rank == 0) {
      Print("Abort");
      goto end;
    }
  }

  if (report.errs == 0) {
    if (rank == 0) {
      Print("Checks ... ");
    }
    MPI_Barrier(MPI_COMM_WORLD);
    report = Merge(bench.Check());
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
