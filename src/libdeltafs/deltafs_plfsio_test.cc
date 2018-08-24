/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "deltafs_plfsio_events.h"
#include "deltafs_plfsio_filter.h"
#include "deltafs_plfsio_internal.h"

#include "pdlfs-common/histogram.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"
#include "pdlfs-common/xxhash.h"

#include <stdlib.h>
#ifdef PDLFS_PLATFORM_POSIX
#ifdef PDLFS_OS_LINUX
#include <sched.h>
#include <sys/types.h>
#endif
#include <sys/resource.h>
#include <sys/time.h>
#endif

#include <algorithm>
#include <map>
#include <set>
#include <vector>

namespace pdlfs {
namespace plfsio {

template <size_t value_size = 32>
class WriteBufTest {
 public:
  explicit WriteBufTest(uint32_t seed = 301) : num_entries_(0), rnd_(seed) {
    options_.value_size = value_size;
    options_.key_size = 8;
    buf_ = new WriteBuffer(options_);
  }

  ~WriteBufTest() {
    delete buf_;  // Done
  }

  Iterator* Flush() {
    buf_->Finish();
    ASSERT_EQ(buf_->NumEntries(), num_entries_);
    return buf_->NewIterator();
  }

  void Add(uint64_t seq) {
    std::string key;
    PutFixed64(&key, seq);
    std::string value;
    test::RandomString(&rnd_, value_size, &value);
    kv_.insert(std::make_pair(key, value));
    buf_->Add(key, value);
    num_entries_++;
  }

  void CheckFirst(Iterator* iter) {
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    Slice value = iter->value();
    ASSERT_TRUE(value == kv_.begin()->second);
    Slice key = iter->key();
    ASSERT_TRUE(key == kv_.begin()->first);
  }

  void CheckLast(Iterator* iter) {
    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    Slice value = iter->value();
    ASSERT_TRUE(value == kv_.rbegin()->second);
    Slice key = iter->key();
    ASSERT_TRUE(key == kv_.rbegin()->first);
  }

  std::map<std::string, std::string> kv_;
  DirOptions options_;
  WriteBuffer* buf_;
  uint32_t num_entries_;
  Random rnd_;
};

TEST(WriteBufTest<>, FixedSizedValue) {
  Add(3);
  Add(2);
  Add(1);
  Add(5);
  Add(4);

  Iterator* iter = Flush();
  CheckFirst(iter);
  CheckLast(iter);
  delete iter;
}

class PlfsIoTest {
 public:
  PlfsIoTest() {
    dirname_ = test::TmpDir() + "/plfsio_test";
    options_.total_memtable_budget = 1 << 20;
    options_.block_batch_size = 256 << 10;
    options_.block_size = 64 << 10;
    options_.block_util = 0.998;
    options_.verify_checksums = true;
    options_.paranoid_checks = true;
    options_.env = Env::Default();
    writer_ = NULL;
    reader_ = NULL;
    epoch_ = 0;
  }

  ~PlfsIoTest() {
    if (writer_ != NULL) {
      delete writer_;
    }
    if (reader_ != NULL) {
      delete reader_;
    }
  }

  void OpenWriter() {
    DestroyDir(dirname_, options_);
    Status s = DirWriter::Open(options_, dirname_, &writer_);
    ASSERT_OK(s);
  }

  void Finish() {
    ASSERT_OK(writer_->Finish());
    delete writer_;
    writer_ = NULL;
  }

  void OpenReader() {
    Status s = DirReader::Open(options_, dirname_, &reader_);
    ASSERT_OK(s);
  }

  void MakeEpoch() {
    if (writer_ == NULL) OpenWriter();
    ASSERT_OK(writer_->EpochFlush(epoch_));
    epoch_++;
  }

  void Append(const Slice& key, const Slice& value) {
    if (writer_ == NULL) OpenWriter();
    ASSERT_OK(writer_->Add(key, value, epoch_));
  }

  struct SaverState {
    std::string* tmp;
    port::Mutex mu;
  };

  static int SaveValue(void* arg, const Slice& key, const Slice& value) {
    SaverState* st = reinterpret_cast<SaverState*>(arg);
    MutexLock ml(&st->mu);
    st->tmp->append(value.data(), value.size());
    return 0;
  }

  size_t Count(int epoch) {
    size_t result;
    DirReader::CountOp op;
    op.SetEpoch(epoch);
    if (writer_ != NULL) Finish();
    if (reader_ == NULL) OpenReader();
    ASSERT_OK(reader_->Count(op, &result));
    return result;
  }

  std::string Scan(int epoch) {
    std::string tmp;
    SaverState state;
    state.tmp = &tmp;
    DirReader::ScanOp op;
    op.SetEpoch(epoch);
    if (writer_ != NULL) Finish();
    if (reader_ == NULL) OpenReader();
    ASSERT_OK(reader_->Scan(op, SaveValue, &state));
    return tmp;
  }

  std::string Read(const Slice& key) {
    std::string tmp;
    DirReader::ReadOp op;
    if (writer_ != NULL) Finish();
    if (reader_ == NULL) OpenReader();
    ASSERT_OK(reader_->Read(op, key, &tmp));
    return tmp;
  }

  DirOptions options_;
  std::string dirname_;
  DirWriter* writer_;
  DirReader* reader_;
  int epoch_;
};

TEST(PlfsIoTest, Empty) {
  MakeEpoch();
  std::string val = Read("non-exists");
  ASSERT_TRUE(val.empty());
}

TEST(PlfsIoTest, SingleEpoch) {
  Append("k1", "v1");
  Append("k2", "v2");
  Append("k3", "v3");
  Append("k4", "v4");
  Append("k5", "v5");
  Append("k6", "v6");
  MakeEpoch();
  ASSERT_EQ(Read("k1"), "v1");
  ASSERT_TRUE(Read("k1.1").empty());
  ASSERT_EQ(Read("k2"), "v2");
  ASSERT_TRUE(Read("k2.1").empty());
  ASSERT_EQ(Read("k3"), "v3");
  ASSERT_TRUE(Read("k3.1").empty());
  ASSERT_EQ(Read("k4"), "v4");
  ASSERT_TRUE(Read("k4.1").empty());
  ASSERT_EQ(Read("k5"), "v5");
  ASSERT_TRUE(Read("k5.1").empty());
  ASSERT_EQ(Read("k6"), "v6");
  ASSERT_EQ(Scan(0), "v1v2v3v4v5v6");
  ASSERT_TRUE(Scan(1).empty());
  ASSERT_EQ(Count(0), 6);
  ASSERT_EQ(Count(1), 0);
}

TEST(PlfsIoTest, MultiEpoch) {
  Append("k1", "v1");
  Append("k2", "v2");
  MakeEpoch();
  Append("k1", "v3");
  Append("k2", "v4");
  MakeEpoch();
  Append("k1", "v5");
  Append("k2", "v6");
  MakeEpoch();
  ASSERT_EQ(Read("k1"), "v1v3v5");
  ASSERT_TRUE(Read("k1.1").empty());
  ASSERT_EQ(Read("k2"), "v2v4v6");
  ASSERT_EQ(Scan(-1), "v1v2v3v4v5v6");
  ASSERT_EQ(Scan(0), "v1v2");
  ASSERT_EQ(Scan(1), "v3v4");
  ASSERT_EQ(Scan(2), "v5v6");
  ASSERT_TRUE(Scan(3).empty());
  ASSERT_EQ(Count(0), 2);
  ASSERT_EQ(Count(1), 2);
  ASSERT_EQ(Count(2), 2);
  ASSERT_EQ(Count(3), 0);
}

TEST(PlfsIoTest, ArrayBlockFmt) {
  options_.leveldb_compatible = false;
  options_.fixed_kv_length = true;
  options_.value_size = 2;
  options_.key_size = 2;
  Append("k1", "v1");
  Append("k2", "v2");
  MakeEpoch();
  Append("k1", "v3");
  Append("k2", "v4");
  MakeEpoch();
  Append("k1", "v5");
  Append("k2", "v6");
  MakeEpoch();
  ASSERT_EQ(Read("k1"), "v1v3v5");
  ASSERT_TRUE(Read("k1.1").empty());
  ASSERT_EQ(Read("k2"), "v2v4v6");
  ASSERT_EQ(Scan(0), "v1v2");
  ASSERT_EQ(Scan(1), "v3v4");
  ASSERT_EQ(Scan(2), "v5v6");
  ASSERT_EQ(Count(0), 2);
  ASSERT_EQ(Count(1), 2);
  ASSERT_EQ(Count(2), 2);
  ASSERT_EQ(Count(3), 0);
}

TEST(PlfsIoTest, Unordered) {
  options_.mode = kDmUniqueUnordered;
  Append("k2", "v2");
  Append("k1", "v1");
  MakeEpoch();
  Append("k1", "v3");
  Append("k2", "v4");
  MakeEpoch();
  Append("k2", "v6");
  Append("k1", "v5");
  MakeEpoch();
  ASSERT_EQ(Read("k1"), "v1v3v5");
  ASSERT_TRUE(Read("k1.1").empty());
  ASSERT_EQ(Read("k2"), "v2v4v6");
  ASSERT_EQ(Scan(0), "v2v1");
  ASSERT_EQ(Scan(1), "v3v4");
  ASSERT_EQ(Scan(2), "v6v5");
  ASSERT_EQ(Count(0), 2);
  ASSERT_EQ(Count(1), 2);
  ASSERT_EQ(Count(2), 2);
  ASSERT_EQ(Count(3), 0);
}

TEST(PlfsIoTest, UnorderedWithArrayBlockFmt) {
  options_.mode = kDmUniqueUnordered;
  options_.leveldb_compatible = false;
  options_.fixed_kv_length = true;
  options_.value_size = 2;
  options_.key_size = 2;
  Append("k2", "v2");
  Append("k1", "v1");
  MakeEpoch();
  Append("k1", "v3");
  Append("k2", "v4");
  MakeEpoch();
  Append("k2", "v6");
  Append("k1", "v5");
  MakeEpoch();
  ASSERT_EQ(Read("k1"), "v1v3v5");
  ASSERT_TRUE(Read("k1.1").empty());
  ASSERT_EQ(Read("k2"), "v2v4v6");
  ASSERT_EQ(Scan(0), "v2v1");
  ASSERT_EQ(Scan(1), "v3v4");
  ASSERT_EQ(Scan(2), "v6v5");
  ASSERT_EQ(Count(0), 2);
  ASSERT_EQ(Count(1), 2);
  ASSERT_EQ(Count(2), 2);
  ASSERT_EQ(Count(3), 0);
}

TEST(PlfsIoTest, Snappy1) {
  options_.compression = kSnappyCompression;
  options_.force_compression = true;
  Append("k1", "v1");
  Append("k2", "v2");
  MakeEpoch();
  Append("k1", "v3");
  Append("k2", "v4");
  MakeEpoch();
  Append("k1", "v5");
  Append("k2", "v6");
  MakeEpoch();
  ASSERT_EQ(Read("k1"), "v1v3v5");
  ASSERT_TRUE(Read("k1.1").empty());
  ASSERT_EQ(Read("k2"), "v2v4v6");
  ASSERT_EQ(Scan(0), "v1v2");
  ASSERT_EQ(Scan(1), "v3v4");
  ASSERT_EQ(Scan(2), "v5v6");
  ASSERT_EQ(Count(0), 2);
  ASSERT_EQ(Count(1), 2);
  ASSERT_EQ(Count(2), 2);
  ASSERT_EQ(Count(3), 0);
}

TEST(PlfsIoTest, Snappy2) {
  options_.index_compression = kSnappyCompression;
  options_.force_compression = true;
  Append("k1", "v1");
  Append("k2", "v2");
  MakeEpoch();
  Append("k1", "v3");
  Append("k2", "v4");
  MakeEpoch();
  Append("k1", "v5");
  Append("k2", "v6");
  MakeEpoch();
  ASSERT_EQ(Read("k1"), "v1v3v5");
  ASSERT_TRUE(Read("k1.1").empty());
  ASSERT_EQ(Read("k2"), "v2v4v6");
  ASSERT_EQ(Scan(0), "v1v2");
  ASSERT_EQ(Scan(1), "v3v4");
  ASSERT_EQ(Scan(2), "v5v6");
  ASSERT_EQ(Count(0), 2);
  ASSERT_EQ(Count(1), 2);
  ASSERT_EQ(Count(2), 2);
  ASSERT_EQ(Count(3), 0);
}

TEST(PlfsIoTest, LargeBatch) {
  const std::string dummy_val(32, 'x');
  const int batch_size = 64 << 10;
  char tmp[10];
  for (int i = 0; i < batch_size; i++) {
    snprintf(tmp, sizeof(tmp), "k%07d", i);
    Append(Slice(tmp), dummy_val);
  }
  MakeEpoch();
  for (int i = 0; i < batch_size; i++) {
    snprintf(tmp, sizeof(tmp), "k%07d", i);
    Append(Slice(tmp), dummy_val);
  }
  MakeEpoch();
  for (int i = 0; i < batch_size; i++) {
    snprintf(tmp, sizeof(tmp), "k%07d", i);
    ASSERT_EQ(Read(Slice(tmp)).size(), dummy_val.size() * 2) << tmp;
    if (i % 1024 == 1023) {
      fprintf(stderr, "key [%07d-%07d): OK\n", i - 1023, i + 1);
    }
  }
  ASSERT_TRUE(Read("kx").empty());
}

TEST(PlfsIoTest, NoFilter) {
  options_.bf_bits_per_key = 0;
  Append("k1", "v1");
  Append("k2", "v2");
  MakeEpoch();
  Append("k3", "v3");
  Append("k4", "v4");
  MakeEpoch();
  Append("k5", "v5");
  Append("k6", "v6");
  MakeEpoch();
  ASSERT_EQ(Read("k1"), "v1");
  ASSERT_TRUE(Read("k1.1").empty());
  ASSERT_EQ(Read("k2"), "v2");
  ASSERT_TRUE(Read("k2.1").empty());
  ASSERT_EQ(Read("k3"), "v3");
  ASSERT_TRUE(Read("k3.1").empty());
  ASSERT_EQ(Read("k4"), "v4");
  ASSERT_TRUE(Read("k4.1").empty());
  ASSERT_EQ(Read("k5"), "v5");
  ASSERT_TRUE(Read("k5.1").empty());
  ASSERT_EQ(Read("k6"), "v6");
  ASSERT_EQ(Scan(0), "v1v2");
  ASSERT_EQ(Scan(1), "v3v4");
  ASSERT_EQ(Scan(2), "v5v6");
  ASSERT_EQ(Count(0), 2);
  ASSERT_EQ(Count(1), 2);
  ASSERT_EQ(Count(2), 2);
  ASSERT_EQ(Count(3), 0);
}

TEST(PlfsIoTest, LogRotation) {
  options_.epoch_log_rotation = true;
  Append("k1", "v1");
  MakeEpoch();
  Append("k1", "v1");
  MakeEpoch();
  Append("k1", "v1");
  MakeEpoch();
  Finish();
}

TEST(PlfsIoTest, MultiMap) {
  options_.mode = kDmMultiMap;
  Append("k1", "v1");
  Append("k1", "v2");
  MakeEpoch();
  Append("k0", "v3");
  Append("k1", "v4");
  Append("k1", "v5");
  MakeEpoch();
  Append("k1", "v6");
  Append("k1", "v7");
  Append("k5", "v8");
  MakeEpoch();
  Append("k1", "v9");
  MakeEpoch();
  ASSERT_EQ(Read("k1"), "v1v2v4v5v6v7v9");
}

namespace {

class EmulatedWritableFile : public WritableFileWrapper {
 public:
  explicit EmulatedWritableFile(uint64_t bytes_ps, Histogram* hist = NULL,
                                EventListener* lis = NULL)
      : lis_(lis), prev_write_micros_(0), hist_(hist), bytes_ps_(bytes_ps) {}
  virtual ~EmulatedWritableFile() {}

  virtual Status Append(const Slice& data) {
    if (!data.empty()) {
      const uint64_t now_micros = Env::Default()->NowMicros();
      if (hist_ != NULL && prev_write_micros_ != 0) {
        hist_->Add(now_micros - prev_write_micros_);
      }
      prev_write_micros_ = now_micros;
      if (lis_ != NULL) {
        IoEvent event;
        event.micros = now_micros;
        event.type = kIoStart;
        lis_->OnEvent(kIoStart, &event);
      }
      const int micros_to_delay =
          static_cast<int>(1000 * 1000 * data.size() / bytes_ps_);
      Env::Default()->SleepForMicroseconds(micros_to_delay);
      if (lis_ != NULL) {
        IoEvent event;
        event.micros = Env::Default()->NowMicros();
        event.type = kIoEnd;
        lis_->OnEvent(kIoEnd, &event);
      }
    }
    return status_;
  }

 private:
  EventListener* lis_;
  uint64_t prev_write_micros_;  // Timestamp of the previous write
  Histogram* hist_;             // Mean time between writes

  uint64_t bytes_ps_;  // Bytes per second
  Status status_;
};

class EmulatedEnv : public EnvWrapper {
 public:
  EmulatedEnv(uint64_t bytes_ps, EventListener* lis)
      : EnvWrapper(Env::Default()), bytes_ps_(bytes_ps), lis_(lis) {}

  virtual ~EmulatedEnv() {
    HistIter iter = hists_.begin();
    for (; iter != hists_.end(); ++iter) {
      delete iter->second;
    }
  }

  virtual Status NewWritableFile(const char* f, WritableFile** r) {
    Slice fname(f);
    if (fname.ends_with(".dat")) {
      Histogram* hist = new Histogram;
      hists_.insert(std::make_pair(fname.ToString(), hist));
      *r = new EmulatedWritableFile(bytes_ps_, hist, lis_);
    } else {
      *r = new EmulatedWritableFile(bytes_ps_);
    }
    return Status::OK();
  }

  const Histogram* GetHist(const char* suffix) {
    HistIter iter = hists_.begin();
    for (; iter != hists_.end(); ++iter) {
      if (Slice(iter->first).ends_with(suffix)) {
        return iter->second;
      }
    }
    return NULL;
  }

 private:
  uint64_t bytes_ps_;  // Bytes per second

  EventListener* lis_;
  typedef std::map<std::string, Histogram*> HistMap;
  typedef HistMap::iterator HistIter;

  HistMap hists_;
};

}  // anonymous namespace

class PlfsIoBench {
 public:
  static BitmapFormat GetBitmapFilterFormat(BitmapFormat deffmt) {
    const char* env = getenv("FT_TYPE");
    if (env == NULL) {
      return deffmt;
    } else if (env[0] == 0) {
      return deffmt;
    } else if (strcmp(env, "bf") == 0) {
      return deffmt;
    } else if (strcmp(env, "bmp") == 0) {
      return kFmtUncompressed;
    } else if (strcmp(env, "r") == 0) {
      return kFmtRoaring;
    } else if (strcmp(env, "fvbp") == 0) {
      return kFmtFastVarintPlus;
    } else if (strcmp(env, "vbp") == 0) {
      return kFmtVarintPlus;
    } else if (strcmp(env, "vb") == 0) {
      return kFmtVarint;
    } else if (strcmp(env, "fpfd") == 0) {
      return kFmtFastPfDelta;
    } else if (strcmp(env, "pfd") == 0) {
      return kFmtPfDelta;
    } else {
      fprintf(stderr, "Bad FT_TYPE: %s\n", env);
      exit(1);
    }
  }

  static FilterType GetFilterType(FilterType deftype) {
    const char* env = getenv("FT_TYPE");
    if (env == NULL) {
      return deftype;
    } else if (env[0] == 0) {
      return deftype;
    } else if (strcmp(env, "bf") == 0) {
      return kFtBloomFilter;
    } else if (strcmp(env, "bmp") == 0) {
      return kFtBitmap;
    } else if (strcmp(env, "r") == 0) {
      return kFtBitmap;
    } else if (strcmp(env, "fvbp") == 0) {
      return kFtBitmap;
    } else if (strcmp(env, "vbp") == 0) {
      return kFtBitmap;
    } else if (strcmp(env, "vb") == 0) {
      return kFtBitmap;
    } else if (strcmp(env, "fpfd") == 0) {
      return kFtBitmap;
    } else if (strcmp(env, "pfd") == 0) {
      return kFtBitmap;
    } else {
      fprintf(stderr, "Bad FT_TYPE: %s\n", env);
      exit(1);
    }
  }

  static int GetOption(const char* key, int defval) {
    const char* env = getenv(key);
    if (env == NULL) {
      return defval;
    } else if (strlen(env) == 0) {
      return defval;
    } else {
      return atoi(env);
    }
  }

  class EventPrinter : public EventListener {
   public:
    EventPrinter() : base_time_(Env::Default()->NowMicros()) {
      events_.reserve(1024);
      iops_.reserve(1024);
    }

    virtual ~EventPrinter() {}

    virtual void OnEvent(EventType type, void* arg) {
      switch (type) {
        case kCompactionStart:
        case kCompactionEnd: {
          CompactionEvent* event = static_cast<CompactionEvent*>(arg);
          event->micros -= base_time_;
          events_.push_back(*event);
          break;
        }
        case kIoStart:
        case kIoEnd: {
          IoEvent* event = static_cast<IoEvent*>(arg);
          event->micros -= base_time_;
          iops_.push_back(*event);
          break;
        }
        default:
          break;
      }
    }

    static std::string ToString(const CompactionEvent& e) {
      char tmp[20];
      snprintf(tmp, sizeof(tmp), "%.3f,%d,%s", 1.0 * e.micros / 1000.0 / 1000.0,
               static_cast<int>(e.part),
               e.type == kCompactionStart ? "START" : "END");
      return tmp;
    }

    static std::string ToString(const IoEvent& e) {
      char tmp[20];
      snprintf(tmp, sizeof(tmp), "%.3f,io,%s", 1.0 * e.micros / 1000.0 / 1000.0,
               e.type == kIoStart ? "START" : "END");
      return tmp;
    }

    void PrintEvents() {
      fprintf(stdout, "\n\n!!! Background Events !!!\n");
      fprintf(stdout, "\n-- XXX --\n");
      for (EventIter iter = events_.begin(); iter != events_.end(); ++iter) {
        fprintf(stdout, "%s\n", ToString(*iter).c_str());
      }
      for (IoIter iter = iops_.begin(); iter != iops_.end(); ++iter) {
        fprintf(stdout, "%s\n", ToString(*iter).c_str());
      }
      fprintf(stdout, "\n-- XXX --\n");
    }

   private:
    uint64_t base_time_;

    typedef std::vector<IoEvent> IoQueue;
    typedef IoQueue::iterator IoIter;
    typedef std::vector<CompactionEvent> EventQueue;
    typedef EventQueue::iterator EventIter;

    EventQueue events_;
    IoQueue iops_;
  };

  PlfsIoBench() : home_(test::TmpDir() + "/plfsio_test_benchmark") {
    mbps_ = GetOption("LINK_SPEED", 6);  // per LANL's configuration
    ordered_keys_ = GetOption("ORDERED_KEYS", false);
    mfiles_ = GetOption("NUM_FILES", 16);  // 16 million per epoch

    num_threads_ = GetOption("NUM_THREADS", 4);  // Threads for bg compaction
    // For advanced perf diagnosis
    print_events_ = GetOption("PRINT_EVENTS", false);
    force_fifo_ = GetOption("FORCE_FIFO", false);

    options_.rank = 0;  // My process id
    if (GetOption("UNORDERED_MODE", false) != 0) {
      options_.mode = kDmUniqueUnordered;
    } else {
#ifndef NDEBUG
      options_.mode = kDmUniqueKey;
#else
      options_.mode = kDmUniqueDrop;
#endif
    }
    options_.lg_parts = GetOption("LG_PARTS", 2);
    options_.skip_sort = ordered_keys_ != 0;
    options_.leveldb_compatible = GetOption("LEVELDB_FMT", true) != 0;
    options_.fixed_kv_length = GetOption("FIXED_KV", true) != 0;
    options_.compression =
        GetOption("SNAPPY", false) ? kSnappyCompression : kNoCompression;
    options_.index_compression =
        GetOption("SNAPPY", false) ? kSnappyCompression : kNoCompression;
    options_.force_compression = true;
    options_.total_memtable_budget =
        static_cast<size_t>(GetOption("MEMTABLE_SIZE", 48) << 20);
    options_.block_size =
        static_cast<size_t>(GetOption("BLOCK_SIZE", 32) << 10);
    options_.block_batch_size =
        static_cast<size_t>(GetOption("BLOCK_BATCH_SIZE", 4) << 20);
    options_.block_util = GetOption("BLOCK_UTIL", 996) / 1000.0;
    options_.block_padding = GetOption("BLOCK_PADDING", true) != 0;
    options_.bf_bits_per_key = static_cast<size_t>(GetOption("BF_BITS", 14));
    options_.bm_fmt = GetBitmapFilterFormat(kFmtUncompressed);
    options_.bm_key_bits = static_cast<size_t>(GetOption("BM_KEY_BITS", 24));
    options_.filter = GetFilterType(kFtBloomFilter);
    options_.filter_bits_per_key =
        static_cast<size_t>(GetOption("FT_BITS", 16));
    options_.value_size = static_cast<size_t>(GetOption("VALUE_SIZE", 40));
    options_.key_size = static_cast<size_t>(GetOption("KEY_SIZE", 8));
    options_.data_buffer =
        static_cast<size_t>(GetOption("DATA_BUFFER", 8) << 20);
    options_.min_data_buffer =
        static_cast<size_t>(GetOption("MIN_DATA_BUFFER", 6) << 20);
    options_.index_buffer =
        static_cast<size_t>(GetOption("INDEX_BUFFER", 2) << 20);
    options_.min_index_buffer =
        static_cast<size_t>(GetOption("MIN_INDEX_BUFFER", 2) << 20);
    options_.listener = &printer_;

    writer_ = NULL;

    env_ = NULL;
  }

  ~PlfsIoBench() {
    delete writer_;
    writer_ = NULL;
    delete env_;
    env_ = NULL;
  }

  void LogAndApply() {
    DestroyDir(home_, options_);
    MaybePrepareKeys(GetOption("PREPARE_KEYS", 0));
    DoIt();
  }

 protected:
  // Compare two 32-bit integers according to their binary encoding.
  // This is different from comparing their values.
  struct STLLessThan {
    bool operator()(uint32_t a, uint32_t b) {
      char tmp1[4];
      EncodeFixed32(tmp1, a);
      char tmp2[4];
      EncodeFixed32(tmp2, b);
      return memcmp(tmp1, tmp2, 4) < 0;
    }
  };

  // Pre-sort all keys so the compaction process
  // can skip the sort operation.
  void MaybeSortKeys() {
    if (options_.skip_sort) {
      fprintf(stderr, "Sorting keys ...\n");
      std::sort(keys_.begin(), keys_.end(), STLLessThan());
      fprintf(stderr, "Done!\n");
    }
  }

  // Pre-generate user keys if bitmap filters are used, or if explicitly
  // requested by user. Otherwise, keys will be lazy generated
  // using a hashing function.
  // REQUIRES: file count must honor key space.
  void MaybePrepareKeys(bool forced) {
    if (forced || options_.filter == kFtBitmap) {
      const int num_files = mfiles_ << 20;
      ASSERT_TRUE(num_files <= (1 << options_.bm_key_bits));
      keys_.clear();
      fprintf(stderr, "Generating keys ... (%d keys)\n", num_files);
      keys_.reserve(static_cast<size_t>(num_files));
      for (int i = 0; i < num_files; i++) {
        keys_.push_back(static_cast<uint32_t>(i));
      }
      std::random_shuffle(keys_.begin(), keys_.end());
      ASSERT_TRUE(keys_.size() == num_files);
      fprintf(stderr, "Done!\n");
      MaybeSortKeys();
    }
  }

  class BigBatch {
   public:
    BigBatch(const DirOptions& options, const std::vector<uint32_t>& keys,
             int base_offset, int size)
        : key_size_(options.key_size),  // Num bytes for each key
          dummy_val_(options.value_size, 'x'),
          options_(options),
          keys_(keys),  // Pre-generated user keys, optional
          use_external_keys_(!keys_.empty()),
          rnd_insertion_(!options_.skip_sort),
          base_offset_(static_cast<uint32_t>(base_offset)),  // Base location
          size_(static_cast<uint32_t>(size)),                // Batch size
          offset_(size_) {  // Current cursor location
      ASSERT_TRUE(key_size_ <= sizeof(key_));
      // Initialize the buffer space for keys
      memset(key_, 0, sizeof(key_));
      if (use_external_keys_) {  // Keys are pre-generated as 32-bit ints
        ASSERT_TRUE(key_size_ >= 4);
      } else {
        ASSERT_TRUE(key_size_ >= 8);
      }
    }

    void Reset(int base_offset, int size) {
      base_offset_ = static_cast<uint32_t>(base_offset);
      size_ = static_cast<uint32_t>(size);
      // Invalid offset, an explicit seek is required
      // before data can be fetched
      offset_ = size_;
    }

    bool Valid() const { return offset_ < size_; }
    uint32_t offset() const { return offset_; }
    Slice fid() const { return Slice(key_, key_size_); }
    Slice data() const { return dummy_val_; }

    void Seek(uint32_t offset) {
      offset_ = offset;
      if (Valid()) {
        MakeKey();
      }
    }

    void Next() {
      offset_++;
      if (Valid()) {
        MakeKey();
      }
    }

   private:
    // Constant after construction
    size_t key_size_;
    std::string dummy_val_;
    const DirOptions& options_;
    const std::vector<uint32_t>& keys_;
    bool use_external_keys_;
    bool rnd_insertion_;
    uint32_t base_offset_;
    uint32_t size_;

    void MakeKey() {
      const uint32_t index = base_offset_ + offset_;
      if (use_external_keys_) {  // Use pre-generated user keys
        ASSERT_TRUE(index < keys_.size());
        EncodeFixed32(key_, keys_[index]);
      } else if (rnd_insertion_) {  // Random insertion
        // Key collisions are still possible, though very unlikely
        uint64_t h = xxhash64(&index, sizeof(index), 0);
        memcpy(key_ + 8, &h, 8);
        memcpy(key_, &h, 8);
      } else {
        // Use big-endian to ensure key ordering
        uint64_t k = htobe64(index);
        memcpy(key_ + 8, &k, 8);
        memcpy(key_, &k, 8);
      }
    }

    Status status_;
    uint32_t offset_;
    char key_[20];
  };

#if defined(PDLFS_PLATFORM_POSIX) && defined(PDLFS_OS_LINUX)
  void* MaybeForceFifoScheduling(pthread_attr_t* attr) {
    if (!force_fifo_) return NULL;
    int min = sched_get_priority_min(SCHED_FIFO);
    int max = sched_get_priority_max(SCHED_FIFO);
    struct sched_param param;
    param.sched_priority = (min + max) / 2 + 1;
    int r1 = pthread_setschedparam(pthread_self(), SCHED_FIFO, &param);
    ASSERT_EQ(r1, 0);
    int r2 = pthread_attr_init(attr);
    ASSERT_EQ(r2, 0);
    int r3 = pthread_attr_setinheritsched(attr, PTHREAD_EXPLICIT_SCHED);
    ASSERT_EQ(r3, 0);
    int r4 = pthread_attr_setschedpolicy(attr, SCHED_FIFO);
    ASSERT_EQ(r4, 0);
    param.sched_priority = (min + max) / 2 - 1;
    int r5 = pthread_attr_setschedparam(attr, &param);
    ASSERT_EQ(r5, 0);
    return attr;
  }
#endif

  void DoIt() {
    bool owns_pool = false;
    if (num_threads_ != 0) {
#if defined(PDLFS_PLATFORM_POSIX) && defined(PDLFS_OS_LINUX)
      pthread_attr_t pthread_attr;
      void* attr = MaybeForceFifoScheduling(&pthread_attr);
      ThreadPool* pool = ThreadPool::NewFixed(num_threads_, true, attr);
#else
      ThreadPool* pool = ThreadPool::NewFixed(num_threads_, true);
#endif
      options_.compaction_pool = pool;
      owns_pool = true;
    } else {
      options_.allow_env_threads = false;
      options_.compaction_pool = NULL;
    }
    bool owns_env = false;
    if (env_ == NULL) {
      const uint64_t speed = static_cast<uint64_t>(mbps_ << 20);
      env_ = new EmulatedEnv(speed, &printer_);
      owns_env = true;
    }
    options_.env = env_;
    Status s = DirWriter::Open(options_, home_, &writer_);
    ASSERT_OK(s) << "Cannot open dir";
    Env::Default()->SleepForMicroseconds(1000);
#ifdef PDLFS_PLATFORM_POSIX
    struct rusage tmp_usage;
    int r0 = getrusage(RUSAGE_SELF, &tmp_usage);
    ASSERT_EQ(r0, 0);
#endif
    const uint64_t start = env_->NowMicros();
    fprintf(stderr, "Inserting data...\n");
    int i = 0;
    const int num_files = (mfiles_ << 20);
    BigBatch batch(options_, keys_, i, num_files);
    while (i < num_files) {
      // Report progress
      if ((i & 0x7FFFF) == 0) {
        fprintf(stderr, "\r%.2f%%", 100.0 * i / num_files);
      }
      s = writer_->Add(batch.fid(), batch.data(), 0);
      if (s.ok()) {
        batch.Next();
        i++;
      } else {
        break;
      }
    }
    ASSERT_OK(s) << "Cannot write";
    fprintf(stderr, "\r100.00%%");
    fprintf(stderr, "\n");

    s = writer_->EpochFlush(0);
    ASSERT_OK(s) << "Cannot flush epoch";
    s = writer_->Finish();
    ASSERT_OK(s) << "Cannot finish";

    fprintf(stderr, "Done!\n");
    const uint64_t end = env_->NowMicros();
    const uint64_t dura = end - start;
#ifdef PDLFS_PLATFORM_POSIX
    PrintStats(tmp_usage, dura, owns_env);
#else
    PrintStats(dura, owns_env);
#endif
    if (print_events_) {
      printer_.PrintEvents();
    }

    delete writer_;
    writer_ = NULL;

    if (owns_pool) {
      delete options_.compaction_pool;
      options_.compaction_pool = NULL;
    }
    if (owns_env) {
      delete options_.env;
      options_.env = NULL;
      env_ = NULL;
    }
  }

#ifdef PDLFS_PLATFORM_POSIX
  static inline double ToSecs(const struct timeval* tv) {
    return tv->tv_sec + tv->tv_usec / 1000.0 / 1000.0;
  }
#endif

  static const char* ToString(FilterType type) {
    switch (type) {
      case kFtBloomFilter:
        return "BF (std bloom filter)";
      case kFtBitmap:
        return "BM (bitmap)";
      default:
        return "Unknown";
    }
  }

  static const char* ToString(BitmapFormat type) {
    switch (type) {
      case kFmtUncompressed:
        return "Uncompressed";
      case kFmtRoaring:
        return "R";
      case kFmtFastVarintPlus:
        return "FAST-VBP";
      case kFmtVarintPlus:
        return "VBP";
      case kFmtVarint:
        return "VB";
      case kFmtFastPfDelta:
        return "FAST-PFD";
      case kFmtPfDelta:
        return "PFD";
      default:
        return "Unknown";
    }
  }

#ifdef PDLFS_PLATFORM_POSIX
  void PrintStats(const struct rusage& tmp_usage, uint64_t dura,
                  bool owns_env) {
#else
  void PrintStats(uint64_t dura, bool owns_env) {
#endif
    const double k = 1000.0, ki = 1024.0;
    fprintf(stderr, "----------------------------------------\n");
    const uint64_t total_memory_usage = writer_->TEST_total_memory_usage();
    fprintf(stderr, "     Total Memory Usage: %.3f MiB\n",
            total_memory_usage / ki / ki);
    fprintf(stderr, "             Total Time: %.3f s\n", dura / k / k);
    const IoStats stats = writer_->GetIoStats();
#ifdef PDLFS_PLATFORM_POSIX
    struct rusage usage;
    int r1 = getrusage(RUSAGE_SELF, &usage);
    ASSERT_EQ(r1, 0);
    double utime = ToSecs(&usage.ru_utime) - ToSecs(&tmp_usage.ru_utime);
    double stime = ToSecs(&usage.ru_stime) - ToSecs(&tmp_usage.ru_stime);
    fprintf(stderr, "          User CPU Time: %.3f s\n", utime);
    fprintf(stderr, "        System CPU Time: %.3f s\n", stime);
#ifdef PDLFS_OS_LINUX
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    int r2 = sched_getaffinity(getpid(), sizeof(cpu_set), &cpu_set);
    ASSERT_EQ(r2, 0);
    fprintf(stderr, "          Num CPU Cores: %d\n", CPU_COUNT(&cpu_set));
    fprintf(stderr, "              CPU Usage: %.1f%%\n",
            k * k * (utime + stime) / CPU_COUNT(&cpu_set) / dura * 100);
#endif
#endif
    fprintf(stderr, "               Dir Mode: ordered=%s\n",
            IsKeyUnOrdered(options_.mode) ? "No" : "Yes");
    fprintf(stderr,
            "             Input Keys: pre-generated=%s, pre-sorted=%s\n",
            keys_.empty() ? "No" : "Yes", ordered_keys_ ? "Yes" : "No");
    fprintf(stderr, "     Snappy Compression: %s\n",
            options_.index_compression == kSnappyCompression ? "Yes" : "No");
    fprintf(stderr, "            Blk Padding: %s\n",
            options_.block_padding ? "Yes" : "No");
    fprintf(stderr, "                 TB Fmt: %s\n",
            options_.leveldb_compatible ? "SST" : "CUSTOM");
    fprintf(stderr, "                FT Type: %s\n", ToString(options_.filter));
    fprintf(stderr, "          FT Mem Budget: %d (bits per key)\n",
            int(options_.filter_bits_per_key));
    if (options_.filter == kFtBloomFilter) {
      fprintf(stderr, "              BF Budget: %d (bits per key)\n",
              int(options_.bf_bits_per_key));
    } else if (options_.filter == kFtBitmap) {
      fprintf(stderr, "           BM Key Space: 0-2^%d\n",
              int(options_.bm_key_bits));
      fprintf(stderr, "                 BM Fmt: %s\n",
              ToString(options_.bm_fmt));
    }
    fprintf(stderr, "     Num Files Inserted: %d M\n", mfiles_);
    fprintf(stderr, "        Logic File Data: %d MiB\n",
            int((options_.key_size + options_.value_size) * mfiles_));
    fprintf(stderr, "  Total MemTable Budget: %d MiB\n",
            int(options_.total_memtable_budget) >> 20);
    fprintf(stderr, "      Estimated TB Size: %.3f MiB\n",
            writer_->TEST_estimated_sstable_size() / ki / ki);
    fprintf(stderr, "        Planned FT Size: %.3f KiB\n",
            writer_->TEST_planned_filter_size() / ki);
    fprintf(stderr, "     Estimated Blk Size: %d KiB (target util: %.1f%%)\n",
            int(options_.block_size) >> 10, options_.block_util * 100);
    fprintf(stderr, "Num MemTable Partitions: %d\n", 1 << options_.lg_parts);
    fprintf(stderr, "         Num Bg Threads: %d\n", num_threads_);
    if (owns_env) {
      fprintf(stderr, "    Emulated Link Speed: %d MiB/s (per log)\n", mbps_);
    } else {
      fprintf(stderr, "    Emulated Link Speed: N/A\n");
    }
    fprintf(stderr, "            Write Speed: %.3f MiB/s (observed by app)\n",
            1.0 * k * k * (options_.key_size + options_.value_size) * mfiles_ /
                dura);
    fprintf(stderr, "              Index Buf: %d MiB (x%d)\n",
            int(options_.index_buffer) >> 20, 1 << options_.lg_parts);
    fprintf(stderr, "     Min Index I/O Size: %d MiB\n",
            int(options_.min_index_buffer) >> 20);
    const uint64_t user_bytes =
        writer_->TEST_key_bytes() + writer_->TEST_value_bytes();
    fprintf(stderr, "  Aggregated TB Indexes: %.3f KiB\n",
            1.0 * writer_->TEST_raw_index_contents() / ki);
    fprintf(stderr, "          Aggregated FT: %.3f MiB (+%.2f%%)\n",
            1.0 * writer_->TEST_raw_filter_contents() / ki / ki,
            1.0 * writer_->TEST_raw_filter_contents() / user_bytes * 100);
    fprintf(stderr, "      Final Dir Indexes: %.3f MiB (+%.2f%%)\n",
            1.0 * stats.index_bytes / ki / ki,
            1.0 * stats.index_bytes / user_bytes * 100);
    fprintf(stderr, "             Index Cost: %.3f (bits per key)\n",
            8.0 * stats.index_bytes / double(mfiles_ << 20));
    fprintf(stderr, "         Compaction Buf: %d MiB (x%d)\n",
            int(options_.block_batch_size) >> 20, 1 << options_.lg_parts);
    fprintf(stderr, "               Data Buf: %d MiB\n",
            int(options_.data_buffer) >> 20);
    fprintf(stderr, "      Min Data I/O Size: %d MiB\n",
            int(options_.min_data_buffer) >> 20);
    fprintf(stderr, "        Total User Data: %.3f MiB (K+V)\n",
            1.0 * user_bytes / ki / ki);
    fprintf(stderr,
            "     Aggregated TB Data: %.3f MiB (%+.2f%% due to blk encoding "
            "and possible compression)\n",
            1.0 * writer_->TEST_raw_data_contents() / ki / ki,
            1.0 * writer_->TEST_raw_data_contents() / user_bytes * 100 - 100);
    fprintf(stderr,
            "         Final Dir Data: %.3f MiB (%+.2f%% due to blk encoding, "
            "checksums, and possible compression and padding)\n",
            1.0 * stats.data_bytes / ki / ki,
            1.0 * stats.data_bytes / user_bytes * 100 - 100);
    if (stats.data_bytes >= user_bytes) {
      fprintf(stderr, "Total Blk Encoding Cost: %.3f (bits per key)\n",
              8.0 * (stats.data_bytes - user_bytes) / double(mfiles_ << 20));
    } else {
      fprintf(stderr, "Total Blk Encoding Cost: N/A\n");
    }
    fprintf(stderr, "           Avg I/O Size: %.3f MiB\n",
            1.0 * stats.data_bytes / stats.data_ops / ki / ki);
    if (owns_env) {
      const Histogram* hist = dynamic_cast<EmulatedEnv*>(env_)->GetHist(".dat");
      ASSERT_TRUE(hist != NULL);
      fprintf(stderr, "                   MTBW: %.3f s\n",
              hist->Average() / k / k);
    } else {
      fprintf(stderr, "                   MTBW: N/A\n");
    }
    const uint32_t num_tables = writer_->TEST_num_sstables();
    fprintf(stderr, "               Total TB: %d\n", int(num_tables));
    fprintf(stderr, "       TB Per Partition: %.1f\n",
            1.0 * num_tables / (1 << options_.lg_parts));
    fprintf(stderr, "           Total TB Blk: %d\n",
            int(writer_->TEST_num_data_blocks()));
    fprintf(stderr, "   Total Keys Compacted: %.1f M (%d dropped)\n",
            1.0 * writer_->TEST_num_keys() / ki / ki,
            int(writer_->TEST_num_dropped_keys()));
    fprintf(stderr, "             Value Size: %d Bytes\n",
            int(options_.value_size));
    fprintf(stderr, "               Key Size: %d Bytes\n",
            int(options_.key_size));
  }

  int mbps_;  // Link speed to emulate (in MBps)
  int ordered_keys_;
  int mfiles_;        // Number of files to insert (in Millions)
  int num_threads_;   // Number of bg compaction threads
  int force_fifo_;    // Force real-time FIFO scheduling
  int print_events_;  // Dump background events
  EventPrinter printer_;
  std::vector<uint32_t> keys_;
  const std::string home_;
  DirOptions options_;
  DirWriter* writer_;
  Env* env_;
};

namespace {

class StringWritableFile : public WritableFileWrapper {
 public:
  explicit StringWritableFile(std::string* buffer) : buf_(buffer) {}
  virtual ~StringWritableFile() {}

  virtual Status Append(const Slice& data) {
    buf_->append(data.data(), data.size());
    return Status::OK();
  }

 private:
  // Owned by external code
  std::string* buf_;
};

class StringFile : public SequentialFile, public RandomAccessFile {
 public:
  explicit StringFile(const std::string* buffer) : buf_(buffer), off_(0) {}
  virtual ~StringFile() {}

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    if (offset > buf_->size()) {
      offset = buf_->size();
    }
    if (n > buf_->size() - offset) {
      n = buf_->size() - offset;
    }
    if (n != 0) {
      *result = Slice(buf_->data() + offset, n);
    } else {
      *result = Slice();
    }
    return Status::OK();
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    if (n > buf_->size() - off_) {
      n = buf_->size() - off_;
    }
    if (n != 0) {
      *result = Slice(buf_->data() + off_, n);
    } else {
      *result = Slice();
    }
    return Skip(n);
  }

  virtual Status Skip(uint64_t n) {
    if (n > buf_->size() - off_) {
      n = buf_->size() - off_;
    }
    off_ += n;
    return Status::OK();
  }

 private:
  // Owned by external code
  const std::string* buf_;
  size_t off_;
};

class StringEnv : public EnvWrapper {
 public:
  StringEnv() : EnvWrapper(Env::Default()) {}

  virtual ~StringEnv() {
    FSIter iter = fs_.begin();
    for (; iter != fs_.end(); ++iter) {
      delete iter->second;
    }
  }

  virtual Status NewWritableFile(const char* f, WritableFile** r) {
    std::string* buf = new std::string;
    fs_.insert(std::make_pair(std::string(f), buf));
    *r = new StringWritableFile(buf);
    return Status::OK();
  }

  virtual Status NewRandomAccessFile(const char* f, RandomAccessFile** r) {
    std::string* buf = Find(f);
    if (buf == NULL) {
      *r = NULL;
      return Status::NotFound(Slice());
    } else {
      *r = new StringFile(buf);
      return Status::OK();
    }
  }

  virtual Status NewSequentialFile(const char* f, SequentialFile** r) {
    std::string* buf = Find(f);
    if (buf == NULL) {
      *r = NULL;
      return Status::NotFound(Slice());
    } else {
      *r = new StringFile(buf);
      return Status::OK();
    }
  }

  virtual Status GetFileSize(const char* f, uint64_t* s) {
    std::string* buf = Find(f);
    if (buf == NULL) {
      *s = 0;
      return Status::NotFound(Slice());
    } else {
      *s = buf->size();
      return Status::OK();
    }
  }

 private:
  typedef std::map<std::string, std::string*> FS;
  typedef FS::iterator FSIter;

  std::string* Find(const char* f) {
    FSIter iter = fs_.begin();
    for (; iter != fs_.end(); ++iter) {
      if (Slice(iter->first) == f) {
        return iter->second;
      }
    }
    return NULL;
  }

  FS fs_;
};

class Histo {
 public:
  Histo() { Clear(); }

  void Add(uint32_t seeks) {
    sum_ += seeks;
    max_ = std::max(max_, seeks);
    if (seeks > 9) {
      seeks = 9;
    }
    histo_[seeks]++;
    num_++;
  }

  double CDF(uint32_t seeks) {
    if (num_ == 0) return 0;
    double subtotal = 0;
    for (uint32_t i = 0; i <= seeks; i++) {
      subtotal += histo_[i];
    }
    return subtotal / num_;
  }

  double Average() const {
    if (num_ == 0) return 0;
    return sum_ / num_;
  }

  void Clear() {
    memset(histo_, 0, sizeof(histo_));
    max_ = 0;
    num_ = 0;
    sum_ = 0;
  }

  uint32_t max_;
  uint32_t num_;  // Total number of seek records
  // Num of times we get i seeks (0<=i<=9)
  uint32_t histo_[10];
  double sum_;
};

}  // anonymous namespace

class PlfsQuBench : protected PlfsIoBench {
 public:
  PlfsQuBench() : PlfsIoBench() {
    num_threads_ = 0;
    mbps_ = 0;

    force_negative_lookups_ = GetOption("FALSE_KEYS", false);
    num_empty_reads_ = 0;
    num_reads_ = 0;

    options_.verify_checksums = false;
    options_.paranoid_checks = true;

    env_ = new StringEnv;
  }

  ~PlfsQuBench() {
    delete writer_;
    writer_ = NULL;
    delete reader_;
    reader_ = NULL;
    delete env_;
    env_ = NULL;
  }

  void LogAndApply() {
    PlfsIoBench::LogAndApply();
    RunQueries();
  }

 protected:
  void RunQueries() {
    options_.allow_env_threads = false;
    options_.reader_pool = NULL;
    options_.env = env_;
    Status s = DirReader::Open(options_, home_, &reader_);
    ASSERT_OK(s) << "Cannot open dir";
    fprintf(stderr, "Reading dir...\n");
    const uint64_t start = env_->NowMicros();
    const int num_files = (mfiles_ << 20);
    BigBatch batch(options_, keys_, 0, num_files);
    batch.Seek(0);
    uint64_t accumulated_seeks = 0;
    std::string dummy_buf;
    char tmp[20];
    memset(tmp, 0, sizeof(tmp));
    while (batch.Valid()) {
      uint32_t i = batch.offset();
      // Report progress
      if ((i & 0x3FFFF) == 0) {
        fprintf(stderr, "\r%.2f%%", 100.0 * i / num_files);
      }
      dummy_buf.clear();
      Slice k = batch.fid();
      if (force_negative_lookups_) {
        uint64_t h1 = xxhash64(k.data(), k.size(), 301);
        memcpy(tmp + 8, &h1, 8);
        uint64_t h2 = xxhash64(k.data(), k.size(), 103);
        memcpy(tmp, &h2, 8);
        k = Slice(tmp, options_.key_size);
      }
      DirReader::ReadOp op;
      s = reader_->Read(op, k, &dummy_buf);
      if (!s.ok()) {
        break;
      }
      const IoStats stats = reader_->GetIoStats();
      seeks_.Add(stats.data_ops - accumulated_seeks);
      accumulated_seeks = stats.data_ops;
      num_reads_++;
      if (dummy_buf.empty()) {
        num_empty_reads_++;
      }
      batch.Next();
    }
    ASSERT_OK(s) << "Cannot read";
    fprintf(stderr, "\r100.00%%\n");
    fprintf(stderr, "Done!\n");

    uint64_t dura = env_->NowMicros() - start;

    Report(dura);

    delete reader_;
    reader_ = NULL;
  }

  void Report(uint64_t dura) {
    const double k = 1000.0, ki = 1024.0;
    fprintf(stderr, "----------------------------------------\n");
    fprintf(stderr, "             Total Time: %.3f s\n", dura / k / k);
    fprintf(stderr, "          Avg Read Time: %.3f us\n",
            1.0 * dura / (mfiles_ << 20));
    fprintf(stderr, "              Num Reads: %.2f M\n", num_reads_ / ki / ki);
    fprintf(stderr, "          Num Neg Reads: %.2f M (%.2f%%)\n",
            num_empty_reads_ / ki / ki, 100.0 * num_empty_reads_ / num_reads_);
    fprintf(stderr, "    Avg Seeks Per Epoch: %.3f, MAX=%d\n", seeks_.Average(),
            int(seeks_.max_));
    fprintf(stderr, "            CDF 1 Seeks: %.6f\n", seeks_.CDF(1));
    fprintf(stderr, "                2 Seeks: %.6f\n", seeks_.CDF(2));
    fprintf(stderr, "                3 Seeks: %.6f\n", seeks_.CDF(3));
    fprintf(stderr, "                4 Seeks: %.6f\n", seeks_.CDF(4));
    fprintf(stderr, "                5 Seeks: %.6f\n", seeks_.CDF(5));
    fprintf(stderr, "                6 Seeks: %.6f\n", seeks_.CDF(6));
    fprintf(stderr, "                7 Seeks: %.6f\n", seeks_.CDF(7));
    fprintf(stderr, "                8 Seeks: %.6f\n", seeks_.CDF(8));
    fprintf(stderr, "               9+ Seeks: %.6f\n", seeks_.CDF(9));
    const IoStats stats = reader_->GetIoStats();
    fprintf(stderr, "  Total Indexes Fetched: %.3f MB\n",
            1.0 * stats.index_bytes / ki / ki);
    fprintf(stderr, "     Total Data Fetched: %.3f GB\n",
            1.0 * stats.data_bytes / ki / ki / ki);
    fprintf(stderr, "           Avg I/O size: %.3f KB\n",
            1.0 * stats.data_bytes / stats.data_ops / ki);
  }

  int force_negative_lookups_;
  DirReader* reader_;

  uint64_t num_empty_reads_;
  uint64_t num_reads_;
  Histo seeks_;
};

}  // namespace plfsio
}  // namespace pdlfs

#if defined(PDLFS_GFLAGS)
#include <gflags/gflags.h>
#endif
#if defined(PDLFS_GLOG)
#include <glog/logging.h>
#endif

static void BM_Usage() {
  fprintf(stderr, "Use --bench=io or --bench=qu to select a benchmark.\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "== workload confs\n");
  fprintf(stderr, "LINK_SPEED\n");
  fprintf(stderr, "NUM_FILES\n");
  fprintf(stderr, "UNORDERED_MODE\n");
  fprintf(stderr, "PREPARE_KEYS\n");
  fprintf(stderr, "ORDERED_KEYS\n");
  fprintf(stderr, "LEVELDB_FMT\n");
  fprintf(stderr, "FIXED_KV\n");
  fprintf(stderr, "VALUE_SIZE\n");
  fprintf(stderr, "KEY_SIZE\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "== plfsdir options\n");
  fprintf(stderr, "LG_PARTS\n");
  fprintf(stderr, "MIN_DATA_BUFFER\n");
  fprintf(stderr, "DATA_BUFFER\n");
  fprintf(stderr, "MIN_INDEX_BUFFER\n");
  fprintf(stderr, "INDEX_BUFFER\n");
  fprintf(stderr, "NUM_THREADS\n");
  fprintf(stderr, "MEMTABLE_SIZE\n");
  fprintf(stderr, "BLOCK_BATCH_SIZE\n");
  fprintf(stderr, "BLOCK_SIZE\n");
  fprintf(stderr, "BLOCK_UTIL\n");
  fprintf(stderr, "BLOCK_PADDING\n");
  fprintf(stderr, "SNAPPY\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "== plfsdir filter options\n");
  fprintf(stderr, "FT_TYPE (bf, bmp, r, fvbp, fpfd)\n");
  fprintf(stderr, "FT_BITS\n");
  fprintf(stderr, "BM_KEY_BITS\n");
  fprintf(stderr, "BF_BITS\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "== adv. options\n");
  fprintf(stderr, "FORCE_FIFO\n");
  fprintf(stderr, "FALSE_KEYS\n");
  fprintf(stderr, "\n");
}

static void BM_LogAndApply(const char* bm) {
  if (strcmp(bm, "io") == 0) {
    pdlfs::plfsio::PlfsIoBench bench;
    bench.LogAndApply();
  } else if (strcmp(bm, "qu") == 0) {
    pdlfs::plfsio::PlfsQuBench bench;
    bench.LogAndApply();
  } else {
    BM_Usage();
  }
}

static void BM_Main(int* argc, char*** argv) {
#if defined(PDLFS_GFLAGS)
  google::ParseCommandLineFlags(argc, argv, true);
#endif
#if defined(PDLFS_GLOG)
  google::InitGoogleLogging((*argv)[0]);
  google::InstallFailureSignalHandler();
#endif
  pdlfs::Slice bench_name;
  if (*argc > 1) {
    bench_name = pdlfs::Slice((*argv)[*argc - 1]);
  } else {
    BM_Usage();
  }
  if (bench_name.starts_with("--bench=")) {
    BM_LogAndApply(bench_name.c_str() + 8);
  } else {
    BM_Usage();
  }
}

int main(int argc, char* argv[]) {
  pdlfs::Slice token;
  if (argc > 1) {
    token = pdlfs::Slice(argv[argc - 1]);
  }
  if (!token.starts_with("--bench")) {
    return pdlfs::test::RunAllTests(&argc, &argv);
  } else {
    BM_Main(&argc, &argv);
    return 0;
  }
}
