/*
 * Copyright (c) 2014-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <set>

#include "mds_cli.h"
#include "pdlfs-common/mutexlock.h"

namespace pdlfs {

Status MDS::CLI::FetchIndex(const DirId& id, int zserver,
                            IndexHandle** result) {
  mutex_.AssertHeld();
  Status s;
  IndexHandle* h = index_cache_->Lookup(id);
  if (h == NULL) {
    mutex_.Unlock();

    DirIndex* idx = new DirIndex(&giga_);
    ReadidxOptions options;
    options.op_due = kMaxMicros;
    options.session_id = cli_id_;
    options.dir_id = id;
    ReadidxRet ret;
    assert(zserver > 0);
    size_t server = zserver % giga_.num_servers;
    assert(server < servers_.size());
    s = servers_[server]->Readidx(options, &ret);
    if (s.ok()) {
      if (!idx->Update(ret.idx) || idx->ZerothServer() != zserver) {
        s = Status::Corruption(Slice());
      }
    }

    mutex_.Lock();
    if (s.ok()) {
      h = index_cache_->Insert(id, idx);
    } else {
      delete idx;
    }
  }
  *result = h;
  return s;
}

Status MDS::CLI::Lookup(const DirId& pid, const Slice& name, int zserver,
                        uint64_t op_due, LookupHandle** result) {
  char tmp[20];
  Slice nhash = DirIndex::Hash(name, tmp);
  mutex_.AssertHeld();
  Status s;
  LookupHandle* h = lookup_cache_->Lookup(pid, nhash);
  if (h == NULL ||
      (env_->NowMicros() + 10) > lookup_cache_->Value(h)->LeaseDue()) {
    IndexHandle* idxh = NULL;
    s = FetchIndex(pid, zserver, &idxh);
    if (s.ok()) {
      mutex_.Unlock();

      assert(idxh != NULL);
      DirIndex* idx = index_cache_->Value(idxh);
      assert(idx != NULL);
      LookupOptions options;
      options.op_due = atomic_path_resolution_ ? op_due : kMaxMicros;
      options.session_id = cli_id_;
      options.dir_id = pid;
      options.name_hash = nhash;
      if (paranoid_checks_) {
        options.name = name;
      }
      LookupRet ret;

      const DirIndex* latest_idx = idx;
      DirIndex tmp_idx(&giga_);
      int redirects_allowed = max_redirects_allowed_;
      do {
        try {
          size_t server = latest_idx->HashToServer(nhash);
          assert(server < servers_.size());
          s = servers_[server]->Lookup(options, &ret);
        } catch (Redirect& re) {
          latest_idx = &tmp_idx;
          if (--redirects_allowed == 0 || !tmp_idx.Update(re)) {
            s = Status::Corruption(Slice());
          } else {
            s = Status::TryAgain(Slice());
          }
        }
      } while (s.IsTryAgain());
      if (s.ok()) {
        if (paranoid_checks_ && !S_ISDIR(ret.stat.DirMode())) {
          s = Status::Corruption(Slice());
        }
      }

      mutex_.Lock();
      if (s.ok() && latest_idx == &tmp_idx) {
        idx->Update(tmp_idx);
      }
      index_cache_->Release(idxh);
      if (s.ok()) {
        LookupStat* stat = new LookupStat(ret.stat);
        h = lookup_cache_->Insert(pid, nhash, stat);
        if (stat->LeaseDue() == 0) {
          lookup_cache_->Erase(pid, nhash);
        }
      }
    }
  }

  *result = h;
  return s;
}

Status MDS::CLI::ResolvePath(const Slice& path, PathInfo* result) {
  mutex_.AssertHeld();

  Status s;
  Slice input(path);
  assert(input.size() != 0);
  assert(input[0] == '/');
  result->lease_due = kMaxMicros;
  result->pid = DirId(0, 0, 0);
  result->zserver = 0;
  result->name = "/";
  result->depth = 0;
  if (input.size() == 1) {
    return s;
  }

  input.remove_prefix(1);
  result->depth++;
  assert(!input.ends_with("/"));
  const char* p = strchr(input.data(), '/');
  for (; p != NULL; p = strchr(input.data(), '/')) {
    const char* q = input.data();
    input.remove_prefix(p - q + 1);
    result->depth++;
    LookupHandle* lh = NULL;
    Slice name = Slice(q, p - q);
    s = Lookup(result->pid, name, result->zserver, result->lease_due, &lh);
    if (s.ok()) {
      assert(lh != NULL);
      LookupStat* stat = lookup_cache_->Value(lh);
      assert(stat != NULL);
      result->pid = DirId(stat->RegId(), stat->SnapId(), stat->InodeNo());
      result->zserver = stat->ZerothServer();
      if (stat->LeaseDue() < result->lease_due) {
        result->lease_due = stat->LeaseDue();
      }
      lookup_cache_->Release(lh);
    } else {
      break;
    }
  }

  if (s.ok()) {
    result->name = input;
  }
  return s;
}

Status MDS::CLI::Fstat(const Slice& path, Stat* stat) {
  Status s;
  char tmp[20];
  PathInfo info;
  MutexLock ml(&mutex_);
  s = ResolvePath(path, &info);
  if (s.ok()) {
    IndexHandle* idxh = NULL;
    s = FetchIndex(info.pid, info.zserver, &idxh);
    if (s.ok()) {
      mutex_.Unlock();

      assert(idxh != NULL);
      DirIndex* idx = index_cache_->Value(idxh);
      assert(idx != NULL);
      FstatOptions options;
      options.op_due = atomic_path_resolution_ ? info.lease_due : kMaxMicros;
      options.session_id = cli_id_;
      options.dir_id = info.pid;
      options.name_hash = DirIndex::Hash(info.name, tmp);
      if (paranoid_checks_) {
        options.name = info.name;
      }
      FstatRet ret;

      const DirIndex* latest_idx = idx;
      DirIndex tmp_idx(&giga_);
      int redirects_allowed = max_redirects_allowed_;
      do {
        try {
          size_t server = latest_idx->HashToServer(options.name_hash);
          assert(server < servers_.size());
          s = servers_[server]->Fstat(options, &ret);
        } catch (Redirect& re) {
          latest_idx = &tmp_idx;
          if (--redirects_allowed == 0 || !tmp_idx.Update(re)) {
            s = Status::Corruption(Slice());
          } else {
            s = Status::TryAgain(Slice());
          }
        }
      } while (s.IsTryAgain());

      mutex_.Lock();
      if (s.ok() && latest_idx == &tmp_idx) {
        idx->Update(tmp_idx);
      }
      index_cache_->Release(idxh);
      if (s.ok()) {
        *stat = ret.stat;
      }
    }
  }

  return s;
}

Status MDS::CLI::Fcreat(const Slice& path, int mode, Stat* stat) {
  Status s;
  char tmp[20];
  PathInfo info;
  MutexLock ml(&mutex_);
  s = ResolvePath(path, &info);
  if (s.ok()) {
    IndexHandle* idxh = NULL;
    s = FetchIndex(info.pid, info.zserver, &idxh);
    if (s.ok()) {
      mutex_.Unlock();

      assert(idxh != NULL);
      DirIndex* idx = index_cache_->Value(idxh);
      assert(idx != NULL);
      FcreatOptions options;
      options.op_due = atomic_path_resolution_ ? info.lease_due : kMaxMicros;
      options.session_id = cli_id_;
      options.dir_id = info.pid;
      options.mode = mode;
      options.uid = uid_;
      options.gid = gid_;
      options.name_hash = DirIndex::Hash(info.name, tmp);
      options.name = info.name;
      FcreatRet ret;

      const DirIndex* latest_idx = idx;
      DirIndex tmp_idx(&giga_);
      int redirects_allowed = max_redirects_allowed_;
      do {
        try {
          size_t server = latest_idx->HashToServer(options.name_hash);
          assert(server < servers_.size());
          s = servers_[server]->Fcreat(options, &ret);
        } catch (Redirect& re) {
          latest_idx = &tmp_idx;
          if (--redirects_allowed == 0 || !tmp_idx.Update(re)) {
            s = Status::Corruption(Slice());
          } else {
            s = Status::TryAgain(Slice());
          }
        }
      } while (s.IsTryAgain());
      if (s.ok()) {
        if (paranoid_checks_ && !S_ISREG(ret.stat.FileMode())) {
          s = Status::Corruption(Slice());
        }
      }

      mutex_.Lock();
      if (s.ok() && latest_idx == &tmp_idx) {
        idx->Update(tmp_idx);
      }
      index_cache_->Release(idxh);
      if (s.ok()) {
        *stat = ret.stat;
      }
    }
  }

  return s;
}

Status MDS::CLI::Mkdir(const Slice& path, int mode, Stat* stat) {
  Status s;
  char tmp[20];
  PathInfo info;
  MutexLock ml(&mutex_);
  s = ResolvePath(path, &info);
  if (s.ok()) {
    IndexHandle* idxh = NULL;
    s = FetchIndex(info.pid, info.zserver, &idxh);
    if (s.ok()) {
      mutex_.Unlock();

      assert(idxh != NULL);
      DirIndex* idx = index_cache_->Value(idxh);
      assert(idx != NULL);
      MkdirOptions options;
      options.op_due = atomic_path_resolution_ ? info.lease_due : kMaxMicros;
      options.session_id = cli_id_;
      options.dir_id = info.pid;
      options.zserver = PickupServer(info.pid);
      options.mode = mode;
      options.uid = uid_;
      options.gid = gid_;
      options.name_hash = DirIndex::Hash(info.name, tmp);
      options.name = info.name;
      MkdirRet ret;

      const DirIndex* latest_idx = idx;
      DirIndex tmp_idx(&giga_);
      int redirects_allowed = max_redirects_allowed_;
      do {
        try {
          size_t server = latest_idx->HashToServer(options.name_hash);
          assert(server < servers_.size());
          s = servers_[server]->Mkdir(options, &ret);
        } catch (Redirect& re) {
          latest_idx = &tmp_idx;
          if (--redirects_allowed == 0 || !tmp_idx.Update(re)) {
            s = Status::Corruption(Slice());
          } else {
            s = Status::TryAgain(Slice());
          }
        }
      } while (s.IsTryAgain());
      if (s.ok()) {
        if (paranoid_checks_ && !S_ISDIR(ret.stat.FileMode())) {
          s = Status::Corruption(Slice());
        }
      }

      mutex_.Lock();
      if (s.ok() && latest_idx == &tmp_idx) {
        idx->Update(tmp_idx);
      }
      index_cache_->Release(idxh);
      if (s.ok()) {
        *stat = ret.stat;
      }
    }
  }

  return s;
}

Status MDS::CLI::Listdir(const Slice& path, std::vector<std::string>* names) {
  Status s;
  assert(!path.ends_with("/"));
  std::string fake_path = path.ToString();
  fake_path.append("/.");
  PathInfo info;
  s = ResolvePath(fake_path, &info);
  if (s.ok()) {
    IndexHandle* idxh = NULL;
    s = FetchIndex(info.pid, info.zserver, &idxh);
    if (s.ok()) {
      mutex_.Unlock();

      assert(idxh != NULL);
      DirIndex* idx = index_cache_->Value(idxh);
      assert(idx != NULL);
      ListdirOptions options;
      options.op_due = atomic_path_resolution_ ? info.lease_due : kMaxMicros;
      options.session_id = cli_id_;
      options.dir_id = info.pid;
      ListdirRet ret;
      ret.names = names;

      std::set<size_t> visited;
      int num_parts = 1 << idx->Radix();
      for (int i = 0; i < num_parts; i++) {
        if (idx->GetBit(i)) {
          size_t server = idx->GetServerForIndex(i);
          assert(server < servers_.size());
          if (visited.count(server) == 0) {
            servers_[server]->Listdir(options, &ret);
            visited.insert(server);
            if (visited.size() >= giga_.num_servers) {
              break;
            }
          }
        }
      }

      mutex_.Lock();
      index_cache_->Release(idxh);
    }
  }
  return s;
}

}  // namespace pdlfs
