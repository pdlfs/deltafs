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
    options.session_id = session_id_;
    options.dir_id = id;
    ReadidxRet ret;
    assert(zserver >= 0);
    size_t server = zserver % giga_.num_servers;
    s = factory_->Get(server)->Readidx(options, &ret);
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
      const DirIndex* idx = index_cache_->Value(idxh);
      assert(idx != NULL);
      LookupOptions options;
      options.op_due = atomic_path_resolution_ ? op_due : kMaxMicros;
      options.session_id = session_id_;
      options.dir_id = pid;
      options.name_hash = nhash;
      if (paranoid_checks_) {
        options.name = name;
      }
      LookupRet ret;

      const DirIndex* latest_idx = idx;
      DirIndex* tmp_idx = NULL;
      int redirects_allowed = max_redirects_allowed_;
      do {
        try {
          size_t server = latest_idx->HashToServer(nhash);
          assert(server < giga_.num_servers);
          s = factory_->Get(server)->Lookup(options, &ret);
        } catch (Redirect& re) {
          if (tmp_idx == NULL) {
            tmp_idx = new DirIndex(&giga_);
            tmp_idx->Update(*idx);
          }
          if (--redirects_allowed == 0 || !tmp_idx->Update(re)) {
            s = Status::Corruption(Slice());
          } else {
            s = Status::TryAgain(Slice());
          }
          latest_idx = tmp_idx;
        }
      } while (s.IsTryAgain());
      if (s.ok()) {
        if (paranoid_checks_ && !S_ISDIR(ret.stat.DirMode())) {
          s = Status::Corruption(Slice());
        }
      }

      mutex_.Lock();
      if (tmp_idx != NULL) {
        if (s.ok()) {
          index_cache_->Release(index_cache_->Insert(pid, tmp_idx));
        } else {
          delete tmp_idx;
        }
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

Status MDS::CLI::Fstat(const Slice& p, Fentry* ent) {
  Status s;
  char tmp[20];
  PathInfo path;
  MutexLock ml(&mutex_);
  s = ResolvePath(p, &path);
  if (s.ok()) {
    IndexHandle* idxh = NULL;
    s = FetchIndex(path.pid, path.zserver, &idxh);
    if (s.ok()) {
      mutex_.Unlock();

      assert(idxh != NULL);
      const DirIndex* idx = index_cache_->Value(idxh);
      assert(idx != NULL);
      FstatOptions options;
      options.op_due = atomic_path_resolution_ ? path.lease_due : kMaxMicros;
      options.session_id = session_id_;
      options.dir_id = path.pid;
      options.name_hash = DirIndex::Hash(path.name, tmp);
      if (paranoid_checks_) {
        options.name = path.name;
      }
      FstatRet ret;

      const DirIndex* latest_idx = idx;
      DirIndex* tmp_idx = NULL;
      int redirects_allowed = max_redirects_allowed_;
      do {
        try {
          size_t server = latest_idx->HashToServer(options.name_hash);
          assert(server < giga_.num_servers);
          s = factory_->Get(server)->Fstat(options, &ret);
        } catch (Redirect& re) {
          if (tmp_idx == NULL) {
            tmp_idx = new DirIndex(&giga_);
            tmp_idx->Update(*idx);
          }
          if (--redirects_allowed == 0 || !tmp_idx->Update(re)) {
            s = Status::Corruption(Slice());
          } else {
            s = Status::TryAgain(Slice());
          }
          latest_idx = tmp_idx;
        }
      } while (s.IsTryAgain());

      mutex_.Lock();
      index_cache_->Release(idxh);
      if (tmp_idx != NULL) {
        if (s.ok()) {
          index_cache_->Release(index_cache_->Insert(path.pid, tmp_idx));
        } else {
          delete tmp_idx;
        }
      }
      if (s.ok()) {
        if (ent != NULL) {
          ent->pid = path.pid;
          ent->nhash = options.name_hash.ToString();
          ent->zserver = path.zserver;
          ent->stat = ret.stat;
        }
      }
    }
  }

  return s;
}

Status MDS::CLI::Fcreat(const Slice& p, bool error_if_exists, int mode,
                        Fentry* ent) {
  Status s;
  char tmp[20];
  PathInfo path;
  MutexLock ml(&mutex_);
  s = ResolvePath(p, &path);
  if (s.ok()) {
    IndexHandle* idxh = NULL;
    s = FetchIndex(path.pid, path.zserver, &idxh);
    if (s.ok()) {
      mutex_.Unlock();

      assert(idxh != NULL);
      const DirIndex* idx = index_cache_->Value(idxh);
      assert(idx != NULL);
      FcreatOptions options;
      options.op_due = atomic_path_resolution_ ? path.lease_due : kMaxMicros;
      options.session_id = session_id_;
      options.dir_id = path.pid;
      options.flags = error_if_exists ? O_EXCL : 0;
      options.mode = mode;
      options.uid = uid_;
      options.gid = gid_;
      options.name_hash = DirIndex::Hash(path.name, tmp);
      options.name = path.name;
      FcreatRet ret;

      const DirIndex* latest_idx = idx;
      DirIndex* tmp_idx = NULL;
      int redirects_allowed = max_redirects_allowed_;
      do {
        try {
          size_t server = latest_idx->HashToServer(options.name_hash);
          assert(server < giga_.num_servers);
          s = factory_->Get(server)->Fcreat(options, &ret);
        } catch (Redirect& re) {
          if (tmp_idx == NULL) {
            tmp_idx = new DirIndex(&giga_);
            tmp_idx->Update(*idx);
          }
          if (--redirects_allowed == 0 || !tmp_idx->Update(re)) {
            s = Status::Corruption(Slice());
          } else {
            s = Status::TryAgain(Slice());
          }
          latest_idx = tmp_idx;
        }
      } while (s.IsTryAgain());
      if (s.ok()) {
        if (paranoid_checks_ && !S_ISREG(ret.stat.FileMode())) {
          s = Status::Corruption(Slice());
        }
      }

      mutex_.Lock();
      index_cache_->Release(idxh);
      if (tmp_idx != NULL) {
        if (s.ok()) {
          index_cache_->Release(index_cache_->Insert(path.pid, tmp_idx));
        } else {
          delete tmp_idx;
        }
      }
      if (s.ok()) {
        if (ent != NULL) {
          ent->pid = path.pid;
          ent->nhash = options.name_hash.ToString();
          ent->zserver = path.zserver;
          ent->stat = ret.stat;
        }
      }
    }
  }

  return s;
}

Status MDS::CLI::Mkdir(const Slice& p, int mode) {
  Status s;
  char tmp[20];
  PathInfo path;
  MutexLock ml(&mutex_);
  s = ResolvePath(p, &path);
  if (s.ok()) {
    IndexHandle* idxh = NULL;
    s = FetchIndex(path.pid, path.zserver, &idxh);
    if (s.ok()) {
      mutex_.Unlock();

      assert(idxh != NULL);
      const DirIndex* idx = index_cache_->Value(idxh);
      assert(idx != NULL);
      MkdirOptions options;
      options.op_due = atomic_path_resolution_ ? path.lease_due : kMaxMicros;
      options.session_id = session_id_;
      options.dir_id = path.pid;
      options.mode = mode;
      options.uid = uid_;
      options.gid = gid_;
      options.name_hash = DirIndex::Hash(path.name, tmp);
      options.name = path.name;
      MkdirRet ret;

      const DirIndex* latest_idx = idx;
      DirIndex* tmp_idx = NULL;
      int redirects_allowed = max_redirects_allowed_;
      do {
        try {
          size_t server = latest_idx->HashToServer(options.name_hash);
          assert(server < giga_.num_servers);
          s = factory_->Get(server)->Mkdir(options, &ret);
        } catch (Redirect& re) {
          if (tmp_idx == NULL) {
            tmp_idx = new DirIndex(&giga_);
            tmp_idx->Update(*idx);
          }
          if (--redirects_allowed == 0 || !tmp_idx->Update(re)) {
            s = Status::Corruption(Slice());
          } else {
            s = Status::TryAgain(Slice());
          }
          latest_idx = tmp_idx;
        }
      } while (s.IsTryAgain());
      if (s.ok()) {
        if (paranoid_checks_ && !S_ISDIR(ret.stat.FileMode())) {
          s = Status::Corruption(Slice());
        }
      }

      mutex_.Lock();
      index_cache_->Release(idxh);
      if (tmp_idx != NULL) {
        if (s.ok()) {
          index_cache_->Release(index_cache_->Insert(path.pid, tmp_idx));
        } else {
          delete tmp_idx;
        }
      }
    }
  }

  return s;
}

// Send inode status changes to metadata server so the metadata server
// may keep an updated view of the file. Cannot operate on directories.
// Metadata server identifies files through file paths. However, file
// path may change (due to rename, unlink, and creat operations)
// as long as we don't keep an active lease on the path that we use to
// open the file in the first place.
//
// Current solution is to always send file ids (reg_id + snap_id + ino)
// along with status updates. So the metadata server can detect
// conflicts either when the file we want to update no long exists
// (e.g. concurrently unlinked by others) or is no longer associated
// with the path (e.g. concurrently renamed by others).
Status MDS::CLI::Ftruncate(const Fentry& ent, uint64_t mtime, uint64_t size) {
  Status s;
  IndexHandle* idxh = NULL;
  MutexLock ml(&mutex_);
  s = FetchIndex(ent.pid, ent.zserver, &idxh);
  if (s.ok()) {
    mutex_.Unlock();

    assert(idxh != NULL);
    const DirIndex* idx = index_cache_->Value(idxh);
    assert(idx != NULL);
    TruncOptions options;  // TODO: add file id to options
    options.op_due = kMaxMicros;
    options.session_id = session_id_;
    options.dir_id = ent.pid;
    options.name_hash = ent.nhash;
    options.mtime = mtime;
    options.size = size;
    TruncRet ret;

    const DirIndex* latest_idx = idx;
    DirIndex* tmp_idx = NULL;
    int redirects_allowed = max_redirects_allowed_;
    do {
      try {
        size_t server = latest_idx->HashToServer(ent.nhash);
        assert(server < giga_.num_servers);
        s = factory_->Get(server)->Trunc(options, &ret);
      } catch (Redirect& re) {
        if (tmp_idx == NULL) {
          tmp_idx = new DirIndex(&giga_);
          tmp_idx->Update(*idx);
        }
        if (--redirects_allowed == 0 || !tmp_idx->Update(re)) {
          s = Status::Corruption(Slice());
        } else {
          s = Status::TryAgain(Slice());
        }
        latest_idx = tmp_idx;
      }
    } while (s.IsTryAgain());
    if (s.ok()) {
      if (paranoid_checks_ && !S_ISREG(ret.stat.FileMode())) {
        s = Status::Corruption(Slice());
      }
    }

    mutex_.Lock();
    index_cache_->Release(idxh);
    if (tmp_idx != NULL) {
      if (s.ok()) {
        index_cache_->Release(index_cache_->Insert(ent.pid, tmp_idx));
      } else {
        delete tmp_idx;
      }
    }
  }

  return s;
}

Status MDS::CLI::Listdir(const Slice& p, std::vector<std::string>* names) {
  Status s;
  assert(!p.ends_with("/"));
  std::string fake_path = p.ToString();
  fake_path.append("/.");
  PathInfo path;
  s = ResolvePath(fake_path, &path);
  if (s.ok()) {
    IndexHandle* idxh = NULL;
    s = FetchIndex(path.pid, path.zserver, &idxh);
    if (s.ok()) {
      mutex_.Unlock();

      assert(idxh != NULL);
      const DirIndex* idx = index_cache_->Value(idxh);
      assert(idx != NULL);
      ListdirOptions options;
      options.op_due = atomic_path_resolution_ ? path.lease_due : kMaxMicros;
      options.session_id = session_id_;
      options.dir_id = path.pid;
      ListdirRet ret;
      ret.names = names;

      std::set<size_t> visited;
      int num_parts = 1 << idx->Radix();
      for (int i = 0; i < num_parts; i++) {
        if (idx->IsSet(i)) {
          size_t server = idx->GetServerForIndex(i);
          assert(server < giga_.num_servers);
          if (visited.count(server) == 0) {
            factory_->Get(server)->Listdir(options, &ret);
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
