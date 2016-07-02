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

#include "mds_srv.h"
#include "pdlfs-common/dirlock.h"
#include "pdlfs-common/mutexlock.h"

namespace pdlfs {

// NOTE: can be called while mutex_ is NOT locked.
Status MDS::SRV::LoadDir(const DirId& id, DirInfo* info, DirIndex* index) {
  Status s;
  MDB::Tx* mdb_tx = NULL;

  // Load directory info. Create if missing...
  s = mdb_->GetInfo(id, info, mdb_tx);
  if (s.IsNotFound()) {
    mdb_tx = mdb_->CreateTx();
    info->mtime = env_->NowMicros();
    info->size = 0;
    s = mdb_->SetInfo(id, *info, mdb_tx);
  }

  // Load directory index. Create if missing...
  if (s.ok()) {
    s = mdb_->GetIdx(id, index, mdb_tx);
    if (s.IsNotFound()) {
      int zserver = PickupServer(id) % idx_opts_.num_virtual_servers;
      DirIndex tmp(id.ino, zserver, &idx_opts_);
      tmp.SetAll();  // Pre-split to all servers
      if (mdb_tx == NULL) {
        mdb_tx = mdb_->CreateTx();
      }
      s = mdb_->SetIdx(id, tmp, mdb_tx);
      if (s.ok()) {
        index->Update(tmp);
      }
    }
  }

  if (s.ok() && mdb_tx != NULL) {
    s = mdb_->Commit(mdb_tx);
  }

  if (mdb_tx != NULL) {
    mdb_->Release(mdb_tx);
  }

  return s;
}

Slice MDS::SRV::EncodeId(const DirId& id, char* scratch) {
  char* p = scratch;
  p = EncodeVarint64(p, id.reg);
  p = EncodeVarint64(p, id.snap);
  p = EncodeVarint64(p, id.ino);
  return Slice(scratch, p - scratch);
}

// Deterministically map directories to their zeroth servers.
int MDS::SRV::PickupServer(const DirId& id) {
  char tmp[30];
  Slice encoding = EncodeId(id, tmp);
  int zserver = DirIndex::RandomServer(encoding, 0);
  return zserver;
}

// Load the states of a directory into an in-memory LRU-cache.
// Return OK on success.
// Errors might occur when the directory being searched does not exist, when
// the LRU-cache is full, when the data read from DB is corrupted, and
// when there are bugs somewhere in the codebase :-|
// REQUIRES: mutex_ has been locked.
Status MDS::SRV::FetchDir(const DirId& id, Dir::Ref** ref) {
  char tmp[30];
  Slice id_encoding = EncodeId(id, tmp);
  mutex_.AssertHeld();
  *ref = NULL;
  Status s;

  while (s.ok() && (*ref) == NULL) {
    Dir::Ref* r = dirs_->Lookup(id);
    if (r != NULL) {
      *ref = r;
    } else {
      // Prevent multiple threads from loading a same directory at the same time
      if (loading_dirs_.Contains(id_encoding)) {
        do {
          loading_cv_.Wait();
        } while (loading_dirs_.Contains(id_encoding));
      } else {
        mutex_.Unlock();
        DirInfo dir_info;
        DirIndex dir_index(&idx_opts_);
        s = LoadDir(id, &dir_info, &dir_index);
        mutex_.Lock();
        if (s.ok()) {
          Dir* d = new Dir(&mutex_, &idx_opts_);
          d->mtime = dir_info.mtime;
          assert(dir_info.size >= 0);
          d->size = dir_info.size;
          d->num_leases = 0;
          d->index.Update(dir_index);
          d->tx.NoBarrier_Store(NULL);
          d->seq = 0;
          d->locked = false;
          try {
            r = dirs_->Insert(id, d);
          } catch (int err) {
            // Not expecting errors other than "buffer-full", which happens
            // when the directory cache is full and no entries can be evicted
            assert(err == ENOBUFS);
            s = Status::BufferFull(Slice());
            r = NULL;
          }
          if (r != NULL) {
            *ref = r;
          } else {
            delete d;
          }
        }

        assert(loading_dirs_.Contains(id_encoding));
        loading_dirs_.Erase(id_encoding);
        loading_cv_.SignalAll();
      }
    }
  }

  return s;
}

// Quickly check directory status. Return OK on success.
// Return a non-OK status when the directory contains errors and must be
// fenced from online operations.
// REQUIRES: mutex_ has been locked.
Status MDS::SRV::ProbeDir(const Dir* d) {
  mutex_.AssertHeld();
  if (!d->status.ok()) {
    return d->status;
  } else {
    return Status::OK();
  }
}

uint64_t MDS::SRV::NextIno() {
  mutex_.AssertHeld();
  return ++ino_;
}

void MDS::SRV::TryReuseIno(uint64_t ino) {
  mutex_.AssertHeld();
  if (ino == ino_) {
    --ino_;
  }
}

// Read file or directory stats. Return OK on success.
// Multiple read threads may read from the same parent directory concurrently
// and none of them will be blocked by each other or by any write thread.
// If there is an on-going write operation, reads will read from a snapshot
// created by the writing thread.
//
// Errors may occur when the file or directory being read does not exist,
// when the current server is not the right one for the specified file or
// directory, when the data read from DB is corrupted, and when other internal
// or external errors occur...
Status MDS::SRV::Fstat(const FstatOptions& options, FstatRet* ret) {
  Status s;
  Dir::Tx* tx = NULL;
  Dir::Ref* ref;
  DirId dir_id(options.reg_id, options.snap_id, options.dir_ino);
  Slice name_hash = options.name_hash;
  if (name_hash.empty()) {
    s = Status::InvalidArgument(Slice());
  } else if (paranoid_checks_ && !options.name.empty()) {
    std::string tmp;
    DirIndex::PutHash(&tmp, options.name);
    if (name_hash.compare(tmp) != 0) {
      s = Status::Corruption(Slice());
    }
  }

  if (s.ok()) {
    MutexLock ml(&mutex_);
    s = FetchDir(dir_id, &ref);
    if (s.ok()) {
      assert(ref != NULL);
      Dir::Guard guard(dirs_, ref);
      const Dir* const d = ref->value;
      assert(d != NULL);
      s = ProbeDir(d);
      if (s.ok()) {
        int srv_id = d->index.HashToServer(name_hash);
        if (srv_id != srv_id_) {
          Slice encoding = d->index.Encode();
          Redirect re(encoding.data(), encoding.size());
          throw re;
        }
      }
      if (s.ok()) {
        mutex_.Unlock();

        MDB::Tx* mdb_tx = NULL;
        tx = reinterpret_cast<Dir::Tx*>(d->tx.Acquire_Load());
        if (tx != NULL) {
          mdb_tx = tx->rep();
          tx->Ref();
        }

        Slice name;
        s = mdb_->GetNode(dir_id, name_hash, &ret->stat, &name, mdb_tx);
        // TODO: paranoid checks

        mutex_.Lock();
        if (tx != NULL) {
          bool last_ref = tx->Unref();
          if (!last_ref) {
            tx = NULL;
          }
        }
      }
    }
  }

  ret->stat.AssertAllSet();
  if (tx != NULL) {
    tx->Dispose(mdb_);
  }
  return s;
}

// Insert a new file into a parent directory. Return OK on success.
// The data generated by this operation is not guaranteed to reach the disk.
// Need to Sync the DB to ensure durability.
//
// Errors may occur when the name used by the file already exists, when
// the current server is not the right one for the specified file, when data
// would not go into the DB, and when other internal or external errors occur...
//
// Write operations against the same parent directory must be serialized so
// they always proceed one after another. Write operations should not block any
// concurrent read operations.
Status MDS::SRV::Fcreat(const FcreatOptions& options, FcreatRet* ret) {
  Status s;
  Dir::Tx* tx = NULL;
  Dir::Ref* ref;
  DirId dir_id(options.reg_id, options.snap_id, options.dir_ino);
  Slice name_hash = options.name_hash;
  if (name_hash.empty() || options.name.empty()) {
    s = Status::InvalidArgument(Slice());
  } else if (paranoid_checks_) {
    std::string tmp;
    DirIndex::PutHash(&tmp, options.name);
    if (name_hash.compare(tmp) != 0) {
      s = Status::Corruption(Slice());
    }
  }

  if (s.ok()) {
    MutexLock ml(&mutex_);
    s = FetchDir(dir_id, &ref);
    if (s.ok()) {
      assert(ref != NULL);
      Dir::Guard guard(dirs_, ref);
      Dir* const d = ref->value;
      assert(d != NULL);
      DirLock dl(d);
      s = ProbeDir(d);
      if (s.ok()) {
        int srv_id = d->index.HashToServer(name_hash);
        if (srv_id != srv_id_) {
          Slice encoding = d->index.Encode();
          Redirect re(encoding.data(), encoding.size());
          throw re;
        }
      }
      if (s.ok()) {
        uint64_t my_time = env_->NowMicros();
        uint64_t my_ino = NextIno();
        mutex_.Unlock();

        tx = new Dir::Tx(mdb_);
        tx->Ref();
        assert(d->tx.Acquire_Load() == NULL);
        d->tx.Release_Store(tx);
        MDB::Tx* mdb_tx = tx->rep();

        if (mdb_->Exists(dir_id, name_hash, mdb_tx)) {
          s = Status::AlreadyExists(Slice());
        } else {
          Stat* stat = &ret->stat;
          uint32_t mode = S_IFREG | (options.mode & ACCESSPERMS);
          stat->SetInodeNo(my_ino);
          stat->SetFileSize(0);
          stat->SetFileMode(mode);
          stat->SetUserId(options.uid);
          stat->SetGroupId(options.gid);
          stat->SetZerothServer(0);
          stat->SetModifyTime(my_time);
          stat->SetChangeTime(my_time);
          s = mdb_->SetNode(dir_id, name_hash, *stat, options.name, mdb_tx);
        }

        if (s.ok()) {
          DirInfo dir_info;
          dir_info.mtime = my_time;
          dir_info.size = 1 + d->size;
          s = mdb_->SetInfo(dir_id, dir_info, mdb_tx);
        }

        if (s.ok()) {
          s = mdb_->Commit(mdb_tx);
        }

        mutex_.Lock();
        if (s.ok()) {
          d->size = 1 + d->size;
          assert(my_time >= d->mtime);
          d->mtime = my_time;
        } else {
          TryReuseIno(my_ino);
        }
        assert(d->tx.NoBarrier_Load() == tx);
        d->tx.NoBarrier_Store(NULL);
        assert(tx != NULL);
        bool last_ref = tx->Unref();
        if (!last_ref) {
          tx = NULL;
        }
      }
    }
  }

  ret->stat.AssertAllSet();
  if (tx != NULL) {
    tx->Dispose(mdb_);
  }
  return s;
}

// Make a new directory under a parent. Return OK on success.
// The data generated by this operation is not guaranteed to reach the disk.
// Need to Sync the DB to ensure durability.
//
// Errors may occur when the name for the new directory already exists, when
// the current server is not the right one for the new directory, when data
// would not go into the DB, and when other internal or external errors occur...
//
// Write operations against the same parent directory must be serialized so
// they always proceed one after another. Write operations should not block any
// concurrent read operations.
Status MDS::SRV::Mkdir(const MkdirOptions& options, MkdirRet* ret) {
  Status s;
  Dir::Tx* tx = NULL;
  Dir::Ref* ref;
  DirId dir_id(options.reg_id, options.snap_id, options.dir_ino);
  Slice name_hash = options.name_hash;
  if (options.zserver >= idx_opts_.num_virtual_servers) {
    s = Status::InvalidArgument(Slice());
  } else if (name_hash.empty() || options.name.empty()) {
    s = Status::InvalidArgument(Slice());
  } else if (paranoid_checks_) {
    std::string tmp;
    DirIndex::PutHash(&tmp, options.name);
    if (name_hash.compare(tmp) != 0) {
      s = Status::Corruption(Slice());
    }
  }

  if (s.ok()) {
    MutexLock ml(&mutex_);
    s = FetchDir(dir_id, &ref);
    if (s.ok()) {
      assert(ref != NULL);
      Dir::Guard guard(dirs_, ref);
      Dir* const d = ref->value;
      assert(d != NULL);
      s = ProbeDir(d);
      if (s.ok()) {
        int srv_id = d->index.HashToServer(name_hash);
        if (srv_id != srv_id_) {
          Slice encoding = d->index.Encode();
          Redirect re(encoding.data(), encoding.size());
          throw re;
        }
      }
      if (s.ok()) {
        uint64_t my_time = env_->NowMicros();
        uint64_t my_ino = NextIno();
        mutex_.Unlock();

        tx = new Dir::Tx(mdb_);
        tx->Ref();
        assert(d->tx.Acquire_Load() == NULL);
        d->tx.Release_Store(tx);
        MDB::Tx* mdb_tx = tx->rep();

        if (mdb_->Exists(dir_id, name_hash, mdb_tx)) {
          s = Status::AlreadyExists(Slice());
        } else {
          Stat* stat = &ret->stat;
          uint32_t mode = S_IFDIR | (options.mode & ACCESSPERMS);
          stat->SetInodeNo(my_ino);
          stat->SetFileSize(0);
          stat->SetFileMode(mode);
          stat->SetUserId(options.uid);
          stat->SetGroupId(options.gid);
          stat->SetZerothServer(options.zserver);
          stat->SetModifyTime(my_time);
          stat->SetChangeTime(my_time);
          s = mdb_->SetNode(dir_id, name_hash, *stat, options.name, mdb_tx);
        }

        if (s.ok()) {
          DirInfo dir_info;
          dir_info.mtime = my_time;
          dir_info.size = 1 + d->size;
          s = mdb_->SetInfo(dir_id, dir_info, mdb_tx);
        }

        if (s.ok()) {
          s = mdb_->Commit(mdb_tx);
        }

        mutex_.Lock();
        if (s.ok()) {
          d->size = 1 + d->size;
          assert(my_time >= d->mtime);
          d->mtime = my_time;
        } else {
          TryReuseIno(my_ino);
        }
        assert(d->tx.NoBarrier_Load() == tx);
        d->tx.NoBarrier_Store(NULL);
        assert(tx != NULL);
        bool last_ref = tx->Unref();
        if (!last_ref) {
          tx = NULL;
        }
      }
    }
  }

  ret->stat.AssertAllSet();
  if (tx != NULL) {
    tx->Dispose(mdb_);
  }
  return s;
}

// Lookup a directory for pathname resolution. Return OK on success.
// Multiple read threads should be able to run concurrently without blocking
// each other or being blocked by any concurrent write operations.
// After a successful lookup operation, a lease can be issued to the calling
// client, who may cache the lookup result until the lease due.
//
// Upon issuing a lease
// ---------------------
//
//   A) If no lease exists against the version of data being read:
//      0) No lease would exist for a past version of the data
//         because each lookup state mutation operation updates
//         or deletes an old lease if one exists.
//      1) If there hasn't been a lease for a newer version of
//         the data, create a new lease for this version
//         and decide an initial expiration time.
//      2) Else, no lease should be generated.
//
//   B) If there happens to be a lease for the specific version:
//      1) If the lease is in free or read state, its expiration time
//         may be extended
//      2) If the lease is in write state, the lease may be reused
//         but must not be extended
//
// Note
// ----
//
//   Because of A-0, if there hasn't been a lease for a newer version
//   of the data, any lease, if one exists, must be one for the
//   current version of data.
//
//   It is possible that the data a lookup operation reads is stale
//   when the operation commits. In such cases, we reply on A-2
//   to happen in order for the lookup operation to notice and to not
//   issue any lease on stale data.
//
//   In order for A-2 to always happen: i) each lookup state mutation
//   operation must install an active lease for the new version; and
//   ii) all lookup operation must finish within the shortest possible
//   lease duration so it is guaranteed to see the newest lease, if
//   one exists.
//
//   Should a lookup operation complete beyond the lease duration,
//   it assumes its data is stale and thus will return with no
//   lease. Also, if the lease table is full at the moment, the
//   lookup operation also returns with no lease.
//
// Errors may occur when the entry in question does not exist or is not a
// directory, when the current server is not the right one for the entry,
// when the data being read from DB is corrupted, and when other internal
// or external error occurs...
Status MDS::SRV::Lookup(const LookupOptions& options, LookupRet* ret) {
  Status s;
  Dir::Tx* tx = NULL;
  Dir::Ref* ref;
  DirId dir_id(options.reg_id, options.snap_id, options.dir_ino);
  Slice name_hash = options.name_hash;
  if (name_hash.empty()) {
    s = Status::InvalidArgument(Slice());
  } else if (paranoid_checks_ && !options.name.empty()) {
    std::string tmp;
    DirIndex::PutHash(&tmp, options.name);
    if (name_hash.compare(tmp) != 0) {
      s = Status::Corruption(Slice());
    }
  }

  if (s.ok()) {
    MutexLock ml(&mutex_);
    s = FetchDir(dir_id, &ref);
    if (s.ok()) {
      assert(ref != NULL);
      Dir::Guard guard(dirs_, ref);
      const Dir* const d = ref->value;
      assert(d != NULL);
      s = ProbeDir(d);
      if (s.ok()) {
        int srv_id = d->index.HashToServer(name_hash);
        if (srv_id != srv_id_) {
          Slice encoding = d->index.Encode();
          Redirect re(encoding.data(), encoding.size());
          throw re;
        }
      }
      if (s.ok()) {
        uint64_t my_start = env_->NowMicros();
        uint64_t my_seq = d->seq;
        mutex_.Unlock();

        MDB::Tx* mdb_tx = NULL;
        tx = reinterpret_cast<Dir::Tx*>(d->tx.Acquire_Load());
        if (tx != NULL) {
          mdb_tx = tx->rep();
          tx->Ref();
        }

        ret->entry.SetLeaseDue(0);

        Slice name;
        Stat stat;
        s = mdb_->GetNode(dir_id, name_hash, &stat, &name, mdb_tx);
        // TODO: paranoid checks
        if (s.ok()) {
          if (!S_ISDIR(stat.FileMode())) {
            s = Status::DirExpected(Slice());
          }
        }

        if (s.ok()) {
          ret->entry.CopyFrom(stat);
        }

        mutex_.Lock();
        uint64_t my_end = env_->NowMicros();
        // No lease either we timeout or have a negative result, otherwise...
        if (s.ok() && (my_end - my_start) < (lease_duration_ - 10)) {
          Lease::Ref* lref = leases_->Lookup(dir_id, name_hash);
          if (lref == NULL) {
            Lease* l = new Lease;
            l->state = kFreeState;
            l->parent = d;
            l->due = 0;
            l->seq = 0;
            try {
              lref = leases_->Insert(dir_id, name_hash, l);
            } catch (int err) {
              // Not expecting errors other than "buffer-full"
              assert(err == ENOBUFS);
              // If the lease table is full, I can return with no lease
              // and the client does not have to know this error
              lref = NULL;
            }
            if (lref != NULL) {
              d->num_leases++;
            } else {
              delete l;
            }
          }
          // No lease will be issued if the lease table is full, otherwise...
          if (lref != NULL) {
            Lease::Guard lguard(leases_, lref);
            Lease* const l = lref->value;
            assert(l != NULL);
            // No lease if the data is possibly stale, otherwise...
            if (l->seq <= my_seq) {
              if (l->state != kWriteState) {
                l->state = kReadState;
                assert(my_end + lease_duration_ >= l->due);
                // TODO: implement dynamic lease duration
                l->due = my_end + lease_duration_;
              } else {
                // A concurrent write operation is in-progress, I am not
                // allowed to extend the lease nor change its state
              }
              ret->entry.SetLeaseDue(l->due);
            }
          }
        }
        if (tx != NULL) {
          bool last_ref = tx->Unref();
          if (!last_ref) {
            tx = NULL;
          }
        }
      }
    }
  }

  ret->entry.AssertAllSet();
  if (tx != NULL) {
    tx->Dispose(mdb_);
  }
  return s;
}

// Change the access mode of a file or directory. Return OK on success.
// Write operations within a single parent directory are executed
// sequentially. No write operation should block concurrent read operations.
//
// The data generated by this operation is not guaranteed to reach the disk.
// Need to Sync the DB to ensure durability.
//
// If the target object is a directory, this operation is subject to
// lease control. If there is a lease for the lookup state of the directory,
// the permission changes can only be applied after that lease expires.
// Also, a new lease must be installed before the operation commits.
//
// Errors may occur when the file or directory targeted does not exist,
// when the current server is not the right one for the target, when
// data read from DB is corrupted or new data would not go into the DB,
// and when other internal or external errors occur...
Status MDS::SRV::Chmod(const ChmodOptions& options, ChmodRet* ret) {
  Status s;
  Dir::Tx* tx = NULL;
  Dir::Ref* ref;
  DirId dir_id(options.reg_id, options.snap_id, options.dir_ino);
  Slice name_hash = options.name_hash;
  if (name_hash.empty()) {
    s = Status::InvalidArgument(Slice());
  } else if (paranoid_checks_ && !options.name.empty()) {
    std::string tmp;
    DirIndex::PutHash(&tmp, options.name);
    if (name_hash.compare(tmp) != 0) {
      s = Status::Corruption(Slice());
    }
  }

  if (s.ok()) {
    MutexLock ml(&mutex_);
    s = FetchDir(dir_id, &ref);
    if (s.ok()) {
      assert(ref != NULL);
      Dir::Guard guard(dirs_, ref);
      Dir* const d = ref->value;
      assert(d != NULL);
      DirLock dl(d);
      s = ProbeDir(d);
      if (s.ok()) {
        int srv_id = d->index.HashToServer(name_hash);
        if (srv_id != srv_id_) {
          Slice encoding = d->index.Encode();
          Redirect re(encoding.data(), encoding.size());
          throw re;
        }
      }
      if (s.ok()) {
        uint64_t my_start = env_->NowMicros();
        mutex_.Unlock();

        tx = new Dir::Tx(mdb_);
        tx->Ref();
        assert(d->tx.Acquire_Load() == NULL);
        d->tx.Release_Store(tx);
        MDB::Tx* mdb_tx = tx->rep();

        Stat* stat = &ret->stat;
        Slice name;
        s = mdb_->GetNode(dir_id, name_hash, stat, &name, mdb_tx);
        // TODO: paranoid checks
        if (s.ok()) {
          uint32_t non_access = ~ACCESSPERMS & stat->FileMode();
          stat->SetFileMode(non_access | (ACCESSPERMS & options.mode));
          stat->SetChangeTime(my_start);
          s = mdb_->SetNode(dir_id, name_hash, *stat, name, mdb_tx);
        }

        if (s.ok()) {
          DirInfo dir_info;
          dir_info.mtime = my_start;
          dir_info.size = d->size;
          s = mdb_->SetInfo(dir_id, dir_info, mdb_tx);
        }

        if (s.ok()) {
          s = mdb_->Commit(mdb_tx);
        }

        mutex_.Lock();
        uint64_t my_end = env_->NowMicros();
        // May need to wait for lease due if the target is a directory
        if (s.ok() && S_ISDIR(stat->FileMode())) {
          Lease::Ref* lref = leases_->Lookup(dir_id, name_hash);
          if (lref == NULL) {
            Lease* l = new Lease;
            l->state = kFreeState;
            l->parent = d;
            l->due = 0;
            l->seq = 0;
            while (lref == NULL) {
              try {
                lref = leases_->Insert(dir_id, name_hash, l);
              } catch (int err) {
                // Not expecting errors other than "buffer-full"
                assert(err == ENOBUFS);
                // I have to install a new lease so each overlapping read can
                // detect if the version of its value is stale or not.
                // So if the lease table is full, I will just sleep long
                // enough and retry the next time.
                // TODO: a possible alternative is too force injecting a
                // lease entry even when the lease table is full
                lref = NULL;
                mutex_.Unlock();
                env_->SleepForMicroseconds(lease_duration_ + 10);
                mutex_.Lock();
                my_end = env_->NowMicros();
              }
            }
            d->num_leases++;
          }
          assert(lref != NULL);
          Lease::Guard lguard(leases_, lref);
          Lease* const l = lref->value;
          assert(l != NULL && l->state != kWriteState);
          while (l->state == kReadState && l->due > my_end) {
            l->state = kWriteState;
            mutex_.Unlock();
            uint64_t diff = l->due - my_end + 10;
            // Wait past lease due
            env_->SleepForMicroseconds(diff);
            mutex_.Lock();
            my_end = env_->NowMicros();
          }
          assert(l->parent == d);
          d->seq = 1 + d->seq;
          l->seq = d->seq;
          assert(my_end + lease_duration_ >= l->due);
          // TODO: replace the following with dynamic lease durations
          l->due = my_end + lease_duration_;
          l->state = kFreeState;
        }
        if (s.ok()) {
          assert(my_start >= d->mtime);
          d->mtime = my_start;
        }
        assert(d->tx.NoBarrier_Load() == tx);
        d->tx.NoBarrier_Store(NULL);
        assert(tx != NULL);
        bool last_ref = tx->Unref();
        if (!last_ref) {
          tx = NULL;
        }
      }
    }
  }

  ret->stat.AssertAllSet();
  if (tx != NULL) {
    tx->Dispose(mdb_);
  }
  return s;
}

// Fetch the entries under a parent directory. Return OK on success.
// Directory listing does not have to return a serializable view of
// the file system. So all list operations will go without synchronizing
// with other concurrent read or write operations.
//
// Errors are mostly masked so an empty list is returned in worst case.
Status MDS::SRV::Listdir(const ListdirOptions& options, ListdirRet* ret) {
  DirId dir_id(options.reg_id, options.snap_id, options.dir_ino);
  mdb_->List(dir_id, NULL, &ret->names, NULL);
  return Status::OK();
}

}  // namespace pdlfs
