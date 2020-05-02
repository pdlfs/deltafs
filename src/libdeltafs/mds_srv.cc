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

#include "mds_srv.h"

#include "pdlfs-common/dirlock.h"
#include "pdlfs-common/mutexlock.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace pdlfs {

// NOTE: can be called while mutex_ is NOT locked.
Status MDS::SRV::LoadDir(const DirId& id, DirInfo* info, DirIndex* index) {
  Status s;
  MDB::Tx* mdb_tx = NULL;

  // Load directory info. Create if missing...
  s = mdb_->GetInfo(id, info, mdb_tx);
  if (s.IsNotFound()) {
    mdb_tx = mdb_->CreateTx();
    info->mtime = CurrentMicros();
    info->size = 0;
    s = mdb_->SetInfo(id, *info, mdb_tx);
  }

  // Load directory index. Create if missing...
  if (s.ok()) {
    s = mdb_->GetDirIdx(id, index, mdb_tx);
    if (s.IsNotFound()) {
      int zserver = PickupServer(id) % giga_.num_virtual_servers;
      DirIndex tmp(zserver, &giga_);
      tmp.SetAll();  // Pre-split to all servers
      if (mdb_tx == NULL) {
        mdb_tx = mdb_->CreateTx();
      }
      s = mdb_->SetDirIdx(id, tmp, mdb_tx);
      if (s.ok()) {
        index->Swap(tmp);
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
        loading_dirs_.Insert(id_encoding);
        mutex_.Unlock();
        DirInfo dir_info;
        DirIndex dir_index(&giga_);
        s = LoadDir(id, &dir_info, &dir_index);
        mutex_.Lock();
        if (s.ok()) {
          Dir* d = new Dir(&mutex_, &giga_);
          d->mtime = dir_info.mtime;
          assert(dir_info.size >= 0);
          d->size = dir_info.size;
          d->num_leases = 0;
          d->index.Swap(dir_index);
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

// Quickly check background status. Return OK on success.
// Return a non-OK status when the directory (or the server as a whole)
// contains errors and must be fenced from online operations.
// REQUIRES: mutex_ has been locked.
Status MDS::SRV::ProbeDir(const Dir* d) {
  mutex_.AssertHeld();
  if (!status_.ok()) {
    return status_;
  } else if (!d->status.ok()) {
    return d->status;
  } else {
    return Status::OK();
  }
}

// REQUIRES: mutex_ has been locked.
uint64_t MDS::SRV::NextIno() {
  mutex_.AssertHeld();
  uint64_t result = ++ino_;
  if (paranoid_checks_) {
    assert(srv_id_ >= 0);
    uint64_t limit = srv_id_ + 1;
    limit <<= 32;
    if (result + 1 >= limit) {
      status_ = Status::BufferFull("No more free inodes");
    }
  }
  return result;
}

// REQUIRES: mutex_ has been locked.
void MDS::SRV::TryReuseIno(uint64_t ino) {
  mutex_.AssertHeld();
  if (ino == ino_) {
    --ino_;
  }
}

// REQUIRES: mutex_ has been locked.
uint32_t MDS::SRV::NextSession() {
  mutex_.AssertHeld();
  session_ += giga_.num_servers;
  return session_;
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
  const DirId& dir_id = options.dir_id;
  const Slice& name_hash = options.name_hash;
  if (name_hash.empty()) {
    s = Status::InvalidArgument("empty name hash");
  } else if (paranoid_checks_ && !options.name.empty()) {
    std::string tmp;
    DirIndex::PutHash(&tmp, options.name);
    if (name_hash.compare(tmp) != 0) {
      s = Status::InvalidArgument("name and hash don't match");
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

        std::string name;
        s = mdb_->GetNode(dir_id, name_hash, &ret->stat, &name, mdb_tx);

        if (s.ok() && paranoid_checks_) {
          std::string tmp;
          DirIndex::PutHash(&tmp, name);
          if (name_hash.compare(tmp) != 0) {
            s = Status::Corruption("name and hash don't match");
          }

          Error(__LOG_ARGS__, "%s/%s: %s", options.dir_id.DebugString().c_str(),
                name.c_str(), s.ToString().c_str());
        }

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

  if (s.ok()) {
    ret->stat.AssertAllSet();
  }
  if (tx != NULL) {
    tx->Dispose(mdb_);
  }
  return s;
}

// Insert a new file into a parent directory. Return OK on success.
// Updates generated by this operation are not guaranteed to reach disk. Must
// do a db sync to ensure durability.
//
// Errors may occur when the name to be created already exists and the
// operation demands exclusiveness, when the operation doesn't ask
// exclusiveness and the name exists but it points to a directory,
// when the current server is not the right one for the specified
// name, when data would not go into the db, and when other
// internal or external errors occur...
//
// Write operations against the same parent directory must be serialized
// so they always proceed one after another. Write operations
// should not block any concurrent read operations.
Status MDS::SRV::Fcreat(const FcreatOptions& options, FcreatRet* ret) {
  Status s;
  Dir::Tx* tx = NULL;
  Dir::Ref* ref;
  const DirId& dir_id = options.dir_id;
  const Slice& name_hash = options.name_hash;
  if (name_hash.empty() || options.name.empty()) {
    s = Status::InvalidArgument("empty name and hash");
  } else if (paranoid_checks_) {
    std::string tmp;
    DirIndex::PutHash(&tmp, options.name);
    if (name_hash.compare(tmp) != 0) {
      s = Status::InvalidArgument("name and hash don't match");
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
        bool entry_exists = false;
        uint64_t my_time = CurrentMicros();
        uint64_t my_ino = NextIno();
        mutex_.Unlock();

        tx = new Dir::Tx(mdb_);
        tx->Ref();
        assert(d->tx.Acquire_Load() == NULL);
        d->tx.Release_Store(tx);
        MDB::Tx* mdb_tx = tx->rep();

        Stat* stat = &ret->stat;
        std::string name;
        s = mdb_->GetNode(dir_id, name_hash, stat, &name, mdb_tx);

        if (s.ok() && paranoid_checks_) {
          std::string tmp;
          DirIndex::PutHash(&tmp, name);
          if (name_hash.compare(tmp) != 0) {
            s = Status::Corruption("name and hash don't match");

            Error(__LOG_ARGS__, "%s/%s: %s",
                  options.dir_id.DebugString().c_str(), name.c_str(),
                  s.ToString().c_str());
          }
        }

        if (s.ok()) {
          if ((options.flags & O_EXCL) == O_EXCL) {
            s = Status::AlreadyExists(Slice());
          } else if (!S_ISREG(stat->FileMode())) {
            s = Status::FileExpected(Slice());
          }

          entry_exists = true;
        } else if (s.IsNotFound()) {
          uint32_t mode = S_IFREG | (options.mode & ACCESSPERMS);
          stat->SetRegId(reg_id_);
          stat->SetSnapId(snap_id_);
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

        ret->created = !entry_exists;

        if (!entry_exists) {
          if (s.ok()) {
            DirInfo dir_info;
            dir_info.mtime = my_time;
            dir_info.size = 1 + d->size;
            s = mdb_->SetInfo(dir_id, dir_info, mdb_tx);
          }

          if (s.ok()) {
            s = mdb_->Commit(mdb_tx);
          }
        }

        mutex_.Lock();
        if (s.ok() && !entry_exists) {
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

  if (s.ok()) {
    ret->stat.AssertAllSet();
  }
  if (tx != NULL) {
    tx->Dispose(mdb_);
  }
  return s;
}

// Remove an existing file from a parent directory. Return OK on success.
// Updates generated by this operations are not guaranteed to reach disk. Must
// do a db sync to ensure durability.
//
// Errors may occur when the name to be deleted does not exist, when
// the name points to a directory, when the current server is not
// the right one for the specific name, when data would not go
// to the db, and when other internal or external errors occur...
//
// Write operations against the same parent directory must be serialized
// so they always proceed one after another. Write operations
// should not block any concurrent read operations.
Status MDS::SRV::Unlink(const UnlinkOptions& options, UnlinkRet* ret) {
  Status s;
  Dir::Tx* tx = NULL;
  Dir::Ref* ref;
  const DirId& dir_id = options.dir_id;
  const Slice& name_hash = options.name_hash;
  if (name_hash.empty()) {
    s = Status::InvalidArgument("empty name hash");
  } else if (paranoid_checks_ && !options.name.empty()) {
    std::string tmp;
    DirIndex::PutHash(&tmp, options.name);
    if (name_hash.compare(tmp) != 0) {
      s = Status::InvalidArgument("name and hash don't match");
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
        bool entry_exists = false;
        uint64_t my_time = CurrentMicros();
        mutex_.Unlock();

        tx = new Dir::Tx(mdb_);
        tx->Ref();
        assert(d->tx.Acquire_Load() == NULL);
        d->tx.Release_Store(tx);
        MDB::Tx* mdb_tx = tx->rep();

        Stat* stat = &ret->stat;
        std::string name;
        s = mdb_->GetNode(dir_id, name_hash, stat, &name, mdb_tx);

        if (s.ok() && paranoid_checks_) {
          std::string tmp;
          DirIndex::PutHash(&tmp, name);
          if (name_hash.compare(tmp) != 0) {
            s = Status::Corruption("name and hash don't match");
          }

          Error(__LOG_ARGS__, "%s/%s: %s", options.dir_id.DebugString().c_str(),
                name.c_str(), s.ToString().c_str());
        }

        if (s.ok()) {
          if (!S_ISREG(stat->FileMode())) {
            s = Status::FileExpected(Slice());
          } else {
            s = mdb_->DelNode(dir_id, name_hash, mdb_tx);
          }

          entry_exists = true;
        } else if (s.IsNotFound()) {
          if ((options.flags & O_EXCL) != O_EXCL) {
            stat->SetRegId(0);
            stat->SetSnapId(0);
            stat->SetInodeNo(0);
            stat->SetFileSize(0);
            stat->SetFileMode(0);
            stat->SetZerothServer(0);
            stat->SetUserId(0);
            stat->SetGroupId(0);
            stat->SetModifyTime(0);
            stat->SetChangeTime(0);
            s = Status::OK();
          }
        }

        if (entry_exists) {
          if (s.ok()) {
            DirInfo dir_info;
            dir_info.mtime = my_time;
            dir_info.size = d->size - 1;
            s = mdb_->SetInfo(dir_id, dir_info, mdb_tx);
          }

          if (s.ok()) {
            s = mdb_->Commit(mdb_tx);
          }
        }

        mutex_.Lock();
        if (s.ok() && entry_exists) {
          assert(d->size > 1);
          d->size = -1 + d->size;
          assert(my_time >= d->mtime);
          d->mtime = my_time;
        }
        assert(d->tx.NoBarrier_Load() == tx);
        d->tx.NoBarrier_Store(NULL);
        assert(tx != NULL);
        bool last_ref = tx->Unref();
        if (!last_ref) {
          tx = NULL;  // To be disposed by the last reader
        }
      }
    }
  }

  if (s.ok()) {
    ret->stat.AssertAllSet();
  }
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
  const DirId& dir_id = options.dir_id;
  const Slice& name_hash = options.name_hash;
  if (name_hash.empty() || options.name.empty()) {
    s = Status::InvalidArgument("empty name and hash");
  } else if (paranoid_checks_) {
    std::string tmp;
    DirIndex::PutHash(&tmp, options.name);
    if (name_hash.compare(tmp) != 0) {
      s = Status::InvalidArgument("name and hash don't match");
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
        bool entry_exists = false;
        uint64_t my_time = CurrentMicros();
        uint64_t my_ino = NextIno();
        DirId my_id(reg_id_, snap_id_, my_ino);
        mutex_.Unlock();

        tx = new Dir::Tx(mdb_);
        tx->Ref();
        assert(d->tx.Acquire_Load() == NULL);
        d->tx.Release_Store(tx);
        MDB::Tx* mdb_tx = tx->rep();

        Stat* stat = &ret->stat;
        std::string name;
        s = mdb_->GetNode(dir_id, name_hash, stat, &name, mdb_tx);

        if (s.ok() && paranoid_checks_) {
          std::string tmp;
          DirIndex::PutHash(&tmp, name);
          if (name_hash.compare(tmp) != 0) {
            s = Status::Corruption("name and hash don't match");

            Error(__LOG_ARGS__, "%s/%s: %s",
                  options.dir_id.DebugString().c_str(), name.c_str(),
                  s.ToString().c_str());
          }
        }

        if (s.ok()) {
          if ((options.flags & O_EXCL) == O_EXCL) {
            s = Status::AlreadyExists(Slice());
          } else if (!S_ISDIR(stat->FileMode())) {
            s = Status::DirExpected(Slice());
          }

          entry_exists = true;
        } else if (s.IsNotFound()) {
          uint32_t mode = S_IFDIR;
          mode |= (options.mode & ACCESSPERMS);
          mode |= (options.mode & DELTAFS_DIR_MASK);
          int rserver = PickupServer(my_id);
          int zserver = rserver % giga_.num_virtual_servers;
          stat->SetRegId(reg_id_);
          stat->SetSnapId(snap_id_);
          stat->SetInodeNo(my_ino);
          stat->SetFileSize(0);
          stat->SetFileMode(mode);
          stat->SetUserId(options.uid);
          stat->SetGroupId(options.gid);
          stat->SetZerothServer(zserver);
          stat->SetModifyTime(my_time);
          stat->SetChangeTime(my_time);
          s = mdb_->SetNode(dir_id, name_hash, *stat, options.name, mdb_tx);
        }

        if (!entry_exists) {
          if (s.ok()) {
            DirInfo dir_info;
            dir_info.mtime = my_time;
            dir_info.size = 1 + d->size;
            s = mdb_->SetInfo(dir_id, dir_info, mdb_tx);
          }

          if (s.ok()) {
            s = mdb_->Commit(mdb_tx);
          }
        }

        mutex_.Lock();
        if (s.ok() && !entry_exists) {
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

  if (s.ok()) {
    ret->stat.AssertAllSet();
  }
  if (tx != NULL) {
    tx->Dispose(mdb_);
  }
  return s;
}

// Update file last access and modification times. Return OK on success.
// Current implementation does not store last access time so only
// the last modification time is actually changed.
//
// Errors may occur when the file or directory targeted does not exist,
// when the current server is not the right one to execute the call,
// when the data read from the DB is corrupted, and when other
// internal or external errors occur...
Status MDS::SRV::Utime(const UtimeOptions& options, UtimeRet* ret) {
  Status s;
  Dir::Tx* tx = NULL;
  Dir::Ref* ref;
  const DirId& dir_id = options.dir_id;
  const Slice& name_hash = options.name_hash;
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
        uint64_t my_time = CurrentMicros();
        mutex_.Unlock();

        tx = new Dir::Tx(mdb_);
        tx->Ref();
        assert(d->tx.Acquire_Load() == NULL);
        d->tx.Release_Store(tx);
        MDB::Tx* mdb_tx = tx->rep();

        std::string name;
        Stat* stat = &ret->stat;
        s = mdb_->GetNode(dir_id, name_hash, stat, &name, mdb_tx);
        // TODO: paranoid checks
        if (s.ok()) {
          stat->SetModifyTime(options.mtime);
          stat->SetChangeTime(my_time);
          s = mdb_->SetNode(dir_id, name_hash, *stat, name, mdb_tx);
        }

        if (s.ok()) {
          s = mdb_->Commit(mdb_tx);
        }

        mutex_.Lock();
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

  if (s.ok()) {
    ret->stat.AssertAllSet();
  }
  if (tx != NULL) {
    tx->Dispose(mdb_);
  }
  return s;
}

// Shrink or extend the size of a file to a specified value. Return OK
// on success. Cannot operate on directories. Also update the last
// modification time of the file.
//
// Errors may occur when the file targeted does not exist or is not a
// regular file, when the current server is not the right one to
// execute the call, when the data read from DB is corrupted,
// and when other internal or external error occur...
Status MDS::SRV::Trunc(const TruncOptions& options, TruncRet* ret) {
  Status s;
  Dir::Tx* tx = NULL;
  Dir::Ref* ref;
  const DirId& dir_id = options.dir_id;
  const Slice& name_hash = options.name_hash;
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
        uint64_t my_time = CurrentMicros();
        mutex_.Unlock();

        tx = new Dir::Tx(mdb_);
        tx->Ref();
        assert(d->tx.Acquire_Load() == NULL);
        d->tx.Release_Store(tx);
        MDB::Tx* mdb_tx = tx->rep();

        std::string name;
        Stat* stat = &ret->stat;
        s = mdb_->GetNode(dir_id, name_hash, stat, &name, mdb_tx);
        // TODO: paranoid checks
        if (s.ok()) {
          if (!S_ISREG(stat->FileMode())) {
            s = Status::FileExpected(Slice());
          } else {
            stat->SetFileSize(options.size);
            stat->SetModifyTime(options.mtime);
            stat->SetChangeTime(my_time);
            s = mdb_->SetNode(dir_id, name_hash, *stat, name, mdb_tx);
          }
        }

        if (s.ok()) {
          s = mdb_->Commit(mdb_tx);
        }

        mutex_.Lock();
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

  if (s.ok()) {
    ret->stat.AssertAllSet();
  }
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
//   when the operation commits. In such cases, the lookup operation will
//   not issue a lease.
//
//   In order for lookup operations to always detect stale data:
//       i) each lookup state mutation operation must
//          install an active lease for the new version
//          of the data it created; and
//      ii) all lookup operation must finish within
//          the shortest possible lease duration so
//          it is guaranteed to see the lease
//          created by an overlapping lookup
//          state mutation operation
//
//   Note that a lease is considered active if it is locked
//             or its due time has not yet past.
//
//   Active leases won't get evicted from the lease table.
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
  const DirId& dir_id = options.dir_id;
  const Slice& name_hash = options.name_hash;
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
        uint64_t my_start = CurrentMicros();
        uint64_t my_seq = d->seq;
        mutex_.Unlock();

        MDB::Tx* mdb_tx = NULL;
        tx = reinterpret_cast<Dir::Tx*>(d->tx.Acquire_Load());
        if (tx != NULL) {
          mdb_tx = tx->rep();
          tx->Ref();
        }

        ret->stat.SetLeaseDue(0);

        std::string name;
        Stat stat;
        s = mdb_->GetNode(dir_id, name_hash, &stat, &name, mdb_tx);
        // TODO: paranoid checks
        if (s.ok()) {
          if (!S_ISDIR(stat.FileMode())) {
            s = Status::DirExpected(Slice());
          }
        }

        if (s.ok()) {
          ret->stat.CopyFrom(stat);
        }

        mutex_.Lock();
        uint64_t my_end = CurrentMicros();
        // No lease either we timeout or have a negative result, otherwise...
        if (s.ok() && (my_end - my_start) < (lease_duration_ - 10)) {
          Lease::Ref* lref = leases_->Lookup(dir_id, name_hash);
          if (lref == NULL) {
            Lease* new_lease = new Lease;
            new_lease->state = kLeaseFree;
            new_lease->parent = d;
            new_lease->due = 0;
            new_lease->seq = 0;
            try {
              lref = leases_->Insert(dir_id, name_hash, new_lease);
            } catch (int err) {
              // Not expecting errors other than ENOBUFS
              assert(err == ENOBUFS);
              // If the lease table is full, will return with no lease
              // and the client does not have to know this error
              lref = NULL;
            }
            if (lref != NULL) {
              d->num_leases++;
            } else {
              delete new_lease;
            }
          }
          // No lease will be issued if the lease table is full, otherwise...
          if (lref != NULL) {
            Lease::Guard lguard(leases_, lref);
            Lease* const lease = lref->value;
            assert(lease != NULL);
            // No lease if the data is possibly stale, otherwise...
            if (lease->seq <= my_seq) {
              if (lease->state != kLeaseLocked) {
                lease->state = kLeaseShared;
                assert(my_end + lease_duration_ >= lease->due);
                // TODO: implement dynamic lease duration
                lease->due = my_end + lease_duration_;
              } else {
                // A concurrent write operation is in-progress and not
                // able to extend the lease nor change its state
              }
              ret->stat.SetLeaseDue(lease->due);
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

  if (s.ok()) {
    ret->stat.AssertAllSet();
  }
  if (tx != NULL) {
    tx->Dispose(mdb_);
  }
  return s;
}

// Change the permission of a given file or directory. Return OK on success.
// Write operations within a single parent directory are executed
// sequentially. No write operation should block concurrent read operations.
//
// The data generated by this operation is not guaranteed to reach the disk.
// Must do a db sync to ensure durability.
//
// If the target object is a directory, this operation is subject to
// lease control. If there is a lease for the lookup state of the directory,
// the permission changes can only be applied after that lease expires.
// Also, a new lease must be installed before the operation commits.
//
// Errors may occur when the file or directory targeted does not exist,
// when the current server is not the right one for the target, when
// data read from db is corrupted or new data would not go into the db,
// and when other internal or external errors occur...
Status MDS::SRV::Uperm(const UpermOptions& options, UpermRet* ret) {
  Status s;
  Dir::Tx* tx = NULL;
  Dir::Ref* ref;
  const DirId& dir_id = options.dir_id;
  const Slice& name_hash = options.name_hash;
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
        uint64_t my_start = CurrentMicros();
        mutex_.Unlock();

        tx = new Dir::Tx(mdb_);
        tx->Ref();
        assert(d->tx.Acquire_Load() == NULL);
        d->tx.Release_Store(tx);
        MDB::Tx* mdb_tx = tx->rep();

        Stat* stat = &ret->stat;
        std::string name;
        s = mdb_->GetNode(dir_id, name_hash, stat, &name, mdb_tx);

        if (s.ok() && paranoid_checks_) {
          std::string tmp;
          DirIndex::PutHash(&tmp, name);
          if (name_hash.compare(tmp) != 0) {
            s = Status::Corruption("name and hash don't match");
          }

          Error(__LOG_ARGS__, "%s/%s: %s", options.dir_id.DebugString().c_str(),
                name.c_str(), s.ToString().c_str());
        }

        if (s.ok()) {
          if (options.mode != DELTAFS_NON_MOD) {
            const uint32_t non_access = (~ACCESSPERMS) & stat->FileMode();
            stat->SetFileMode(non_access | (ACCESSPERMS & options.mode));
          }
          if (options.uid != DELTAFS_NON_UID) {
            stat->SetUserId(options.uid);
          }
          if (options.gid != DELTAFS_NON_GID) {
            stat->SetGroupId(options.gid);
          }
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
        uint64_t my_end = CurrentMicros();
        // Wait until lease expiration if the target is a directory
        if (s.ok() && S_ISDIR(stat->FileMode())) {
          Lease::Ref* lease_ref = leases_->Lookup(dir_id, name_hash);
          if (lease_ref == NULL) {
            Lease* new_lease = new Lease;
            new_lease->state = kLeaseFree;
            new_lease->parent = d;
            new_lease->due = 0;
            new_lease->seq = 0;
            while (lease_ref == NULL) {
              try {
                lease_ref = leases_->Insert(dir_id, name_hash, new_lease);
              } catch (int err) {
                // Not expecting errors other than ENOBUFS
                assert(err == ENOBUFS);
                // Has to install a new lease so each overlapping read can
                // detect if the version of its value is stale or not.
                // If the lease table is full, will just sleep long enough and
                // retry the next time.
                // TODO: a possible alternative is too force injecting a
                // lease entry even when the lease table is full
                lease_ref = NULL;
                mutex_.Unlock();
                SleepForMicroseconds(lease_duration_ + 10);
                mutex_.Lock();
                my_end = CurrentMicros();
              }
            }
            d->num_leases++;
          }
          assert(lease_ref != NULL);
          Lease::Guard lguard(leases_, lease_ref);
          Lease* const lease = lease_ref->value;
          assert(lease != NULL && lease->state != kLeaseLocked);
          while (lease->state == kLeaseShared && lease->due > my_end) {
            lease->state = kLeaseLocked;
            uint64_t diff = lease->due - my_end + 10;
            mutex_.Unlock();
            // Wait past lease due
            SleepForMicroseconds(diff);
            mutex_.Lock();
            my_end = CurrentMicros();
          }
          assert(lease->parent == d);
          d->seq = 1 + d->seq;
          lease->seq = d->seq;
          assert(my_end + lease_duration_ >= lease->due);
          lease->due = my_end + lease_duration_;
          lease->state = kLeaseFree;
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

  if (s.ok()) {
    ret->stat.AssertAllSet();
  }
  if (tx != NULL) {
    tx->Dispose(mdb_);
  }
  return s;
}

// Fetch the entries under a parent directory. Return OK on success.
// Directory listing does not have to return a serializable view of
// the file system. So all list operations will go without synchronizing
// with other concurrent read or write operations.
// Errors are mostly masked so an empty list is returned in worst case.
Status MDS::SRV::Listdir(const ListdirOptions& options, ListdirRet* ret) {
  static const size_t kSizeLimit = 1000;
  mdb_->List(options.dir_id, NULL, ret->names, NULL, kSizeLimit);
  return Status::OK();
}

// Return the index encoding of a parent directory. Return OK on success
Status MDS::SRV::Readidx(const ReadidxOptions& options, ReadidxRet* ret) {
  Status s;
  Dir::Ref* ref;
  MutexLock ml(&mutex_);
  s = FetchDir(options.dir_id, &ref);
  if (s.ok()) {
    assert(ref != NULL);
    Dir::Guard guard(dirs_, ref);
    const Dir* const d = ref->value;
    assert(d != NULL);
    s = ProbeDir(d);
    if (s.ok()) {
      Slice encoding = d->index.Encode();
      ret->idx.assign(encoding.data(), encoding.size());
    }
  }

  return s;
}

// Assign an unique session id to a connecting client. Also informs the
// client of the env we are running on so the client knowns where
// to access file data and file system metadata.
// Return OK on success.
Status MDS::SRV::Opensession(const OpensessionOptions&, OpensessionRet* ret) {
  Status s;
  ret->env_name = mds_env_->env_name;
  ret->env_conf = mds_env_->env_conf;
  ret->fio_name = mds_env_->fio_name;
  ret->fio_conf = mds_env_->fio_conf;
  mutex_.Lock();
  ret->session_id = NextSession();
  mutex_.Unlock();
  return s;
}

// Obtain location info on input delta sets
// Return OK on success.
Status MDS::SRV::Getinput(const GetinputOptions&, GetinputRet* ret) {
  ret->info = mds_env_->input_conf;
  return Status::OK();
}

// Obtain location info on output deltas.
// Return OK on success.
Status MDS::SRV::Getoutput(const GetoutputOptions&, GetoutputRet* ret) {
  ret->info = mds_env_->output_conf;
  return Status::OK();
}

}  // namespace pdlfs
