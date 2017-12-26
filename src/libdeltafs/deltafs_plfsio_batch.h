/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/slice.h"
#include "pdlfs-common/status.h"

#include <stdint.h>

namespace pdlfs {
namespace plfsio {

// A cursor yields a sequence of write entries from an input batch. The
// following class defines the interface. Multiple threads can invoke const
// methods on a cursor without external synchronization, but if any of the
// threads may call a non-const method, all threads accessing the same cursor
// must use external synchronization.
class BatchCursor {
 public:
  BatchCursor() {}
  virtual ~BatchCursor();

  // A cursor is either positioned at a write entry, or not valid. This
  // method returns true iff the cursor is valid.
  virtual bool Valid() const = 0;

  // Position at the write entry at the target memory offset. This cursor is
  // Valid() after this call iff the specified memory offset is legit.
  virtual void Seek(uint32_t offset) = 0;

  // Moves to the next entry in the batch. After this call, Valid() is true iff
  // the cursor was not positioned at the last entry in the batch.
  // REQUIRES: Valid()
  virtual void Next() = 0;

  // If an error has occurred, return it. Otherwise, return OK.
  virtual Status status() const = 0;

  // Return the fid for the current entry. The underlying storage for
  // the returned slice is valid only until the next modification of
  // the cursor.
  // REQUIRES: Valid()
  virtual Slice fid() const = 0;

  // Return the data for the current entry. The underlying storage for
  // the returned slice is valid only until the next modification of
  // the cursor.
  // REQUIRES: Valid()
  virtual Slice data() const = 0;

  // Return the memory offset for the current entry.
  // REQUIRES: Valid()
  virtual uint32_t offset() const = 0;

 private:
  // No copying allowed
  void operator=(const BatchCursor&);
  BatchCursor(const BatchCursor&);
};

}  // namespace plfsio
}  // namespace pdlfs
