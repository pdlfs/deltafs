/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

namespace pdlfs {
namespace plfsio {

enum EventType { kCompactionStart, kCompactionEnd, kIoStart, kIoEnd };

struct CompactionEvent {
  EventType type;  // Event type

  size_t part;  // Memtable partition index

  // Current time micros
  uint64_t micros;
};

struct IoEvent {
  EventType type;  // Event type

  // Current time micros
  uint64_t micros;
};

class EventListener {
 public:
  EventListener() {}
  virtual ~EventListener();

  virtual void OnEvent(EventType type, void* event) = 0;
};

}  // namespace plfsio
}  // namespace pdlfs
