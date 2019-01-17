/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <assert.h>

namespace pdlfs {
namespace plfsio {
namespace v2 {

template <typename T>
class List;

template <typename T>
class ListEntry {
 public:
  T* rep_;

 private:
  ~ListEntry() {}

  int refs_;
  ListEntry() : rep_(NULL), refs_(0), p_(NULL), prev_(this), next_(this) {}
  void operator=(const ListEntry& entry);
  ListEntry(const ListEntry&);
  template <typename TT>
  friend class List;
  typedef ListEntry<T> E;
  List<T>* p_;
  E* prev_;
  E* next_;
};

template <typename T>
class List {
 private:
  typedef ListEntry<T> E;

  void operator=(const List& list);
  List(const List&);
  // Dummy list head of doubly linked entries
  E head_;

  void Delete(E* e) {
    assert(e->p_ == this);
    assert(e->refs_ == 0);
    e->prev_->next_ = e->next_;
    e->next_->prev_ = e->prev_;
    delete e;
  }

 public:
  List() {}

  bool empty() const { return (head_.next_ == &head_); }

  E* New(void* rep, int refs = 0) {
    E* e = new E;
    e->rep_ = rep;
    e->p_ = this;
    e->next_ = &head_;
    e->prev_ = head_.prev_;
    e->prev_->next_ = e;
    e->next_->prev_ = e;
    e->refs_ = refs;
    return e;
  }

  T* Unref(E* e) {
    assert(e->refs_ > 0);
    e->refs_--;
    if (e->refs_ == 0) {
      T* rep = e->rep_;
      Delete(e);
      return rep;
    }

    return NULL;
  }
};

}  // namespace v2
}  // namespace plfsio
}  // namespace pdlfs
