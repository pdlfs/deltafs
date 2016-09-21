#pragma once

/*
 * Copyright (c) 2015 The SILT Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "bit_access.h"

namespace pdlfs {
namespace ectrie {

template <unsigned int Order = 0>
class exp_golomb {
 public:
  template <typename T, typename BufferType>
  static void encode(BufferType& out_buf, const T& n) {
    T m;
    if (Order)
      m = (n >> Order) + 1;
    else
      m = n + 1;

    int len = 0;
    {
      T p = m;
      while (p) {
        len++;
        p >>= 1;
      }
    }

    for (int i = 1; i < len; i++) {
      out_buf.push_back(0);
    }

    out_buf.push_back(1);
    out_buf.append(m, static_cast<size_t>(len - 1));

    if (Order) {
      out_buf.append(n, static_cast<size_t>(Order));
    }
  }

  template <typename T, typename BufferType>
  static T decode(const BufferType& in_buf, size_t& in_out_buf_iter) {
    int len = 1;
    while (true) {
      if (in_buf[in_out_buf_iter++]) break;
      len++;
    }

    T m = static_cast<T>(1) << (len - 1);
    // "template" prefix is used to inform the compiler that in_buf.get is a
    // member template
    m |= in_buf.template get<T>(in_out_buf_iter, static_cast<size_t>(len - 1));
    in_out_buf_iter += static_cast<size_t>(len - 1);

    T n;
    if (Order) {
      n = (m - 1) << Order;
      n |= in_buf.template get<T>(in_out_buf_iter, static_cast<size_t>(Order));
      in_out_buf_iter += static_cast<size_t>(Order);
    } else
      n = m - 1;

    return n;
  }
};

}  // namespace ectrie
}  // namespace pdlfs
