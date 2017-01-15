/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <ctype.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>

#include "pdlfs-common/slice.h"
#include "pdlfs-common/strutil.h"

namespace pdlfs {

void AppendSignedNumberTo(std::string* str, int64_t num) {
  char buf[30];
  snprintf(buf, sizeof(buf), "%+lld", (long long)num);
  str->append(buf);
}

void AppendNumberTo(std::string* str, uint64_t num) {
  char buf[30];
  snprintf(buf, sizeof(buf), "%llu", (unsigned long long)num);
  str->append(buf);
}

void AppendEscapedStringTo(std::string* str, const Slice& value) {
  for (size_t i = 0; i < value.size(); i++) {
    char c = value[i];
    if (c >= ' ' && c <= '~') {
      str->push_back(c);
    } else {
      char buf[10];
      snprintf(buf, sizeof(buf), "\\x%02x",
               static_cast<unsigned int>(c) & 0xff);
      str->append(buf);
    }
  }
}

std::string NumberToString(uint64_t num) {
  std::string r;
  AppendNumberTo(&r, num);
  return r;
}

std::string EscapeString(const Slice& value) {
  std::string r;
  AppendEscapedStringTo(&r, value);
  return r;
}

bool ConsumeDecimalNumber(Slice* in, uint64_t* val) {
  uint64_t v = 0;
  int digits = 0;
  while (!in->empty()) {
    char c = (*in)[0];
    if (c >= '0' && c <= '9') {
      ++digits;
      const uint64_t delta = (c - '0');
      static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0);
      if (v > kMaxUint64 / 10 ||
          (v == kMaxUint64 / 10 && delta > kMaxUint64 % 10)) {
        // Overflow
        return false;
      }
      v = (v * 10) + delta;
      in->remove_prefix(1);
    } else {
      break;
    }
  }
  *val = v;
  return (digits > 0);
}

bool ParsePrettyBool(const Slice& value, bool* val) {
  if (value == "y" || value.starts_with("yes") || value.starts_with("true") ||
      value.starts_with("enable")) {
    *val = true;
    return true;
  } else if (value == "n" || value.starts_with("no") ||
             value.starts_with("false") || value.starts_with("disable")) {
    *val = false;
    return true;
  } else {
    return false;
  }
}

bool ParsePrettyNumber(const Slice& value, uint64_t* val) {
  Slice input = value;
  uint64_t base;
  if (!ConsumeDecimalNumber(&input, &base)) {
    return false;
  } else {
    if (input.empty()) {
      *val = base;
      return true;
    } else if (input.starts_with("k")) {
      *val = base * 1024;
      return true;
    } else if (input.starts_with("m")) {
      *val = base * 1024 * 1024;
      return true;
    } else if (input.starts_with("g")) {
      *val = base * 1024 * 1024 * 1024;
      return true;
    } else {
      return false;
    }
  }
}

static Slice Trim(const Slice& v) {
  Slice input = v;
  while (!input.empty()) {
    if (isspace(input[0])) {
      input.remove_prefix(1);
    } else if (isspace(input[-1])) {
      input.remove_suffix(1);
    } else {
      break;
    }
  }
  return input;
}

size_t SplitString(std::vector<std::string>* v, const Slice& value, char delim,
                   int max_splits) {
  size_t count = 0;  // Number of resulting substrings
  int splits = 0;    // Number of split operations
  Slice input = value;
  while (!input.empty() && (max_splits < 0 || splits < max_splits)) {
    const char* start = input.data();
    const char* limit = strchr(start, delim);
    if (limit != NULL) {
      input.remove_prefix(limit - start + 1);
      if (limit - start != 0) {
        Slice sub = Trim(Slice(start, limit - start));
        if (!sub.empty()) {
          v->push_back(sub.ToString());
          count++;
        }
      }
      splits++;
    } else {
      break;
    }
  }
  Slice sub = Trim(input);
  if (!sub.empty()) {
    v->push_back(sub.ToString());
    count++;
  }
  return count;
}

std::string PrettySize(uint64_t input) {
  char p[100];
  const unsigned long long n = static_cast<unsigned long long>(input);
  if (n >= 1024LLU * 1024LLU * 1024LLU) {
    snprintf(p, sizeof(p), "%lluGB (%llu Bytes)", n / 1024 / 1024 / 1024, n);
  } else if (n >= 1024LLU * 1024LLU) {
    snprintf(p, sizeof(p), "%lluMB (%llu Bytes)", n / 1024 / 1024, n);
  } else if (n >= 1024LLU) {
    snprintf(p, sizeof(p), "%lluK (%llu Bytes)", n / 1024, n);
  } else {
    snprintf(p, sizeof(p), "%llu Bytes", n);
  }

  return p;
}

}  // namespace pdlfs
