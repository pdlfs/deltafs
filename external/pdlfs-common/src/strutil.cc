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

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */
#include "pdlfs-common/strutil.h"
#include "pdlfs-common/slice.h"

#include <ctype.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>

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
      const uint64_t delta = static_cast<unsigned>(c - '0');
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

static bool IsLiteralFalse(const Slice& input) {
  const char* li[] = {"0",        "OFF",  "NO",   "FALSE", "IGNORED",
                      "DISABLED", "SKIP", "NONE", "N"};
  for (size_t i = 0; i < sizeof(li) / sizeof(void*); i++) {
    if (input == li[i]) {
      return true;
    }
  }
  return false;
}

static bool IsLiteralTrue(const Slice& input) {
  const char* li[] = {"1", "ON", "YES", "TRUE", "ENABLED", "Y"};
  for (size_t i = 0; i < sizeof(li) / sizeof(void*); i++) {
    if (input == li[i]) {
      return true;
    }
  }
  return false;
}

bool ParsePrettyBool(const Slice& value, bool* val) {
  std::string input = value.ToString();
  for (size_t i = 0; i < input.size(); i++) {
    input[i] = static_cast<char>(toupper(input[i]));
  }
  if (IsLiteralTrue(input)) {
    *val = true;
    return true;
  } else if (IsLiteralFalse(input)) {
    *val = false;
    return true;
  } else {
    return false;
  }
}

bool ParsePrettyNumber(const Slice& value, uint64_t* result) {
  Slice input = value;
  uint64_t base;
  if (!ConsumeDecimalNumber(&input, &base)) {
    return false;
  } else {
    if (input.empty()) {
      *result = base;
      return true;
    } else if (input[0] == 'K' || input[0] == 'k') {
      *result = base << 10;
      return true;
    } else if (input[0] == 'M' || input[0] == 'm') {
      *result = base << 20;
      return true;
    } else if (input[0] == 'G' || input[0] == 'g') {
      *result = base << 30;
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

size_t SplitString(std::vector<std::string>* v, const char* str, char delim,
                   int max_splits) {
  size_t count = 0;  // Number of resulting substrings
  int splits = 0;    // Number of split operations
  Slice input = str;
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
  char tmp[100];
  const unsigned long long n = static_cast<unsigned long long>(input);
  if (n >= 1024LLU * 1024LLU * 1024LLU) {
    double m = double(n) / 1024 / 1024 / 1024;
    snprintf(tmp, sizeof(tmp), "%.1f GiB (%llu Bytes)", m, n);
  } else if (n >= 1024LLU * 1024LLU) {
    double m = double(n) / 1024 / 1024;
    snprintf(tmp, sizeof(tmp), "%.1f MiB (%llu Bytes)", m, n);
  } else if (n >= 1024LLU) {
    double m = double(n) / 1024;
    snprintf(tmp, sizeof(tmp), "%.1f KiB (%llu Bytes)", m, n);
  } else {
    snprintf(tmp, sizeof(tmp), "%llu Bytes", n);
  }

  return tmp;
}

}  // namespace pdlfs
