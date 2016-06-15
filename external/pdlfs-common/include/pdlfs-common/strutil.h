#pragma once

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdint.h>
#include <string>

namespace pdlfs {

class Slice;

// Append a human-readable printout of "num" to *str
extern void AppendSignedNumberTo(std::string* str,
                                 int64_t num);               // Signed number
extern void AppendNumberTo(std::string* str, uint64_t num);  // Unsigned number

// Append a human-readable printout of "value" to *str.
// Escapes any non-printable characters found in "value".
extern void AppendEscapedStringTo(std::string* str, const Slice& value);

// Return a human-readable printout of "num"
extern std::string NumberToString(uint64_t num);

// Return a human-readable version of "value".
// Escapes any non-printable characters found in "value".
extern std::string EscapeString(const Slice& value);

// Parse a human-readable number from "*in" into *value.  On success,
// advances "*in" past the consumed number and sets "*val" to the
// numeric value.  Otherwise, returns false and leaves *in in an
// unspecified state.
extern bool ConsumeDecimalNumber(Slice* in, uint64_t* val);

}  // namespace pdlfs
