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
#include <vector>

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

// Split a string into a list of substrings using a specified delimiter.
// If max_splits is non-negative, the number of splits won't exceed it.
// Return the size of the resulting array.
extern size_t SplitString(std::vector<std::string>*, const Slice& value,
                          char delim = ';', int max_splits = -1);

// Parse a human-readable text to a long int value.
// Return true if successfully parsed and false otherwise.
extern bool ParsePrettyNumber(const Slice& value, uint64_t* val);

// Parse a human-readable text to a boolean value.
// Return true if successfully parsed and false otherwise.
extern bool ParsePrettyBool(const Slice& value, bool* val);

}  // namespace pdlfs
