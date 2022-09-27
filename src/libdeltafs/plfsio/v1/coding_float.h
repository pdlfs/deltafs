//
// Created by Ankush J on 11/2/20.
//

#include "pdlfs-common/coding.h"

#pragma once

namespace pdlfs {
inline void EncodeFloat32(char* buf, float value) {
  uint32_t val_uint;
  assert(sizeof(float) == sizeof(uint32_t));
  memcpy(&val_uint, &value, sizeof(uint32_t));
  EncodeFixed32(buf, val_uint);
}

inline float DecodeFloat32(const char* ptr) {
  float ret;
  assert(sizeof(float) == sizeof(uint32_t));
  uint32_t val_uint = DecodeFixed32(ptr);
  memcpy(&ret, &val_uint, sizeof(uint32_t));
  return ret;
}

inline void PutFloat32(std::string* dst, float value) {
  char buf[sizeof(value)];
  EncodeFloat32(buf, value);
  dst->append(buf, sizeof(buf));
}
}  // namespace pdlfs
