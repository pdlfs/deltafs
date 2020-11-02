//
// Created by Ankush J on 11/2/20.
//

#include "pdlfs-common/coding.h"

#pragma once

namespace pdlfs {
inline void EncodeFloat32(char* buf, float value) {
  assert(sizeof(float) == sizeof(uint32_t));
  uint32_t val_uint = *reinterpret_cast<uint32_t*>(&value);
  EncodeFixed32(buf, val_uint);
}

inline float DecodeFloat32(const char* ptr) {
  assert(sizeof(float) == sizeof(uint32_t));
  uint32_t val_uint = DecodeFixed32(ptr);
  return *reinterpret_cast<float*>(&val_uint);
}

inline void PutFloat32(std::string* dst, float value) {
  char buf[sizeof(value)];
  EncodeFloat32(buf, value);
  dst->append(buf, sizeof(buf));
}
}  // namespace pdlfs