#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <sys/stat.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

int deltafs_open(const char* __path, int __oflags, mode_t __mode, struct stat*);
int deltafs_mkfile(const char* __path, mode_t __mode);
int deltafs_mkdir(const char* __path, mode_t __mode);
int deltafs_chmod(const char* __path, mode_t __mode);
int deltafs_stat(const char* __path, struct stat*);
int deltafs_access(const char* __path, int __mode);
int deltafs_accessdir(const char* __path, int __mode);
int deltafs_unlink(const char* __path);
typedef int (*deltafs_filler_t)(const char* __name, void* __arg);
int deltafs_listdir(const char* __path, deltafs_filler_t, void* __arg);
ssize_t deltafs_pread(int __fd, void* __buf, size_t __sz, off_t __off);
ssize_t deltafs_read(int __fd, void* __buf, size_t __sz);
ssize_t deltafs_pwrite(int __fd, const void* __buf, size_t __sz, off_t __off);
ssize_t deltafs_write(int __fd, const void* __buf, size_t __sz);
int deltafs_fstat(int __fd, struct stat*);
int deltafs_ftruncate(int __fd, off_t __len);
int deltafs_fdatasync(int __fd);
int deltafs_close(int __fd);

#ifdef __cplusplus
}
#endif
