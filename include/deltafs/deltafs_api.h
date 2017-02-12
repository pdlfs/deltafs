#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>

// Used as a mode to create a special type of directories where all
// I/O operations to files beneath these directories will be performed
// in a parallel log-structured manner that resembles plfs.
#define DELTAFS_DIR_PLFS_STYLE 0x10000
#define DELTAFS_DIR_MASK 0xf0000

#define DELTAFS_DIR_IS_PLFS_STYLE(mode) \
  ((mode & DELTAFS_DIR_MASK) == DELTAFS_DIR_PLFS_STYLE)

#ifdef __cplusplus
extern "C" {
#endif

// ---------------------
// main file system api
// ---------------------

void deltafs_print_sysinfo();
int deltafs_nonop();  // XXX: simply trigger client initialization
mode_t deltafs_umask(mode_t __mode);
int deltafs_chroot(const char* __path);
int deltafs_chdir(const char* __path);
char* deltafs_getcwd(char* __buf, size_t __sz);
int deltafs_creat(const char* __path, mode_t __mode);
int deltafs_open(const char* __path, int __oflags, mode_t __mode);
int deltafs_openstat(const char* __path, int __oflags, mode_t, struct stat*);
int deltafs_openat(int fd, const char* __path, int __oflags, mode_t __mode);
int deltafs_getattr(const char* __path, struct stat* __stbuf);
int deltafs_mkfile(const char* __path, mode_t __mode);
int deltafs_mkdirs(const char* __path, mode_t __mode);
int deltafs_mkdir(const char* __path, mode_t __mode);
int deltafs_chmod(const char* __path, mode_t __mode);
int deltafs_chown(const char* __path, uid_t __usr, gid_t __grp);
int deltafs_stat(const char* __path, struct stat* __stbuf);
int deltafs_truncate(const char* __path, off_t __len);
int deltafs_access(const char* __path, int __mode);
int deltafs_accessdir(const char* __path, int __mode);
int deltafs_unlink(const char* __path);
typedef int (*deltafs_filler_t)(const char* __name, void* __arg);
int deltafs_listdir(const char* __path, deltafs_filler_t, void* __arg);
ssize_t deltafs_pread(int __fd, void* __buf, size_t __sz, off_t __off);
ssize_t deltafs_read(int __fd, void* __buf, size_t __sz);
ssize_t deltafs_pwrite(int __fd, const void* __buf, size_t __sz, off_t __off);
ssize_t deltafs_write(int __fd, const void* __buf, size_t __sz);
int deltafs_epoch_flush(int __fd, void* __arg);
int deltafs_fstat(int __fd, struct stat* __stbuf);
int deltafs_ftruncate(int __fd, off_t __len);
int deltafs_fdatasync(int __fd);
int deltafs_close(int __fd);

// ------------------------
// Light-weight plfsdir api
// ------------------------

typedef struct deltafs_plfsdir_stat {
  uint64_t ds_wtim;  // total time spent on compaction (us)
  off_t ds_isz;      // total index written (bytes)
  off_t ds_dsz;      // total data written (bytes)
} deltafs_plfsdir_stat_t;

typedef struct deltafs_plfsdir {
} deltafs_plfsdir_t;  // XXX: opaque handle for an opened plfsdir
#define DELTAFS_PLFSDIR deltafs_plfsdir_t
// Create an empty plfsdir dataset at a named location on the underlying storage
// system. Use the conf string to alter the default behavior of this dataset.
// The returned instance should be deleted by deltafs_plfsdir_close()
// when it is no longer needed and ready to be closed.
// Return NULL on errors, and a non-NULL pointer on success.
DELTAFS_PLFSDIR* deltafs_plfsdir_create(const char* __name, const char* __conf);
int deltafs_plfsdir_write_stat(DELTAFS_PLFSDIR* __dir,
                               struct deltafs_plfsdir_stat* __statbuf);
int deltafs_plfsdir_append(DELTAFS_PLFSDIR* __dir, const char* __fname,
                           const void* __buf, size_t __sz);
int deltafs_plfsdir_epoch_flush(DELTAFS_PLFSDIR* __dir, void* __arg);
int deltafs_plfsdir_close(DELTAFS_PLFSDIR* __dir);

#ifdef __cplusplus
}
#endif
