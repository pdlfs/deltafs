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

/* Used as a mode to create a special type of directories where all
   I/O operations to files beneath these directories will be performed
   in a parallel log-structured manner that resembles plfs */
#define DELTAFS_DIR_PLFS_STYLE 0x10000
#define DELTAFS_DIR_MASK 0xf0000

#define DELTAFS_DIR_IS_PLFS_STYLE(mode) \
  ((mode & DELTAFS_DIR_MASK) == DELTAFS_DIR_PLFS_STYLE)

#ifdef __cplusplus
extern "C" {
#endif

/*
 * ---------------------
 * Main file system api
 * ---------------------
 */

void deltafs_print_sysinfo();
int deltafs_nonop(); /* Simply trigger client initialization */
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

/*
 * ------------------------
 * Light-weight plfsdir api
 * ------------------------
 */

struct deltafs_env; /* Opaque handle for an opened env */
typedef struct deltafs_env deltafs_env_t;
/* Returns NULL on errors. A heap-allocated env instance otherwise.
   The returned object should be deleted via deltafs_env_close(). */
deltafs_env_t* deltafs_env_open(const char* __name, const char* __conf);
int deltafs_env_is_system(deltafs_env_t* __env);
int deltafs_env_close(deltafs_env_t* __env);

struct deltafs_plfsdir; /* Opaque handle for an opened plfsdir */
typedef struct deltafs_plfsdir deltafs_plfsdir_t;
/* Returns NULL on errors. A heap-allocated plfsdir handle otherwise.
   The returned object should be deleted via deltafs_plfsdir_free_handle(). */
deltafs_plfsdir_t* deltafs_plfsdir_create_handle(int __mode);
int deltafs_plfsdir_set_key_size(deltafs_plfsdir_t* __dir, int __key_size);
int deltafs_plfsdir_set_filter_bits_per_key(deltafs_plfsdir_t* __dir, int __b);
int deltafs_plfsdir_set_env(deltafs_plfsdir_t* __dir, deltafs_env_t* __env);
int deltafs_plfsdir_open(deltafs_plfsdir_t* __dir, const char* __name,
                         const char* __conf);
int deltafs_plfsdir_append(deltafs_plfsdir_t* __dir, const char* __fname,
                           int __epoch, const void* __buf, size_t __sz);
/* Returns NULL if not found. A malloc()ed array otherwise.
   The result should be deleted by free(). */
char* deltafs_plfsdir_readall(deltafs_plfsdir_t* __dir, const char* __fname);
/* Returns NULL if not found. A malloc()ed array otherwise.
   The result should be deleted by free(). */
char* deltafs_plfsdir_get_property(deltafs_plfsdir_t* __dir, const char* __key);
int deltafs_plfsdir_epoch_flush(deltafs_plfsdir_t* __dir, int __epoch);
int deltafs_plfsdir_finish(deltafs_plfsdir_t* __dir);
int deltafs_plfsdir_free_handle(deltafs_plfsdir_t* __dir);

#ifdef __cplusplus
}
#endif
