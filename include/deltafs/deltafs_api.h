/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <sys/stat.h>
#include <sys/types.h>

#include <stddef.h>
#include <stdint.h>

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
 * File system env
 * ------------------------
 */
struct deltafs_env; /* Opaque handle for an opened deltafs env */
typedef struct deltafs_env deltafs_env_t;
/* Returns NULL on errors. A heap-allocated env instance otherwise.
   The returned object should be deleted via deltafs_env_close(). */
deltafs_env_t* deltafs_env_init(int __argc, void** __argv);
int deltafs_env_is_system(deltafs_env_t* __env);
int deltafs_env_close(deltafs_env_t* __env);

/*
 * ------------------------
 * Background thread pool
 * ------------------------
 */
struct deltafs_tp; /* Opaque handle for a deltafs thread pool */
typedef struct deltafs_tp deltafs_tp_t;
/* Returns NULL on errors. A heap-allocated thread pool instance otherwise.
   The returned object should be deleted via deltafs_tp_close(). */
deltafs_tp_t* deltafs_tp_init(int __size);
/* Pause executing queued tasks or tasks submitted in future */
int deltafs_tp_pause(deltafs_tp_t* __tp);
/* Resume executing tasks */
int deltafs_tp_rerun(deltafs_tp_t* __tp);
int deltafs_tp_close(deltafs_tp_t* __tp);

/*
 * ------------------------
 * Light-weight plfsdir api
 * ------------------------
 */
struct deltafs_plfsdir; /* Opaque handle for an opened deltafs plfsdir */
typedef struct deltafs_plfsdir deltafs_plfsdir_t;
/* Use the default plfsdir storage engine */
#define DELTAFS_PLFSDIR_DEFAULT 0
/* Use leveldb as the storage engine
   Some features may become unavailable */
#define DELTAFS_PLFSDIR_LEVELDB 1
#define DELTAFS_PLFSDIR_LEVELDB_L0ONLY 3
#define DELTAFS_PLFSDIR_LEVELDB_L0ONLY_BF 7
/* Returns NULL on errors. A heap-allocated plfsdir handle otherwise.
   The returned object should be deleted via deltafs_plfsdir_free_handle(). */
deltafs_plfsdir_t* deltafs_plfsdir_create_handle(const char* __conf, int __mode,
                                                 int __io_engine);
int deltafs_plfsdir_set_key_size(deltafs_plfsdir_t* __dir, size_t __key_size);
int deltafs_plfsdir_set_val_size(deltafs_plfsdir_t* __dir, size_t __val_size);
/* Enforce multimap semantics */
int deltafs_plfsdir_set_multimap(deltafs_plfsdir_t* __dir, int __flag);
/* Set file system env. */
int deltafs_plfsdir_set_env(deltafs_plfsdir_t* __dir, deltafs_env_t* __env);
/* Set background thread pool. */
int deltafs_plfsdir_set_thread_pool(deltafs_plfsdir_t* __dir,
                                    deltafs_tp_t* __tp);
int deltafs_plfsdir_set_rank(deltafs_plfsdir_t* __dir, int __rank);
int deltafs_plfsdir_enable_io_measurement(deltafs_plfsdir_t* __dir, int __flag);
int deltafs_plfsdir_set_non_blocking(deltafs_plfsdir_t* __dir, int __flag);
/* Error printer type */
typedef void (*deltafs_printer_t)(const char* __err, void* __arg);
int deltafs_plfsdir_set_err_printer(deltafs_plfsdir_t* __dir,
                                    deltafs_printer_t __printer,
                                    void* __printer_arg);
/* Return the total number of configured memtable partitions. */
int deltafs_plfsdir_get_memparts(deltafs_plfsdir_t* __dir);
int deltafs_plfsdir_open(deltafs_plfsdir_t* __dir, const char* __name);
/* Put a piece of data into a given key.
   Return -1 on errors, or num bytes written. */
ssize_t deltafs_plfsdir_put(deltafs_plfsdir_t* __dir, const char* __key,
                            size_t __keylen, int __epoch, const char* __value,
                            size_t __sz);
/* Appends a piece of data into a given file.
   __fname will be hashed to become a fixed-sized key.
   Return -1 on errors, or num bytes written. */
ssize_t deltafs_plfsdir_append(deltafs_plfsdir_t* __dir, const char* __fname,
                               int __epoch, const void* __buf, size_t __sz);
/* Retrieve data from a given key at a specific epoch, or all epochs.
   Returns NULL if not found. A malloc()ed array otherwise.
   Stores the size of the value in *__sz.
   The result should be deleted by free(). */
char* deltafs_plfsdir_get(deltafs_plfsdir_t* __dir, const char* __key,
                          size_t __keylen, int __epoch, size_t* __sz,
                          size_t* __table_seeks, size_t* __seeks);
/* Retrieve data from a given filename at a specific epoch, or all epochs.
   Returns NULL if not found. A malloc()ed array otherwise.
   Stores the length of the file in *__sz.
   The result should be deleted by free(). */
void* deltafs_plfsdir_read(deltafs_plfsdir_t* __dir, const char* __fname,
                           int __epoch, size_t* __sz, size_t* __table_seeks,
                           size_t* __seeks);
/* Scan directory contents at a specific epoch, or all epochs.
   Report results to *saver. Return -1 on errors.
   Otherwise, return the total number of entries scanned. */
ssize_t deltafs_plfsdir_scan(deltafs_plfsdir_t* __dir, int __epoch,
                             void (*saver)(void* arg, const char* __key,
                                           size_t __keylen, const char* __value,
                                           size_t sz),
                             void* arg);
/* Returns NULL if not found. A malloc()ed array otherwise.
   The result should be deleted by free(). */
char* deltafs_plfsdir_get_property(deltafs_plfsdir_t* __dir, const char* __key);
/* A helper wrapper implemented on top of deltafs_plfsdir_get_property().
   Used when the property is known to be an integer. */
long long deltafs_plfsdir_get_integer_property(deltafs_plfsdir_t* __dir,
                                               const char* __key);
int deltafs_plfsdir_epoch_flush(deltafs_plfsdir_t* __dir, int __epoch);
int deltafs_plfsdir_flush(deltafs_plfsdir_t* __dir, int __epoch);
int deltafs_plfsdir_finish(deltafs_plfsdir_t* __dir);
int deltafs_plfsdir_free_handle(deltafs_plfsdir_t* __dir);

/*
 * -------------
 * Version query
 * -------------
 */

int deltafs_version_major();
int deltafs_version_minor();
int deltafs_version_patch();

#ifdef __cplusplus
}
#endif
