
/*
 * pdlfs_platform.h  platform-wide constants
 * 29-Jul-2016  chuck@ece.cmu.edu
 */

#ifndef PDLFS_COMMON_PDLFS_PLATFORM_H
#define PDLFS_COMMON_PDLFS_PLATFORM_H 1

/*
 * this file abstracts out building with old-style makefiles and cmake
 * builds.   when we stop using old-style makefiles we can just move
 * pdlfs_platform_expand.h here.
 */
#if defined(PDLFS_MAKEFILE)

/*
 * old-style makefile must pass platform information in via -D on
 * command line, so we don't do anything here.
 */

#else /* PDLFS_MAKEFILE */

/* pull in the file expanded by cmake ... */
#include "pdlfs-common/pdlfs_platform_expand.h"

#endif /* PDLFS_MAKEFILE */

#endif /* PDLFS_COMMON_PDLFS_PLATFORM_H */
