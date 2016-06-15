
/*
 * pdlfs_config.h  base config and defns from the build system
 * 02-Jun-2016  chuck@ece.cmu.edu
 */

#ifndef PDLFS_COMMON_PDLFS_CONFIG_H
#define PDLFS_COMMON_PDLFS_CONFIG_H 1

/*
 * this file abstracts out Makefile vs. cmake builds
 * XXX
 */
#if defined(PDLFS_MAKEFILE)

/* old-style make files pass in config via -D on the command line */

#else

#include "pdlfs-common/pdlfs_config_expand.h"

#endif

#endif /* PDLFS_COMMON_PDLFS_CONFIG_H */
