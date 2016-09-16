/*
 * deltafs_config.h  configuration and options from the build system
 * 15-Sep-2016  chuck@ece.cmu.edu
 */

#ifndef DELTAFS_DELTAFS_CONFIG_H
#define DELTAFS_DELTAFS_CONFIG_H 1

#if defined(DELTAFS_MAKEFILE)

/*
 * old-style makefile must pass platform information in via -D on
 * command line, so we don't do anything here.
 */

#else /* DELTAFS_MAKEFILE */

/* pull in info from cmake */
#include "deltafs/deltafs_config_expand.h"

#endif /* DELTAFS_MAKEFILE */

#endif /* DELTAFS_DELTAFS_CONFIG_H */
